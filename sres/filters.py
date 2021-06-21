from flask import g, session, url_for, escape, current_app
from copy import deepcopy
import collections
import re
from datetime import datetime, timedelta
import time
from natsort import natsorted, ns
from bs4 import BeautifulSoup
from flask_mail import Message
import mimetypes
import os
import threading
import logging

from sres.db import _get_db
from sres.auth import is_user_administrator, get_auth_user, get_auth_user_oid
from sres import utils
from sres.users import User, oids_to_usernames
from sres.conditions import Conditions, OpenConditions, AdvancedConditions
from sres.columns import column_oid_to_uuid, table_uuids_from_column_references, Column
from sres.tables import Table
from sres.studentdata import StudentData, substitute_text_variables
from sres.files import get_file_access_url, GridFile
from sres.logs import log_message_send, get_send_logs, get_feedback_stats, get_interaction_logs
from sres.tracking import make_urls_trackable, get_beacon_html
from sres.connector_canvas import CanvasConnector, is_canvas_connection_enabled

FEEDBACK_STYLES = {
    'null': {
        'style': 'null',
        'prompt': "",
        'options': []
    },
    'helpfulyesno': {
        'style': 'helpfulyesno',
        'prompt': "Help us improve: Was this message helpful?",
        'options': [
            {'value': 'Yes', 'display': 'Yes'},
            {'value': 'No', 'display': 'No'}
        ]
    }
}

HIDEABLE_UI_ELEMENTS = [
    ('n', 'name'),
    ('d', 'description'),
    ('a', 'administrators'),
    ('p', 'primary_conditions'),
    ('z', 'primary_conditions_selector'),
    ('m', 'contact_methods'),
    ('N', 'sender_name'),
    ('E', 'sender_email'),
    ('v', 'email_advanced'),
    ('s', 'email_sections'),
    ('h', 'attachments'),
    ('r', 'tracking'),
    ('f', 'feedback'),
    ('L', 'btn_collective'),
    ('C', 'btn_clone'),
    ('P', 'btn_convert_to_portal'),
    ('R', 'btn_rereference'),
    ('D', 'btn_delete'),
    ('I', 'btn_insert_column_reference'),
    ('M', 'btn_mc_magic_formatter'),
]

_CONTACT_METHODS = {
    'email': {
        'name': "Email",
        'enabled': True
    },
    'sms': {
        'name': "SMS",
        'enabled': False
    },
    'canvasinbox': {
        'name': "Canvas inbox",
        'enabled': False
    }
}

def list_authorised_filters(show_archived=False, only_fields=None, name=None, only_where_user_is_admin=False):
    """
        Gets all the filters that the current user is authorised to view.
        
        show_archived (boolean)
        only_fields (None or list) The db keys to return, as a list of strings.
            If [], requests all fields. If None, returns basic fields only.
        name (str) A filter to apply for the name of the filter.
        only_where_user_is_admin (bool) If True, will ignore any superadmin rights and only return 
            where current user is actually an admin for a portal.
        
        Returns a list of dicts, straight from db.filters
    """
    db = _get_db()
    find_filter = {}
    # administrators
    if not is_user_administrator('super') or only_where_user_is_admin:
        find_filter['administrators'] = get_auth_user_oid()
    # archived or not
    if show_archived:
        find_filter['workflow_state'] = {'$in': ['archived', '', None, 'active']}
    else:
        find_filter['workflow_state'] = {'$in': ['', None, 'active']}
    # name search
    if name is not None and name:
        find_filter['name'] = {'$regex': name, '$options': 'i'}
    # fields
    if only_fields is None:
        return_fields = ['uuid', 'name', 'description', 'created', 'modified', 'workflow_state', 'run_history']
    else:
        return_fields = only_fields
    # find!
    return list(db.filters.find(find_filter, return_fields).sort([('created', -1), ('name', 1)]))

def get_filters_for_table_uuid(table_uuid):
    db = _get_db()
    find_filter = {}
    find_filter['workflow_state'] = 'active'
    find_filter['tracking_record'] = {'$elemMatch': {'table_uuid': table_uuid}}
    return list(db.filters.find(find_filter))

def send_messages(filter_uuid, identifiers, auth_username, attempts_already=0, ignorelist_identifiers=None, rerun_primary_conditions=False):
    """Called upon to send filter messages asynchronously."""
    from sres.jobs import APSJob
    filter = Filter()
    if filter.load(filter_uuid):
        logging.debug('send_messages trying [{}] [{}] on pid [{}]'.format(attempts_already, filter_uuid, os.getpid()))
        # check if have tried too many times already...
        if attempts_already > 10:
            logging.error('send_messages too many attempts, quitting [{}]'.format(filter_uuid))
            return False
        # check if already running somewhere
        job_id = filter.get_queue_job_id()
        job = APSJob(job_id)
        if not job.claim_job(skip_loading=True):
            logging.debug('filter has_already_started, quitting [{}] pid [{}]'.format(filter_uuid, os.getpid()))
            return False
        # check who still needs to be sent this message
        identifiers_already_sent = filter.get_all_targets(return_identifiers=True)
        if rerun_primary_conditions == True:
            if ignorelist_identifiers is None or type(ignorelist_identifiers) is not list:
                # misconfiguration
                ignorelist_identifiers = []
                logging.error(f'filter [{filter_uuid}] appears ignorelist_identifiers misconfigured')
            # get a new set of target identifiers
            _filter_results = filter.run_conditions()
            _identifiers = [ v['sid'] for k, v in _filter_results['data'].items() ]
            identifiers_to_be_sent = list(set(_identifiers) - set(ignorelist_identifiers) - set(identifiers_already_sent))
        else:
            identifiers_to_be_sent = list(set(identifiers) - set(identifiers_already_sent))
        logging.debug(f'filter [{filter_uuid}] already sent [{len(identifiers_already_sent)}] to be sent [{len(identifiers_to_be_sent)}]')
        # if run_history is blank, then add
        if len(filter.config['run_history']) == 0:
            filter.add_run_history(auth_username)
        # iterate send messages
        failure_encountered = False
        count_successful_sends = 0
        from sres import create_app
        app = create_app()
        for identifier in identifiers_to_be_sent:
            try:
                with app.app_context():
                    # actually do the send
                    results = filter.get_personalised_message(
                        identifiers=[identifier],
                        mode='send',
                        auth_username=auth_username
                    )
                count_successful_sends += 1
            except Exception as e:
                failure_encountered = True
                logging.error('send_messages failed to identifier [{}] [{}]'.format(identifier, filter_uuid))
                logging.exception(e)
        # are we done? if not, reschedule for followup
        if failure_encountered:
            with app.app_context():
                filter.queue_send(
                    identifiers=identifiers_to_be_sent,
                    auth_username=auth_username,
                    attempts_already=attempts_already + 1
                )
        elif count_successful_sends > 0:
            logging.info(f'send_messages complete [{filter_uuid}] [{count_successful_sends}] on [{os.getpid()}]')
            # alert the sender??
            with app.app_context():
                msg = Message(
                    sender=current_app.config['SRES']['NOREPLY_EMAIL'],
                    recipients=[filter.config['email']['sender']['email']],
                    subject="SRES filter send complete",
                    body="We have finished sending {recipient_count} messages for the filter {filter_name}. View the logs: {view_log}".format(
                        recipient_count=len(filter.get_all_targets(return_identifiers=True)),
                        filter_name=filter.config['name'],
                        view_log=url_for('filter.view_logs', filter_uuid=filter.config['uuid'], _external=True)
                    ),
                    charset='utf-8'
                )
                # send!
                with current_app.mail.record_messages() as outbox:
                    send_result = current_app.mail.send(msg)
                    print(outbox)
        else:
            logging.info(f'send_messages completed without sending any messages [{filter_uuid}] [{count_successful_sends}] on [{os.getpid()}]')
        job.release_claim()
        del app
    else:
        logging.error('send_messages failed, could not load [{}]'.format(filter_uuid))

def send_scheduled_message_reminder(filter_uuid, send_ts):
    """Called upon to send a reminder that the filter is going to send soon."""
    from sres.jobs import APSJob
    filter = Filter()
    if filter.load(filter_uuid):
        job_id = filter.get_schedule_reminder_job_id()
        job = APSJob(job_id)
        if not job.claim_job(skip_loading=True):
            return
        from sres import create_app
        app = create_app()
        with app.app_context():
            msg = Message(
                sender=current_app.config['SRES']['NOREPLY_EMAIL'],
                recipients=[filter.config['email']['sender']['email']],
                subject="Reminder: SRES filter send is scheduled",
                body="""This is a courtesy reminder that the filter {filter_name} is scheduled to send at {send_ts}.\n\n
If you would like this to proceed, there is nothing you need to do.\n\n
If you want to edit the filter, click: {edit_filter}\n\n
If you want to preview the filter and/or change the schedule, click: {preview_filter}""".format(
                    filter_name=filter.config['name'],
                    send_ts=send_ts,
                    edit_filter=url_for('filter.edit_filter', filter_uuid=filter.config['uuid'], _external=True),
                    preview_filter=url_for('filter.preview_filter', filter_uuid=filter.config['uuid'], _external=True)
                ),
                charset='utf-8'
            )
            # send!
            with current_app.mail.record_messages() as outbox:
                send_result = current_app.mail.send(msg)
                print(outbox)
        job.release_claim()
        del app
    else:
        logging.error('send_scheduled_message_reminder failed, could not load [{}]'.format(filter_uuid))

def _convert_html_message_to_plaintext(html, convert_ahrefs=True):
    _html = html.replace('<p>', '\n').replace('</p>', '\n').replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n')
    soup = BeautifulSoup( _html, 'html.parser' )
    if convert_ahrefs:
        for a in soup.find_all('a', href=True):
            soup.a.replace_with(a.get_text() + ' (' + a['href'] + ')')
    return soup.get_text()

class Filter:
    
    default_config = {
        '_referenced_column_references': [],
        'uuid': None,
        'name': '',
        'description': '',
        'administrators': [],
        'conditions': {}, # queryBuilder config object
        'advanced_conditions': {
            'enabled': False,
            'expression': ''
        },
        'contact_type': ['email'],
        'email': {
            'sender': {
                'name': '',
                'email': ''
            },
            'addresses': {
                'reply_to': '',
                'cc': '',
                'bcc': '',
                'to': ''
            },
            'subject': '',
            'body_first': '',
            'sections': [
                # {
                #    'show_when': '', # (string) conditions|always
                #    'conditions': {}, # (dict) queryBuilder config
                #    'content': '', # (string) HTML content
                #    'display_order': 0 (int) Display order
                # }
            ],
            'body_last': '',
            'target_column': None, # ObjectId
            'attachments': [
                # {
                #    'original_filename': '', # (string)
                #    'filesize': 0, # (int) bytes
                #    'filename': '' # (string) local filename
                # }
            ],
            'feedback': {
                'style': 'helpfulyesno',
                'prompt': "Help us improve: Was this message helpful?",
                'options': [
                    {
                        'value': 'Yes',
                        'display': 'Yes',
                        'valence': 1
                    },
                    {
                        'value': 'No', 
                        'display': 'No',
                        'valence': -1
                    }
                ]
            }
        },
        'created': None,
        'modified': None,
        'run_history': [],
        'sms': {
            'body': '',
            'runs_remaining': 0,
            'target_column': None # ObjectId
        },
        'tracking_record': [
            {
                'table_uuid': '',
                'column_uuid': ''
            }
        ],
        'workflow_state': 'active',
        'legacy': {
            'advanced_conditions': 0,
            'conditions_clause': ''
        }
    }
    
    def __init__(self):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
        self.is_collective_asset = False
        self.contact_methods = deepcopy(_CONTACT_METHODS)
        if is_canvas_connection_enabled():
            self.contact_methods['canvasinbox']['enabled'] = True
        else:
            self.contact_methods.pop('canvasinbox')
    
    def load(self, filter_uuid=None):
        find_filter = {}
        find_filter['uuid'] = utils.clean_uuid(filter_uuid)
        results = self.db.filters.find(find_filter)
        results = list(results)
        if len(results) == 1:
            return self._load(results[0])
        else:
            return False
    
    def _load(self, db_result):
        self._id = db_result['_id']
        for key, value in self.default_config.items():
            try:
                if isinstance(self.config[key], collections.Mapping):
                    # is dict-type so try and merge
                    self.config[key] = {**value, **db_result[key]}
                else:
                    self.config[key] = db_result[key]
            except:
                self.config[key] = value
        return True

    def update(self, override_username=None, override_user_oid=None):
        if self.is_user_authorised(username=override_username, override_user_oid=override_user_oid):
            self.config['_referenced_column_references'] = self.get_referenced_column_references()
            result = self.db.filters.update_one({'uuid': self.config['uuid']}, {'$set': self.config})
            return result.acknowledged
        else:
            return False
        
    def create(self, override_user_oid=None):
        if is_user_administrator('filter', user_oid=override_user_oid) or is_user_administrator('super', user_oid=override_user_oid):
            pass
        else:
            return False
        self.config['uuid'] = utils.create_uuid()
        result = self.db.filters.insert_one(self.config)
        if result.acknowledged and self.load(self.config['uuid']):
            return self.config['uuid']
        else:
            return None
    
    def clone(self, add_cloned_notice=True, set_user_as_sole_administrator=False, reset_tracking_record=False, user_oid=None):
        """
            Clones the current filter. Returns the new uuid (string) if successful,
            or None if not.
        """
        filter_clone = Filter()
        if filter_clone.create(override_user_oid=user_oid):
            source_filter_config = deepcopy(self.config)
            # remove keys that should not be cloned
            del source_filter_config['uuid']
            del source_filter_config['run_history']
            # update some keys
            source_filter_config['created'] = datetime.now()
            if add_cloned_notice:
                source_filter_config['name'] = 'Clone of {}'.format(source_filter_config['name'])
                source_filter_config['description'] = '[Cloned filter] {}'.format(source_filter_config['description'])
            filter_clone.config = {**filter_clone.config, **source_filter_config}
            if reset_tracking_record:
                filter_clone.reset_tracking_record_config()
            filter_clone.add_user_to_administrators(commit_immediately=False, overwrite=set_user_as_sole_administrator, user_oid=user_oid)
            # save
            if filter_clone.update(override_user_oid=user_oid):
                return filter_clone.config['uuid']
        return None
    
    def delete(self, override_username=None, override_user_oid=None):
        self.config['workflow_state'] = 'deleted'
        return self.update(override_username=override_username, override_user_oid=override_user_oid)
    
    def add_user_to_administrators(self, user_oid=None, commit_immediately=True, overwrite=False):
        if user_oid is None:
            user_oid = get_auth_user_oid()
        if overwrite:
            self.config['administrators'] = [user_oid]
        else:
            if user_oid not in self.config['administrators']:
                self.config['administrators'].append(user_oid)
        if commit_immediately:
            return self.update(override_user_oid=user_oid)
        return True
    
    def is_user_authorised(self, username=None, override_user_oid=None):
        if is_user_administrator('super', username=username, user_oid=override_user_oid):
            return True
        if override_user_oid is not None:
            user_oid = override_user_oid
        else:
            user_oid = None
            if username is None:
                user_oid = get_auth_user_oid()
            else:
                user = User()
                if user.find_user(username=username):
                    user_oid = user._id
        if user_oid and user_oid in self.config['administrators']:
            return True
        return False
    
    def get_referenced_column_references(self, order_by_prevalence=True, deduplicate=True, uuids_only=False, skip_tracking_record=False):
        """
            Returns a list of string references of columns that are referenced in the current filter.
        """
        all_column_references = []
        # conditions
        conditions = Conditions(identifier=None, conditions=self.config['conditions'], student_data=None)
        all_column_references.extend(conditions.extract_all_column_references())
        # advanced conditions
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['advanced_conditions']['expression']))
        # email
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['sender']['name']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['sender']['email']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['addresses']['reply_to']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['addresses']['cc']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['addresses']['bcc']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['addresses'].get('to', '')))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['subject']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['body_first']))
        all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['body_last']))
        if self.config['email']['target_column']:
            all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['email']['target_column']))
        # email sections
        for section in self.config['email']['sections']:
            if section['content']:
                all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, section['content']))
            if section['conditions']:
                if section.get('show_when') == 'conditions':
                    conditions = Conditions(identifier=None, conditions=section['conditions'], student_data=None)
                    all_column_references.extend(conditions.extract_all_column_references())
        # tracking
        if not skip_tracking_record:
            for record in self.config['tracking_record']:
                if record.get('column_uuid'):
                    all_column_references.append(record['column_uuid'])
        # sms
        # TODO
        
        if len(all_column_references) == 0:
            return all_column_references
        
        # clean delimiters
        all_column_references = utils.clean_delimiter_from_column_references(all_column_references)
        # order by preference
        if order_by_prevalence:
            all_column_references = [r for r, c in collections.Counter(all_column_references).most_common() for r in [r] * c]
        # de-duplicate
        if deduplicate:
            all_column_references = list(dict.fromkeys(all_column_references))
        # if uuids_only
        if uuids_only:
            all_column_uuids = [ r.split('.')[0] for r in all_column_references ]
            all_column_uuids = list(dict.fromkeys(all_column_uuids))
            return all_column_uuids
        else:
            return all_column_references
    
    def get_referenced_table_uuids(self, order_by_prevalence=True, deduplicate=True, skip_tracking_record=False):
        """
            Returns a list of string uuids of tables that correspond to columns referenced
            in the current filter.
        """
        all_column_references = self.get_referenced_column_references(order_by_prevalence, deduplicate, skip_tracking_record=skip_tracking_record)
        # figure out tables
        all_table_uuids = table_uuids_from_column_references(all_column_references)
        return all_table_uuids
    
    def rereference_columns(self, original_column_reference, new_column_reference):
        """Re-references columns based on the mapping provided. Does not save.
            Provide column references in non-delimited forms. Uses a brute force 
            (recursive string finding) approach.
        """
        def _rereference_column_references(obj, original_column_reference, new_column_reference):
            if type(obj) is str:
                if original_column_reference in obj:
                    return obj.replace(original_column_reference, new_column_reference)
                else:
                    return obj
            elif type(obj) is list:
                return [ _rereference_column_references(el, original_column_reference, new_column_reference) for el in obj ]
            elif type(obj) is dict:
                return { 
                    _rereference_column_references(k, original_column_reference, new_column_reference): _rereference_column_references(v, original_column_reference, new_column_reference)
                    for k, v in obj.items() 
                }
            else:
                return obj
        # do replacements
        self.config['conditions'] = _rereference_column_references(self.config['conditions'], original_column_reference, new_column_reference)
        self.config['email']['sender'] = _rereference_column_references(self.config['email']['sender'], original_column_reference, new_column_reference)
        self.config['email']['addresses'] = _rereference_column_references(self.config['email']['addresses'], original_column_reference, new_column_reference)
        self.config['email']['subject'] = _rereference_column_references(self.config['email']['subject'], original_column_reference, new_column_reference)
        self.config['email']['body_first'] = _rereference_column_references(self.config['email']['body_first'], original_column_reference, new_column_reference)
        self.config['email']['body_last'] = _rereference_column_references(self.config['email']['body_last'], original_column_reference, new_column_reference)
        self.config['email']['sections'] = _rereference_column_references(self.config['email']['sections'], original_column_reference, new_column_reference)
        self.config['email']['target_column'] = _rereference_column_references(self.config['email']['target_column'], original_column_reference, new_column_reference)
    
    def get_authorised_usernames(self):
        if self.config['administrators']:
            return [v for k, v in oids_to_usernames(self.config['administrators']).items()]
        else:
            return []
    
    def add_attachment(self, local_filename, original_filename, file_size):
        mime_type = utils.guess_mime_type(local_filename)
        self.config['email']['attachments'].append({
            'original_filename': original_filename,
            'filesize': file_size,
            'filename': local_filename,
            'mime_type': mime_type
        })
        return True
        
    def delete_attachment(self, local_filename):
        try:
            delete_dict = next(f for i, f in enumerate(self.config['email']['attachments']) if f['filename'] == local_filename)
            self.config['email']['attachments'].remove(delete_dict)
            return True
        except:
            # probably couldn't find - problem!
            pass
        return False
    
    def get_attachment(self, local_filename, get_file_content=False):
        """
            Returns a dict with the metadata about the attachment identified by local_filename.
            This dict can also contain file_content.
        """
        try:
            attachment = next(f for i, f in enumerate(self.config['email']['attachments']) if f['filename'] == local_filename)
            _attachment = deepcopy(attachment)
            gf = GridFile('files')
            if gf.find_and_load(local_filename):
                if 'mime_type' not in _attachment.keys() or not _attachment['mime_type']:
                    _attachment['mime_type'] = gf.mime_type
                if get_file_content:
                    _attachment['file_content'] = gf.get_file().read()
                return _attachment
            else:
                return None
        except Exception as e:
            # probably couldn't find - problem!
            return None
    
    def pre_run_check(self):
        """Performs some simple pre-flight checks before filter is run"""
        ret = {
            'success': True,
            'messages': [] # list of tuples
        }
        if not self.config['advanced_conditions']['enabled']:
            if len(self.config['conditions']['rules']) == 0:
                ret['success'] = False
                ret['messages'].append(("Cannot run filter - there needs to be at least one primary condition.", 'danger'))
        return ret
    
    def run_conditions(self):
        """
            Runs the primary conditions.
        """
        if self.config['advanced_conditions']['enabled']:
            expr = BeautifulSoup(self.config['advanced_conditions']['expression'], 'html.parser').get_text()
            advanced_conditions = AdvancedConditions(expr)
            results = advanced_conditions.run_conditions()
            return results
        else:
            conditions = OpenConditions(self.config['conditions'])
            results = conditions.run_conditions()
            # sort by... surname
            results['data'] = dict(natsorted(results['data'].items(), key=lambda kv: kv[1]['surname'], alg=ns.IGNORECASE))
            # return
            return results
    
    def get_personalised_message(self, identifiers, mode='preview', auth_username=None):
        """
            Processes a personalised message for this current filter for the specified identifier(s).
            May be used to send a message.
            
            identifiers (list of strings) Typically SID
            mode (string) preview|send
            auth_username (string) Needed for when not running in a request context.
            
            Returns list of dicts of structure ret_element. Each dict corresponds to one identifier.
        """
        ret = []
        ret_element = {
            'identifier': '',
			'success': False,
			'messages': [], # list of tuples
			'email': {
				'details': {},
				'attachments': [],
				'target': {},
				'body': '',
				'body_plaintext': '',
				'subject': '',
				'send_result': {
					'success': False,
					'target': '',
					'messages': [] # list of tuples
				}
			},
            'contact_types': [],
            'contact_types_display': []
		}
        _referenced_table_uuids = self.get_referenced_table_uuids()
        _referenced_tables = {}
        main_table_uuid = _referenced_table_uuids[0]
        for _referenced_table_uuid in _referenced_table_uuids:
            _referenced_tables[_referenced_table_uuid] = Table()
            _referenced_tables[_referenced_table_uuid].load(_referenced_table_uuid)
        # load up connectors if needed
        if 'canvasinbox' in self.config['contact_type'] and is_canvas_connection_enabled():
            canvas_connector = CanvasConnector(_override_username=auth_username)
            canvas_connector.load_connections(self.config['tracking_record'][0]['table_uuid'])
            canvas_connector.load_connected_course_ids()
        # iterate through identifiers
        for identifier in identifiers:
            main_table_student_data = StudentData(_referenced_tables[main_table_uuid])
            ret_el = deepcopy(ret_element)
            ret_el['identifier'] = identifier
            # try and find the student
            student_found = False
            student_data = None
            if main_table_student_data.find_student(identifier):
                student_found = True
                student_data = main_table_student_data
            elif len(_referenced_tables) > 1:
                # try another table?
                for _referenced_table_uuid, _referenced_table in _referenced_tables.items():
                    if _referenced_table_uuid == main_table_uuid:
                        continue # no need to check this one, already checked
                    _student_data = StudentData(_referenced_table)
                    if _student_data.find_student(identifier):
                        student_found = True
                        student_data = _student_data
                        break
            if student_found and student_data:
                # basic email details
                ret_el['email']['details']['sender_name'] = substitute_text_variables(
                    input=self.config['email']['sender']['name'], 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text']
                ret_el['email']['details']['sender_email'] = substitute_text_variables(
                    input=self.config['email']['sender']['email'], 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text']
                ret_el['email']['details']['reply_to'] = substitute_text_variables(
                    input=self.config['email']['addresses']['reply_to'], 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text']
                ret_el['email']['details']['cc'] = substitute_text_variables(
                    input=self.config['email']['addresses']['cc'], 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text']
                ret_el['email']['details']['bcc'] = substitute_text_variables(
                    input=self.config['email']['addresses']['bcc'], 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text'] 
                ret_el['email']['details']['to'] = substitute_text_variables(
                    input=self.config['email']['addresses'].get('to', ''), 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text'] 
                ret_el['email']['target']['name'] = '{} {}'.format(student_data.config['preferred_name'], student_data.config['surname'])
                ret_el['email']['target']['email'] = student_data.config['email']
                # attachments
                ret_el['email']['attachments'] = []
                if self.config['email']['attachments']:
                    for attachment in self.config['email']['attachments']:
                        attachment_info = self.get_attachment(
                            local_filename=attachment['filename'],
                            get_file_content=True if mode == 'send' else False
                        )
                        if attachment_info:
                            attachment_info['filename'] = get_file_access_url(attachment['filename'])
                            ret_el['email']['attachments'].append(attachment_info)
                        else:
                            print('failed getting attachment_info', identifier, attachment['filename'])
                # message
                ret_el['email']['subject'] = substitute_text_variables(
                    input=self.config['email']['subject'], 
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )['new_text']
                ret_el['email']['body'] = self.get_substituted_email_body(
                    identifier=identifier, 
                    default_table_uuid=student_data.table.config['uuid'], 
                    preloaded_student_data=student_data
                )
                # recipients
                if len(ret_el['email']['details']['to'].strip()) > 0:
                    ret_el['email']['target']['email'] = re.findall("[^\s,;]+", ret_el['email']['details']['to'])
                    ret_el['email']['target']['name'] = ''
                # determine action
                if mode == 'send':
                    tracking_log_uuid = utils.create_uuid(sep='-')
                    # url tracking
                    ret_el['email']['body'] = make_urls_trackable(ret_el['email']['body'], tracking_log_uuid)
                    # inject_feedback_request
                    ret_el['email']['body'] = self.inject_feedback_request(ret_el['email']['body'], tracking_log_uuid)
                    # inject tracking beacon
                    ret_el['email']['body'] = ret_el['email']['body'] + '<br>' + get_beacon_html(tracking_log_uuid)
                    # recipients
                    recipients = ret_el['email']['target']['email'] if isinstance(ret_el['email']['target']['email'], list) else [ret_el['email']['target']['email']]
                    # send!
                    logging.debug("Attempting to send message filter [{}] [{}]".format(self.config['uuid'], str(recipients)))
                    try:
                        # check if already sent
                        if self.get_sent_messages(targets=recipients):
                            logging.warning("Skipping sending filter [{}] to [{}] because a record already exists in logs".format(self.config['uuid'], str(recipients)))
                            continue
                        # continue with send
                        if 'email' in self.config['contact_type']:
                            # form message
                            msg = Message(
                                subject=ret_el['email']['subject'],
                                recipients=recipients,
                                html=ret_el['email']['body'],
                                sender=(ret_el['email']['details']['sender_name'], ret_el['email']['details']['sender_email']),
                                reply_to=ret_el['email']['details']['reply_to'],
                                cc=re.findall("[^\s,;]+", ret_el['email']['details']['cc']),
                                bcc=re.findall("[^\s,;]+", ret_el['email']['details']['bcc']),
                                charset='utf-8'
                            )
                            # attachments
                            if ret_el['email']['attachments']:
                                for a, attachment in enumerate(ret_el['email']['attachments']):
                                    msg.attach(
                                        filename=attachment['original_filename'],
                                        content_type=attachment['mime_type'],
                                        data=attachment['file_content']
                                    )
                            with current_app.mail.record_messages() as outbox:
                                current_app.mail.send(msg)
                            # logging
                            log_message_send(
                                target=ret_el['email']['target']['email'], 
                                contact_type='email', 
                                message={
                                    'subject': ret_el['email']['subject'],
                                    'body': ret_el['email']['body']
                                },
                                source_asset_type='filter', 
                                source_asset_uuid=self.config['uuid'], 
                                log_uuid=tracking_log_uuid,
                                identifier=student_data.config['sid']
                            )
                            ret_el['contact_types'].append('email')
                            logging.info(f"Filter email dispatched [{self.config['uuid']}] [{recipients}] [{tracking_log_uuid}]")
                        if 'canvasinbox' in self.config['contact_type'] and is_canvas_connection_enabled():
                            # get canvas context
                            connected_course_ids = canvas_connector.connected_course_ids
                            course_context_id = None
                            if len(connected_course_ids) > 0:
                                course_context_id = connected_course_ids[0]
                            # make body
                            plain_body = _convert_html_message_to_plaintext(ret_el['email']['body'])
                            # make different tracking_log_uuid
                            canvasinbox_tracking_log_uuid = utils.create_uuid(sep='-')
                            plain_body = plain_body.replace(tracking_log_uuid, canvasinbox_tracking_log_uuid)
                            # get recipient user id
                            canvas_recipient = student_data.config['alternative_id1']
                            # send it
                            canvas_connector.create_conversation(
                                subject=ret_el['email']['subject'],
                                body=plain_body,
                                recipient=canvas_recipient,
                                override_course_context_id=course_context_id
                            )
                            # logging
                            log_message_send(
                                target=f"{student_data.config['sid']} ({canvas_recipient})", 
                                contact_type='canvasinbox', 
                                message={
                                    'subject': ret_el['email']['subject'],
                                    'body': plain_body
                                },
                                source_asset_type='filter', 
                                source_asset_uuid=self.config['uuid'], 
                                log_uuid=canvasinbox_tracking_log_uuid,
                                identifier=student_data.config['sid']
                            )
                            ret_el['contact_types'].append('canvasinbox')
                            logging.info(f"Filter canvasinbox dispatched [{self.config['uuid']}] [{canvas_recipient}] [{canvasinbox_tracking_log_uuid}]")
                        # increment_counter
                        self.increment_counter(
                            log_uuid=tracking_log_uuid, 
                            reset_to=0, 
                            preloaded_student_data=student_data
                        )
                        # update send_result
                        ret_el['email']['send_result']['success'] = True
                        ret_el['email']['send_result']['target'] = ret_el['email']['target']['email']
                    except Exception as e:
                        logging.error("FAILED send message filter [{}] [{}] [{}]".format(self.config['uuid'], str(recipients), repr(e)))
                        logging.exception(e)
                        ret_el['email']['send_result']['success'] = False
                        ret_el['messages'].append(("Error sending to {}.".format(str(recipients)), "warning"))
                    # wipe attachment filecontent
                    if ret_el['email']['attachments']:
                        for a, attachment in enumerate(ret_el['email']['attachments']):
                            if 'file_content' in attachment.keys():
                                del ret_el['email']['attachments'][a]['file_content']
                elif mode == 'preview':
                    ret_el['email']['body'] = self.inject_feedback_request(ret_el['email']['body'], '')
                    ret_el['email']['body_plaintext'] = _convert_html_message_to_plaintext(ret_el['email']['body'])
                    ret_el['contact_types'] = self.config['contact_type'].copy()
                    if not is_canvas_connection_enabled() and 'canvasinbox' in ret_el['contact_types']:
                        ret_el['contact_types'].remove('canvasinbox')
                ret_el['contact_types_display'] = [ _CONTACT_METHODS.get(ct, {}).get('name', "Unknown") for ct in ret_el['contact_types'] ]
                # success
                ret_el['success'] = True
            else:
                ret_el['messages'].append(("Identifier {} not found.".format(identifier), "warning"))
            ret.append(ret_el)
        return ret
    
    def add_run_history(self, auth_username):
        self.config['run_history'].append({
            'by': auth_username,
            'timestamp': datetime.now(),
            'contact_type': self.config['contact_type']
        })
        return self.update(override_username=auth_username)
    
    def get_substituted_email_body(self, identifier, default_table_uuid, preloaded_student_data):
        complete_body = ''
        # first section
        complete_body = complete_body + substitute_text_variables(
            input=self.config['email']['body_first'],
            identifier=identifier,
            default_table_uuid=default_table_uuid,
            preloaded_student_data=preloaded_student_data
        )['new_text']
        # additional sections
        for section in self.config['email']['sections']:
            show_section = False
            if section['show_when'] == 'always':
                show_section = True
            elif section['show_when'] == 'conditions':
                conditions_helper = Conditions(identifier=identifier, conditions=section['conditions'], student_data=preloaded_student_data)
                show_section = conditions_helper.evaluate_conditions()
            if show_section:
                complete_body = complete_body + substitute_text_variables(
                    input=section['content'],
                    identifier=identifier,
                    default_table_uuid=default_table_uuid,
                    preloaded_student_data=preloaded_student_data
                )['new_text']
        # last section
        complete_body = complete_body + substitute_text_variables(
            input=self.config['email']['body_last'],
            identifier=identifier,
            default_table_uuid=default_table_uuid,
            preloaded_student_data=preloaded_student_data
        )['new_text']
        return complete_body
    
    def run_sms(self):
        # TODO
        pass
    
    def reset_tracking_record_config(self):
        self.config['tracking_record'] = deepcopy(Filter.default_config['tracking_record'])
        return True
    
    def count_filters_run_per_targets(self, targets, from_date=None, to_date=None, for_any_filter=True):
        """
            Gets the number of messages sent to targets.
            
            targets (list of strings)
            from_date (datetime)
            to_date (datetime)
            for_any_filter (bool) Whether to return for any filter (True) or just this one (False)
            
            Returns a dict keyed by target, value is number of messages sent.
        """
        if from_date is None:
            from_date = datetime.now() - timedelta(days=7)
        if to_date is None:
            to_date = datetime.now()
        logs = get_send_logs(
            source_asset_type='filter', 
            source_asset_uuid=(None if for_any_filter else self.config['uuid']), 
            targets=targets,
            from_date=from_date,
            to_date=to_date
        )
        ret = {t.lower(): 0 for t in targets}
        for log in logs:
            try:
                ret[log['target'].lower()] += 1
            except:
                pass
        return ret
    
    def notify_feedback_comment(self, log_uuid, comment):
        
        class NotifyFeedbackComment(threading.Thread):
            def __init__(self, filter, log_uuid, comment):
                super(NotifyFeedbackComment, self).__init__()
                self.filter = filter
                self.log_uuid = log_uuid
                self.comment = comment
            def run(self):
                all_logs = get_send_logs(source_asset_type='filter', source_asset_uuid=self.filter.config['uuid'])
                # 10 as the hard-coded threshold for minimum number of recipients to reduce identifiability
                if len(all_logs) >= 10:
                    from sres import create_app
                    app = create_app()
                    with app.app_context():
                        log_uuid = utils.create_uuid(sep='-')
                        email_subject = "Feedback received on your SRES message"
                        email_body = "<p>This is an automated message from SRES.</p>"
                        email_body += "<p>An anonymous comment has been submitted for the filter <strong>{}</strong>. The comment is shown below.</p>".format(
                            self.filter.config['name']
                        )
                        email_body += "<p><em>{}</em></p>".format(escape(self.comment))
                        email_body += "<p>You can <a href=\"{}\">view all the stats for this filter</a> on SRES, including email opens, link clicks, and more feedback.</p>".format(
                            url_for('filter.view_logs', filter_uuid=self.filter.config['uuid'], _external=True)
                        )
                        email_body += "<hr>"
                        email_body += "<p>Help us improve: Was this message helpful? <a href=\"{}\">Yes</a> | <a href=\"{}\">No</a></p>".format(
                            url_for('tracking.feedback', log_uuid=log_uuid, v='Yes', _external=True),
                            url_for('tracking.feedback', log_uuid=log_uuid, v='No', _external=True)
                        )
                        # inject tracking beacon
                        email_body += '<br>' + get_beacon_html(log_uuid)
                        # compose
                        msg = Message(
                            sender=current_app.config['SRES']['NOREPLY_EMAIL'],
                            recipients=[self.filter.config['email']['sender']['email']],
                            subject=email_subject,
                            html=email_body,
                            charset='utf-8'
                        )
                        # send!
                        with current_app.mail.record_messages() as outbox:
                            send_result = current_app.mail.send(msg)
                            print(outbox)
                        # log
                        log_message_send(
                            target=self.filter.config['email']['sender']['email'], 
                            contact_type='email', 
                            message={
                                'subject': email_subject,
                                'body': email_body
                            },
                            source_asset_type='filter_feedback', 
                            source_asset_uuid=self.log_uuid, 
                            log_uuid=log_uuid
                        )
                    del app
                
        notification = NotifyFeedbackComment(self, log_uuid, comment)
        notification.start()

    def inject_feedback_request(self, text, log_uuid):
        if self.config['email']['feedback']['style'] != 'null':
            text += '<br><hr>{}'.format(self.config['email']['feedback']['prompt'])
            for option in self.config['email']['feedback']['options']:
                text += ' [<a href="{}">{}</a>]'.format(
                    url_for('tracking.feedback', log_uuid=log_uuid, f=self.config['uuid'], v=option['value'], _external=True),
                    option['display']
                )
        return text
    
    def increment_counter(self, log_uuid, reset_to=None, preloaded_student_data=None):
        """
            Sets the column value for the tracking counter.
            
            log_uuid (string uuid)
            reset_to (None or int) The number to set as the current counter value.
            preloaded_student_data (StudentData) Optional to speed up locating student.
                If provided, this must be loaded.
            
            Returns simply True or False for success.
        """
        if not preloaded_student_data:
            # find the student
            logs = get_send_logs(
                source_asset_type='filter', 
                source_asset_uuid=self.config['uuid'], 
                log_uuid=log_uuid
            )
            if logs:
                column = Column()
                if column.load(self.config['tracking_record'][0]['column_uuid']):
                    student_data = StudentData(column.table)
                    if not student_data.find_student(logs[0]['target']):
                        return False
                else:
                    return False
            else:
                return False
        else:
            student_data = preloaded_student_data
        # determine new counter value
        if reset_to is None:
            current_data = student_data.get_data(self.config['tracking_record'][0]['column_uuid'])
            if current_data['success'] and utils.is_number(current_data['data']):
                current_value = int(float(current_data['data']))
            else:
                current_value = 0
            new_value = current_value + 1
        else:
            new_value = reset_to
        # save
        result = student_data.set_data(
            column_uuid=self.config['tracking_record'][0]['column_uuid'],
            data=new_value,
            commit_immediately=True,
            skip_auth_checks=True
        )
        return result['success']
    
    def get_interaction_logs(self):
        # get base interaction logs
        interaction_logs = get_interaction_logs(
            'filter',
            self.config['uuid'],
            all_targets=self.get_all_targets()
        )
        # get target type
        all_targets = self.get_all_targets_types()
        target_to_type_mapper = {}
        for t in all_targets:
            if type(t['target']) is list:
                _target = ','.join(t['target'])
            elif type(t['target']) is str:
                _target = t['target']
            else:
                _target = str(t['target'])
            target_to_type_mapper[_target] = t.get('type', '')
        
        for target, record in interaction_logs['records'].items():
            if target in target_to_type_mapper and target_to_type_mapper[target]:
                interaction_logs['records'][target]['contact_type_display'] = _CONTACT_METHODS.get(target_to_type_mapper[target], {}).get('name', "Unknown")
        # return
        return interaction_logs
        
    def get_feedback_stats(self, days=None):
        return get_feedback_stats('filter', self.config['uuid'], days)

    def get_recipient_sent_count(self):
        _records = self.db.message_send_logs.find(
            {
                'source_asset_type': 'filter',
                'source_asset_uuid': self.config['uuid']
            },
            ['target']
        )
        records = []
        for r in _records:
            if type(r['target']) is str:
                records.append(r['target'])
            elif type(r['target']) is list:
                records.extend(r['target'])
        records = list(dict.fromkeys(records))
        return len(records)
    
    def get_all_targets(self, return_identifiers=False):
        records = self.db.message_send_logs.find(
            {
                'source_asset_type': 'filter',
                'source_asset_uuid': self.config['uuid']
            },
            ['target', 'identifier']
        )
        records = list(records)
        if return_identifiers:
            records = [r['identifier'] for r in records]
        else:
            records = [r['target'] for r in records]
        records, topology = utils.flatten_list(records)
        records = list(dict.fromkeys(records))
        return records
        
    def get_all_targets_types(self):
        records = self.db.message_send_logs.find(
            {
                'source_asset_type': 'filter',
                'source_asset_uuid': self.config['uuid']
            },
            ['target', 'identifier', 'type', 'uuid']
        )
        return list(records)
    
    def get_recipient_open_count(self):
        records = self.db.interaction_logs.find(
            {
                'source_asset_type': 'filter',
                'source_asset_uuid': self.config['uuid'],
                'action': 'open'
            },
            ['target', 'action']
        )
        records = [r['target'] for r in records]
        records, topology = utils.flatten_list(records)
        records = list(dict.fromkeys(records))
        return len(records)
    
    def get_sent_messages(self, targets=None, log_uuid=None):
        ret = []
        if targets is not None:
            if isinstance(targets, list):
                pass
            else:
                targets = [targets]
        else:
            targets = None
        logs = get_send_logs(
            source_asset_type='filter', 
            source_asset_uuid=self.config['uuid'],
            targets=targets,
            log_uuid=log_uuid
        )
        logs = list(logs)
        for log in logs:
            ret_el = {}
            ret_el['target'] = log.get('target', '')
            ret_el['log_uuid'] = log.get('uuid', '')
            ret_el['sent'] = log.get('sent', '')
            ret_el['type'] = log.get('type', '')
            ret_el['type_display'] = _CONTACT_METHODS.get(ret_el['type'], {}).get('name', "Unknown")
            ret_el['message'] = log.get('message', {
                'subject': '',
                'body': ''
            })
            ret.append(ret_el)
        return ret
    
    def get_substituted_sms_body(self):
        pass
    
    def get_sms_recipient_number(self):
        pass
    
    def get_queue_job_id(self):
        return 'sres_filter_send_f{}'.format(self.config['uuid'])
    
    def queue_send(self, identifiers, auth_username, attempts_already=0):
        job_id = self.get_queue_job_id()
        current_app.scheduler.add_job(
            send_messages,
            kwargs={
                'filter_uuid': self.config['uuid'],
                'identifiers': identifiers,
                'attempts_already': attempts_already,
                'auth_username': auth_username
            },
            trigger='date',
            run_date=datetime.now() + timedelta(seconds=10),
            max_instances=1,
            coalesce=True,
            id=job_id,
            replace_existing=True,
            misfire_grace_time=300
        )
        return {
            'success': True,
            'identifier_count': len(identifiers),
            'identifiers': identifiers,
            'sender_email': self.config['email']['sender']['email']
        }
    
    def is_send_queue_active(self):
        job_id = self.get_queue_job_id()
        if current_app.scheduler.get_job(job_id):
            return True
        else:
            return False
            
    def get_scheduled_job_id(self):
        return f"sres_filter_scheduled_send_f{self.config['uuid']}"
    
    def get_schedule_reminder_job_id(self):
        return f"sres_filter_scheduled_send_reminder_f{self.config['uuid']}"
    
    def schedule_send(self, identifiers, auth_username, scheduled_dt, reminder_hours_advance=None, advanced_schedule=False, ignorelist_identifiers=None):
        """Schedules a filter send action
            
            identifiers
            auth_username (list of str)
            scheduled_dt (Datetime)
            reminder_hours_advance (None or numeric)
            advanced_schedule (bool) Whether we are scheduling this in advance
            ignorelist_identifiers (None, or list of str) Identifiers to ignore
        """
        # schedule
        current_app.scheduler.add_job(
            send_messages,
            kwargs={
                'filter_uuid': self.config['uuid'],
                'identifiers': identifiers,
                'ignorelist_identifiers': ignorelist_identifiers,
                'rerun_primary_conditions': True if advanced_schedule else False,
                'auth_username': auth_username
            },
            trigger='date',
            run_date=scheduled_dt,
            max_instances=1,
            coalesce=True,
            id=self.get_scheduled_job_id(),
            replace_existing=True,
            misfire_grace_time=300
        )
        # set reminder?
        if utils.is_number(reminder_hours_advance):
            reminder_hours_advance = float(reminder_hours_advance)
            current_app.scheduler.add_job(
                send_scheduled_message_reminder,
                kwargs={
                    'filter_uuid': self.config['uuid'],
                    'send_ts': scheduled_dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
                },
                trigger='date',
                run_date=scheduled_dt - timedelta(hours=reminder_hours_advance),
                max_instances=1,
                coalesce=True,
                id=self.get_schedule_reminder_job_id(),
                replace_existing=True,
                misfire_grace_time=300
            )
        # return
        return {
            'success': True,
            'identifier_count': len(identifiers),
            'identifiers': identifiers,
            'sender_email': self.config['email']['sender']['email']
        }
    
    def is_send_schedule_active(self):
        job_id = self.get_scheduled_job_id()
        if current_app.scheduler.get_job(job_id):
            return True
        else:
            return False
    
    def get_send_schedule_run_utc_ts(self):
        job_id = self.get_scheduled_job_id()
        job = current_app.scheduler.get_job(job_id)
        if job is not None:
            #return job.next_run_time.astimezone().strftime('%Y-%m-%d %H:%M:%S')
            return job.next_run_time.isoformat()
        else:
            return ''
    
    def delete_scheduled_send(self):
        job_id = self.get_scheduled_job_id()
        try:
            current_app.scheduler.remove_job(job_id)
            return True
        except:
            return False
    
    