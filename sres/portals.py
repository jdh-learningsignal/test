from flask import g, session, url_for, escape, current_app
from copy import deepcopy
import re
import collections
from datetime import datetime
import logging

from sres.db import _get_db
from sres.auth import get_auth_user_oid, is_user_administrator, get_auth_user
from sres.users import User, oids_to_usernames
from sres.columns import table_uuids_from_column_references
from sres import utils
from sres.conditions import Conditions
from sres.logs import get_latest_feedback_events, get_feedback_stats
from sres.summaries import table_uuids_from_summary_uuids

FEEDBACK_STYLES = {
    'null': {
        'style': 'null',
        'prompt': "",
        'options': []
    },
    'helpfulyesno': {
        'style': 'helpfulyesno',
        'prompt': "Help us improve: Was this page helpful?",
        'options': [
            {
                'value': 'Yes',
                'display': 'Yes',
                'valence': 1,
                'followup_prompts': [
                    {
                        'type': 'textarea',
                        'name': 'comment',
                        'prompt': "Thanks for your feedback. How was it helpful?"
                    }
                ]
            },
            {
                'value': 'No',
                'display': 'No',
                'valence': -1,
                'followup_prompts': [
                    {
                        'type': 'textarea',
                        'name': 'comment',
                        'prompt': "Sorry it wasn't helpful. How can we improve?"
                    }
                ]
            }
        ]
    }
}

AUTHORISED_ROLES = {
    'administrator': {},
    'teacher': {},
    'viewer': {}
}

def list_authorised_portals(auth_user=None, show_deleted=False, only_fields=None, only_where_user_is_admin=False):
    """
        Gets all the portals that the specified or current user is authorised to view.
        
        auth_user (string) username
        show_deleted(boolean)
        only_fields (None or list) The db keys to return, as a list of strings.
            If [], requests all fields. If None, returns basic fields only.
        only_where_user_is_admin (bool) If True, will ignore any superadmin rights and only return 
            where current user is actually an admin for a portal.
        
        Returns a list of dicts, straight from db.portals
    """
    db = _get_db()
    filter = {}
    # administrators
    if not is_user_administrator('super') or only_where_user_is_admin:
        filter['administrators'] = get_auth_user_oid() if auth_user is None else usernames_to_oids([auth_user], add_if_not_exists=False)[0]
    # archived or not
    if show_deleted:
        filter['workflow_state'] = {'$in': ['archived', '', None, 'active']}
    else:
        filter['workflow_state'] = {'$in': ['', None, 'active']}
    # fields
    if only_fields is None:
        return_fields = ['uuid', 'name', 'description', 'created', 'modified', 'workflow_state', 'active']
    else:
        return_fields = only_fields
    # find!
    return list(db.portals.find(filter, return_fields).sort([('created', -1), ('name', 1)]))

def get_portals_for_table_uuid(table_uuid):
    db = _get_db()
    filter = {}
    filter['workflow_state'] = 'active'
    filter['default_table_uuid'] = table_uuid
    return list(db.portals.find(filter))

def interpret_portal_availability(config):
    ret = {
        'available': False,
        'messages': []
    }
    if not config['active']['available']:
        ret['messages'].append(("This portal has been made unavailable to students.", "warning"))
    elif config['active']['from'].date() > datetime.now().date():
        ret['messages'].append(("This portal will become available in the future.", "warning"))
    elif config['active']['to'].date() < datetime.now().date():
        ret['messages'].append(("This portal is no longer available.", "warning"))
    else:
        ret['available'] = True
    return ret

class Portal:
    
    default_config = {
        '_referenced_column_references': [],
        'uuid': None,
        'name': '',
        'description': '',
        'administrators': [],
        'teachers': [],
        'teachers_limit_by_columnuuid': '',
        'viewers': [],
        'viewers_limit_by_columnuuid': '',
        'active': {
            'available': True,
            'from': None,
            'to': None,
            'available_to_teachers': True,
            'available_to_viewers': True
        },
        'created': None,
        'modified': None,
        'require_auth': True,
        'default_table_uuid': '',
        'if_student_unknown': 'disallow',
        'workflow_state': 'active',
        'panels': [
            # {
            #     'uuid'
            #     'show_when'
            #     'conditions': {} # queryBuilder config
            #     'mode'
            #     'content'
            #     'display_order'
            #     'availability'
            #     'availability_from' # datetime
            #     'availability_to' # datetime
            #     'availability_from_str'
            #     'availability_to_str'
            #     'collapsible'
            #     'collapsible_default_display'
            #     'trigger_reload_on_save'
            # }
        ],
        'feedback': FEEDBACK_STYLES['helpfulyesno'],
        'max_width_px': '',
        'reload_portal_interval': ''
    }
    
    def __init__(self):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
        self.is_collective_asset = False
    
    def load(self, portal_uuid):
        filter = {}
        filter['uuid'] = utils.clean_uuid(portal_uuid)
        results = self.db.portals.find(filter)
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
            result = self.db.portals.update_one({'uuid': self.config['uuid']}, {'$set': self.config})
            return result.acknowledged
        else:
            return False
        
    def create(self, override_user_oid=None):
        if is_user_administrator('filter', user_oid=override_user_oid) or is_user_administrator('super', user_oid=override_user_oid):
            pass
        else:
            return False
        self.config['uuid'] = utils.create_uuid()
        result = self.db.portals.insert_one(self.config)
        if result.acknowledged and self.load(self.config['uuid']):
            return self.config['uuid']
        else:
            return None
    
    def clone(self, add_cloned_notice=True, set_user_as_sole_administrator=False, user_oid=None):
        """
            Clones the current portal. Returns the new uuid (string) if successful,
            or None if not.
        """
        portal_clone = Portal()
        if portal_clone.create(override_user_oid=user_oid):
            source_portal_config = deepcopy(self.config)
            # remove keys that should not be cloned
            del source_portal_config['uuid']
            # update some keys
            source_portal_config['created'] = datetime.now()
            if add_cloned_notice:
                source_portal_config['name'] = 'Clone of {}'.format(source_portal_config['name'])
                source_portal_config['description'] = '[Cloned portal] {}'.format(source_portal_config['description'])
            portal_clone.config = {**portal_clone.config, **source_portal_config}
            portal_clone.add_user_to_administrators(commit_immediately=False, overwrite=set_user_as_sole_administrator, user_oid=user_oid)
            # save
            if portal_clone.update(override_user_oid=user_oid):
                return portal_clone.config['uuid']
        return None
    
    def delete(self):
        self.config['workflow_state'] = 'deleted'
        return self.update()
    
    def add_user_to_administrators(self, user_oid=None, commit_immediately=True, overwrite=False):
        if user_oid is None:
            user_oid = get_auth_user_oid()
        if overwrite:
            self.config['administrators'] = [user_oid]
        else:
            if user_oid not in self.config['administrators']:
                self.config['administrators'].append(user_oid)
        if commit_immediately:
            return self.update()
        return True
    
    def is_user_authorised(self, username=None, override_user_oid=None, roles=None):
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
        if user_oid:
            if roles is None:
                roles = ['administrator']
            for role in roles:
                if user_oid in self.config.get(f'{role}s'):
                    return True
        return False
    
    def get_user_highest_role(self, username=None):
        if username is None:
            username = get_auth_user()
        for role in AUTHORISED_ROLES.keys():
            if self.is_user_authorised(roles=[role]):
                return role
        return None
    
    def get_referenced_column_references(self, order_by_prevalence=True, deduplicate=True, only_panel_uuid=None, uuids_only=False, only_columns=True):
        """
            Returns a list of string references of columns and summaries that are referenced in the current portal.
        """
        all_column_references = []
        # iterate through panels
        for panel in self.config['panels']:
            if only_panel_uuid and panel['uuid'] != only_panel_uuid:
                continue
            all_column_references.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, panel['content']))
            if not only_columns:
                all_column_references.extend(re.findall(utils.DELIMITED_SUMMARY_REFERENCE_PATTERN, panel['content']))
            if panel.get('show_when') == 'conditions':
                conditions = Conditions(identifier=None, conditions=panel['conditions'], student_data=None)
                all_column_references.extend(conditions.extract_all_column_references())
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
    
    def get_referenced_table_uuids(self, order_by_prevalence=True, deduplicate=True, only_panel_uuid=None):
        """
            Returns a list of string uuids of tables that correspond to columns and summaries referenced
            in the current portal.
        """
        all_column_references = self.get_referenced_column_references(order_by_prevalence, deduplicate, only_panel_uuid, only_columns=False)
        # figure out tables
        all_table_uuids = table_uuids_from_column_references(all_column_references)
        all_table_uuids = table_uuids_from_summary_uuids(all_column_references, existing_table_uuids_list=all_table_uuids)
        return all_table_uuids
    
    def is_portal_available(self):
        return interpret_portal_availability(self.config)
        #ret = {
        #    'available': False,
        #    'messages': []
        #}
        #if not self.config['active']['available']:
        #    ret['messages'].append(("This portal has been made unavailable to students.", "warning"))
        #elif self.config['active']['from'].date() > datetime.now().date():
        #    ret['messages'].append(("This portal will become available in the future.", "warning"))
        #elif self.config['active']['to'].date() < datetime.now().date():
        #    ret['messages'].append(("This portal is no longer available.", "warning"))
        #else:
        #    ret['available'] = True
        #return ret
    
    def get_authorised_usernames(self, role='administrator'):
        """Retrieve the authorised usernames for the specified role.
            
            role (str) administrator|teacher|viewer
        """
        if role not in AUTHORISED_ROLES.keys():
            return []
        if not self.config.get(f'{role}s'):
            return []
        return [ v for k, v in oids_to_usernames(self.config.get(f'{role}s')).items() ]
    
    def add_extra_student(self, username):
        from sres.studentdata import StudentData
        ret = {
            'success': False,
            'results': {},
            'messages': []
        }
        # grab the tables referred to in this portal
        referenced_table_uuids = self.get_referenced_table_uuids()
        # add student to each table
        for referenced_table_uuid in referenced_table_uuids:
            student_data = StudentData(referenced_table_uuid)
            ret['results'][referenced_table_uuid] = student_data.add_single_student_from_scratch(username)
        return ret
    
    def get_interaction_logs(self):
        ret = {
            'records': {},
            'urls': [],
            'opened_by': [],
            'total_opens': 0
        }
        records = self.db.interaction_logs.find(
            {
                'source_asset_type': 'portal',
                'source_asset_uuid': self.config['uuid']
            }
        )
        for record in records:
            try:
                auth_user = record.get('data', {}).get('auth_user')
                target = record.get('target')
            except:
                continue
            if auth_user and auth_user == target:
                if target not in ret['records'].keys():
                    ret['records'][target] = {
                        'target': target,
                        'opens': 0,
                        'clicks': {}
                    }
                if record.get('action') == 'open':
                    # increment overall counter
                    ret['total_opens'] += 1
                    # count for this target
                    ret['records'][target]['opens'] += 1
                    # add target to openers
                    if target not in ret['opened_by']:
                        ret['opened_by'].append(target)
                elif record.get('action') == 'click':
                    url = record.get('data', {}).get('details')
                    if url:
                        # remove pointless url clicks
                        if '/login/logout' in url:
                            continue
                        # save
                        if url in ret['records'][target]['clicks'].keys():
                            ret['records'][target]['clicks'][url] += 1
                        else:
                            ret['records'][target]['clicks'][url] = 1
                        if url not in ret['urls']:
                             ret['urls'].append(url)
        return ret

    def seconds_since_last_feedback_event(self, username=None):
        if username is None:
            username = get_auth_user()
        try:
            latest_feedback_event = get_latest_feedback_events(
                source_asset_type='portal',
                source_asset_uuid=self.config['uuid'],
                target=username
            )
            if len(latest_feedback_event) == 0:
                return -1 # i.e. never
            else:
                return (datetime.now() - latest_feedback_event[0]['timestamp']).total_seconds()
        except:
            logging.error('Could not calculate seconds_since_last_feedback_event [{}] for [{}]'.format(self.config['uuid'], get_auth_user()))
            return -1

    def get_feedback_stats(self, days=None):
        return get_feedback_stats('portal', self.config['uuid'], days)
    
    def get_max_width(self):
        try:
            return int(self.config['max_width_px'])
        except:
            return ''
    
    