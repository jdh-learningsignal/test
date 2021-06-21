from flask import current_app, g
import json
import requests
import pandas as pd
from copy import deepcopy
from datetime import datetime, timedelta
from dateutil import parser, tz
import re
import bleach
from natsort import natsorted, ns
from threading import Thread
from bs4 import BeautifulSoup
import base64
import os, sys
import logging
from urllib import parse
from hashlib import sha1
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed

from sres.db import DbCookie, _get_db
from sres.auth import get_auth_user, is_user_administrator
from sres import utils
from sres.studentdata import StudentData, run_aggregation_bulk
from sres.tables import Table
from sres.columns import find_column_by_name, Column, ColumnReferences
from sres.aggregatorcolumns import find_aggregators_of_columns, AggregatorColumn
from sres.config import _get_proxies

CONNECTION_META = {
    'past_meeting_participants': {
        'type': 'past_meeting_participants',
        'display': 'Past meeting participants',
        'hint': 'The participants from already-run meetings'
    }
}

def _encrypt_auth_token(unencrypted_token):
    return utils.encrypt_to_hex(unencrypted_token)

def _decrypt_auth_token(encrypted_token):
    return utils.decrypt_from_hex(encrypted_token)

def _make_job_id(connection_type, table_uuid, identifiers=None):
    if identifiers is None or (isinstance(identifiers, list) and len(identifiers) == 0):
        return 'sres_connector_zoom_{}_t{}'.format(
            connection_type,
            table_uuid
        )
    else:
        return 'sres_connector_zoom_{}_t{}_i{}'.format(
            connection_type,
            table_uuid,
            sha1(str(identifiers).encode()).hexdigest()
        )

def import_handler(connection_type, table_uuid, connection_config, connection_index, override_username, once_off=False):
    time_start = datetime.now()
    res = {
        'success': False,
        'messages': []
    }
    try:
        # check if already running somewhere
        from sres.jobs import APSJob
        job_id = _make_job_id(connection_type, table_uuid)
        job = APSJob(job_id)
        if not job.claim_job(skip_loading=True if once_off or system_level_connection else False):
            res['messages'].append(("Job already running.", "warning"))
            logging.info('Job already running, not starting again [{}]'.format(job_id))
            return res
    except Exception as e:
        print(e)
    # continue
    try:
        zoom_connector = ZoomConnector(_override_username=override_username)
        # grab connection details
        connections = zoom_connector.get_connection(table_uuid, connection_type)
        # see if enabled
        if not connections['enabled']:
            logging.debug('not running because not enabled [{}] [{}]'.format(connection_type, table_uuid))
            zoom_connector.schedule_task(action='delete', table_uuid=table_uuid, connection_type=connection_type)
            job.release_claim()
            return res
        # continue
        existing_connections = connections.get('connections', [])
        #logging.debug(f"import_handler [{connection_index}] [{existing_connections}] [{table_uuid}]")
        connection_config = existing_connections[connection_index]
        print('IMPORT_HANDLER', connection_type, table_uuid, override_username, connection_config)
        logging.info(f"connector_zoom.import_handler [{connection_type}] [{table_uuid}] [{override_username}] [{connection_config}]")
        if 'sres_username' in connection_config.keys():
            zoom_connector.override_username = connection_config['sres_username']
        zoom_connector._import_worker(connection_type, table_uuid, connection_config, res)
        job.release_claim()
        return res
    except Exception as e:
        logging.error('Exception running job [{}]'.format(job_id))
        logging.exception(e)
        print(e)
        job.release_claim()

#def run_triggerable_connections(table_uuid, identifiers=[]):
#    for con_id, con in CONNECTION_META.items():
#        if con.get('system_level_connection') == True:
#            connector = ZoomConnector(_override_username='__system__')
#            connection = connector.get_connection(table_uuid, con_id, system_level_connection=True)
#            if connection.get('enabled') and connection.get('triggerable') == 'yes':
#                import_handler(
#                    con_id,
#                    table_uuid,
#                    [],
#                    connection.get('username', '__system__'),
#                    system_level_connection=True,
#                    identifiers=identifiers
#                )

class ZoomConnector:
    
    def __init__(self, is_cron=False, _override_username=None):
        self.is_cron = False
        self.override_username = None
        self.config = {}
        
        self.connections = {}
        self.connected_course_ids = []
        
        # import config for db directly from instance - this is needed because 
        # this method could be called from outside of an active request
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from instance.config import SRES as SRES_CONFIG
        if 'zoom' in SRES_CONFIG['LMS'].keys():
            self.config = deepcopy(SRES_CONFIG['LMS']['zoom']['config'])
            self.config['api_url'] = self.config['base_url'] + self.config['api_path']
        
        # if is_cron and get_auth_user()[0:6] == '_cron-':
        #     self.is_cron = True
        #     self.override_username = get_auth_user()[6:]
        if _override_username is not None and is_user_administrator('super'):
            self.override_username = _override_username
        else:
            try:
                self.override_username = get_auth_user()
            except:
                self.override_username = _override_username
        
        self.db_cookie = DbCookie(self.override_username or get_auth_user())
        self.data_logger = logging.getLogger('sres.db.studentdata')
    
    def check_token_validity(self):
        """Basic token checker by requesting user's own details."""
        result = self._send_request(
            method='GET', 
            url='{api_url}users/me'.format(api_url=self.config['api_url'])
        )
        if result is not None and result['status_code'] == 200:
            return True
        return False
    
    def set_token(self, token, token_type, expires=None):
        if not expires:
            expires = datetime.now() + timedelta(days=365)
        if token_type in ['auth', 'refresh']:
            #if host_id is None:
            return self.db_cookie.set(
                key=f'{self.override_username}.sres.connector.zoom.{token_type}_token', 
                value=_encrypt_auth_token(token), 
                expires=None,
                use_key_as_is=True # do not additionally prepend with username since already specified
            )
            #else:
            #    return self.db_cookie.set(
            #        key=f'{host_id}.sres.connector.zoom.{token_type}_token', 
            #        value=_encrypt_auth_token(token),
            #        expires=None,
            #        use_key_as_is=True
            #    )
    
    def get_auth_token(self):
        return self.get_token('auth')
    
    def get_token(self, token_type):
        if token_type in ['auth', 'refresh']:
            #if host_id is None:
            #token = self.db_cookie.get(key=f'sres.connector.zoom.{token_type}_token')
            #else:
            token = self.db_cookie.get_like(
                key_pattern=f'{self.override_username}.sres.connector.zoom.{token_type}_token',
                ignore_username=True,
                get_latest_only=True,
                default=''
            )
            #logging.debug(f'{token_type} get_token {token}')
            if token:
                token = token.get('value', '')
                return _decrypt_auth_token(token)
        return ''
    
    def _get_user_details(self):
        result = self._send_request(
            method='GET', 
            url='{api_url}users/me'.format(api_url=self.config['api_url']),
            data_key=None
        )
        if result['success']:
            return {
                'success': True,
                'data': result['data']
            }
        else:
            return {'success': False}
    
    def _get_user_meetings(self, get_instances=False, parse_instances=False):
        """Requests the current user's meetings from Zoom's API.
        
            get_instances (boolean) Whether we should retrieve the unique instance meeting UUIDs
            parse_instances (boolean) Whether to explode the instances as part of the returned meetings list
        """
        result = self._send_request(
            method='GET', 
            url='{api_url}users/me/meetings?page_size=100'.format(api_url=self.config['api_url']),
            data_key='meetings'
        )
        if result['success']:
            if get_instances:
                # set up storage variables
                meeting_instances = []
                meetings = { m.get('id'): m for m in result['data'] }
                # define method to process the retrieved instances and put it into the storage variables
                def _process_ended_meeting_instances(future):
                    _res = future.result()
                    if _res['success']:
                        _data = _res['data']
                        _meeting = _res['meeting']
                        meeting_instances.append(
                            {
                                'meeting_id': _meeting.get('id'),
                                'instances': _data
                            }
                        )
                        meetings[_meeting.get('id')]['instances'] = _data
                    else:
                        logging.error('_process_ended_meeting_instances, failed')
                # multithreading to make this IO-bound process faster
                with ThreadPoolExecutor() as executor:
                    # start the getting of ended meeting instances
                    future_to_meeting_id = {
                        executor.submit(self._get_ended_meeting_instances, target_meeting): target_meeting_id for target_meeting_id, target_meeting in meetings.items()
                    }
                    # loop through the futures as they are completed
                    for future in as_completed(future_to_meeting_id):
                        _process_ended_meeting_instances(future)
                # return
                meetings = list(meetings.values())
            else:
                # not getting instances, just get standard meeting info
                meetings = result['data']
            # parse out instances if needed
            if get_instances and parse_instances:
                user_meetings = []
                for _user_meeting in meetings:
                    user_meeting_base = {
                        'uuid': _user_meeting.get('id'), # use numeric id as the uuid for now
                        'id': _user_meeting.get('id'),
                        'host_id': _user_meeting.get('host_id'),
                        'topic': _user_meeting.get('topic'),
                        'type': _user_meeting.get('type'),
                        'join_url': _user_meeting.get('join_url'),
                        'start_time': _user_meeting.get('start_time'),
                        'timezone': _user_meeting.get('timezone')
                    }
                    if _user_meeting.get('instances', []):
                        for _instance in _user_meeting.get('instances'):
                            user_meeting = deepcopy(user_meeting_base)
                            user_meeting['uuid'] = _instance.get('uuid')
                            user_meeting['start_time'] = _instance.get('start_time')
                            #user_meeting['config'] = utils.to_b64(user_meeting)
                            user_meetings.append(user_meeting)
                    else:
                        user_meeting = deepcopy(user_meeting_base)
                        #user_meeting['config'] = utils.to_b64(user_meeting)
                        user_meetings.append(user_meeting)
                meetings = user_meetings
            # add sres_username
            current_sres_auth_user = get_auth_user()
            for i, meeting in enumerate(meetings):
                meetings[i]['sres_username'] = current_sres_auth_user
            # sort
            meetings = self._sort_meeting_configs(meetings)
            # return
            return {
                'success': True,
                'data': meetings
            }
        else:
            return {'success': False}
    
    def _get_ended_meeting_instances(self, meeting):
        """Gets the instances for the specified meeting.
            
            meeting (dict) {uuid, id, host_id, topic, type, join_url, duration, timezone}
        """
        meeting_id = meeting.get('id')
        result = self._send_request(
            method='GET', 
            url='{api_url}past_meetings/{meeting_id}/instances'.format(
                api_url=self.config['api_url'],
                meeting_id=meeting_id
            ),
            data_key='meetings'
        )
        if result['success']:
            return {
                'success': True,
                'data': result['data'],
                'meeting': meeting,
                'meeting_id': meeting_id
            }
        else:
            return {'success': False}
    
    def _get_past_meeting_participants(self, meeting_identifier):
        """Requests past meeting participants from the Zoom API. Returns the raw result from the API.
            
            meeting_identifier (str or numeric) Zoom meeting uuid or id.
        """
        if meeting_identifier.startswith('/') or '//' in meeting_identifier:
            meeting_identifier = parse.quote(parse.quote(meeting_identifier, safe=''), safe='')
        result = self._send_request(
            method='GET', 
            url='{api_url}past_meetings/{meeting_uuid}/participants'.format(
                api_url=self.config['api_url'],
                meeting_uuid=meeting_identifier,
            ),
            data_key='participants'
        )
        if result['success']:
            return {
                'success': True,
                'data': result['data']
            }
        else:
            return {'success': False}
    
    def get_meeting_participants(self, meeting_identifier):
        """Returns a non-duplicated list of meeting participants.
            
            meeting_identifier (str or numeric) A Zoom meeting uuid or id.
        """
        ret = {
            'success': False,
            'participants': []
        }
        # get data
        data = self._get_past_meeting_participants(meeting_identifier)
        if data['success']:
            # remove duplicates
            participants = {}
            for participant in data['data']:
                _participant_id = participant.get('id')
                if _participant_id and _participant_id not in participants.keys():
                    participants[_participant_id] = participant
            # save
            ret['success'] = True
            ret['participants'] = list(participants.values())
        return ret
    
    def get_past_meeting_participants_friendly_as_df(self, meeting_configs):
        """Returns a DataFrame with email addresses against integers (or blank) based on number
            of meetings that each person attended.
            
            meeting_configs (list of dicts)
        """
        data = {}
        for meeting_config in meeting_configs:
            meeting_identifier = meeting_config.get('uuid')
            sres_username = meeting_config.get('sres_username', None)
            self.override_username = sres_username
            _res = self.get_meeting_participants(meeting_identifier)
            if _res['success']:
                _participants = _res['participants']
                for _participant in _participants:
                    _participant_email = _participant.get('user_email', '')
                    if _participant_email:
                        if _participant_email not in data.keys():
                            data[_participant_email] = {
                                'email': _participant_email,
                                'data': 1
                            }
                        else:
                            data[_participant_email]['data'] += 1
        # make df
        df_data = pd.DataFrame.from_dict(data, dtype='str', orient='index')
        return df_data
    
    def _sort_meeting_configs(self, meeting_configs):
        try:
            sorted_meeting_configs = natsorted(meeting_configs, key=lambda i: (i.get('start_time', datetime(2100, 12, 31).timestamp()), i.get('topic', '')), alg=ns.IGNORECASE)
            return sorted_meeting_configs
        except Exception as e:
            logging.error(e)
            logging.debug(meeting_configs)
            return meeting_configs
    
    # CONNECTION SAVING
    
    def set_connection(self, table_uuid, connection_type, additional_data):
        ret = {
            'success': False,
            'messages': []
        }
        auth_token = self.get_auth_token()
        if not auth_token:
            ret['messages'].append(("Authorisation token unavailable.", "danger"))
        else:
            settings = {
                'enabled': True,
                'table_uuid': table_uuid
            }
            settings = {**settings, **additional_data}
            ret['success'] = self.db_cookie.set(
                key=f'sres.connector.zoom.{connection_type}.t{table_uuid}', 
				value=json.dumps(settings),
                use_key_as_is=True # do not prepend with username
            )
            if ret['success']:
                ret['messages'].append(("Successfully updated: {}".format(CONNECTION_META[connection_type]['display']), "success"))
            else:
                ret['messages'].append(("Could not save settings.", "warning"))
        return ret
    
    def get_connection(self, table_uuid, connection_type):
        cookie = self.db_cookie.get_like(
            key_pattern=f'sres.connector.zoom.{connection_type}.t{table_uuid}',
            ignore_username=True,
            get_latest_only=True,
            default=''
        )
        ret = {
            'enabled': False,
            'table_uuid': table_uuid
        }
        if cookie:
            connection_data = json.loads(cookie.get('value', '{}'))
            return {**ret, **connection_data}
        else:
            return ret
    
    #def unset_connection(self, table_uuid, connection_type):
    #    return self.db_cookie.delete(key='sres.connector.zoom.{}.t{}'.format(connection_type, table_uuid))
    
    #def disable_connection(self, table_uuid, connection_type):
    #    connection = self.get_connection(table_uuid=table_uuid, connection_type=connection_type)
    #    connection['enabled'] = False
    #    return self.db_cookie.set(
    #        key='sres.connector.zoom.{}.t{}'.format(connection_type, table_uuid), 
    #        value=json.dumps(connection)
    #    )
    
    def extract_meeting_configs_from_connections(self, connections, existing_user_meetings=None, merge=False):
        """Extracts the meeting config dicts from the provided list of connections.
            Optionally avoids collisions if provided with existing_user_meetings list.
            
            merge (boolean) Whether to return a merged list (True) or return just the extracted configs (False).
        """
        keyed_meeting_configs = {}
        existing_meeting_identifiers = []
        if existing_user_meetings:
            for existing_user_meeting in existing_user_meetings:
                existing_meeting_identifiers.append(existing_user_meeting.get('uuid'))
        for connection in connections:
            _meeting_configs = connection.get('meeting_configs', [])
            for _meeting_config in _meeting_configs:
                if _meeting_config.get('uuid') not in existing_meeting_identifiers:
                    if _meeting_config.get('uuid') not in keyed_meeting_configs.keys():
                        keyed_meeting_configs[_meeting_config.get('uuid')] = _meeting_config
        if merge:
            for existing_user_meeting in existing_user_meetings:
                if existing_user_meeting.get('uuid') not in keyed_meeting_configs.keys():
                    keyed_meeting_configs[existing_user_meeting.get('uuid')] = existing_user_meeting
        meeting_configs = list(keyed_meeting_configs.values())
        meeting_configs = self._sort_meeting_configs(meeting_configs)
        return meeting_configs
    
    #def load_connections(self, table_uuid):
    #    for con_id, con in CONNECTION_META.items():
    #        self.connections[con_id] = self.get_connection(table_uuid=table_uuid, connection_type=con_id)
    
    def schedule_task(self, action, table_uuid, connection_type, connection_config, connection_index, run_now=False):
        ret = {
            'success': False,
            'messages': []
        }
        job_id = _make_job_id(connection_type, table_uuid)
        print(action.upper(), 'job_id', job_id, run_now)
        if run_now and action == 'run':
            try:
                current_app.scheduler.add_job(
                    import_handler,
                    args=(
                        connection_type, 
                        table_uuid, 
                        connection_config, 
                        connection_index, 
                        connection_config.get('sres_username', get_auth_user()), 
                        True
                    ),
                    trigger='date',
                    max_instances=1,
                    coalesce=True,
                    id=job_id + '_oncenow',
                    replace_existing=True,
                    misfire_grace_time=60
                )
                ret['success'] = True
            except Exception as e:
                logging.error('schedule_task error for job_id [{}]'.format(job_id))
                logging.exception(e)
        return ret
    
    def _import_worker(self, connection_type, table_uuid, connection_config, res):
        t0 = datetime.now()
        # run import
        if connection_type == 'past_meeting_participants':
            df_data = self.get_past_meeting_participants_friendly_as_df(meeting_configs=connection_config.get('meeting_configs', []))
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data,
                data_type=connection_type,
                target_column_uuid=connection_config.get('column_destination_uuid')
            )
        # update res
        res = {**res, **_res}
        # no return value
        logging.info(f"Zoom connector _import_worker completed [{connection_type}] [{(datetime.now() - t0).total_seconds()}] [{table_uuid}]")
    
    def _import_data(self, table_uuid, df_data, data_type, target_column_uuid, multi_entry_options=[], mapped_multi_entry_options={}, perform_subfield_shifts=False):
        ret = {
            'success': False,
            'messages': []
        }
        destination_column_uuids = []
        destination_identifiers = []
        column_mappings = {} # keyed by column_uuid - maps column_uuids to column references
        preloaded_columns = {} # keyed by column_uuid
        expected_identifiers = ['id', 'zoom_id', 'sid', 'email']
        expected_identifier_in_use = ''
        # create column if not exists, otherwise update
        for column_header in list(df_data):
            if column_header in expected_identifiers + []:
                # don't import data from these columns!
                pass
            else:
                column = Column()
                if column.load(target_column_uuid):
                    old_multientryoptions = deepcopy(column.config['multi_entry']['options'])
                    # update some metadata for existing column
                    if multi_entry_options:
                        column.config['type'] = 'multiEntry'
                        column.config['multi_entry']['options'] = multi_entry_options
                        column.update(override_username=self.override_username)
                    else:
                        column.config['type'] = 'mark'
                        column.config['simple_input']['allow_free'] = 'true'
                    if mapped_multi_entry_options and column_header in mapped_multi_entry_options.keys():
                        column.config['type'] = 'multiEntry'
                        column.config['multi_entry']['options'] = mapped_multi_entry_options[column_header]['multi_entry_options']
                        column.update(override_username=self.override_username)
                    # perform subfield shifts if needed
                    #if perform_subfield_shifts and (multi_entry_options or mapped_multi_entry_options):
                    #    column_references = ColumnReferences(override_username=self.override_username)
                    #    subfield_shifts = column_references.parse_subfield_shift(old_multientryoptions, column.config['multi_entry']['options'])
                    #    if subfield_shifts['shift_needed']:
                    #        column_references.perform_subfield_shifts(
                    #            column_uuid=column.config['uuid'],
                    #            old_to_new_mapping=subfield_shifts['old_to_new_mapping'],
                    #            override_username=self.override_username
                    #        )
                destination_column_uuids.append(column.config['uuid'])
                column_mappings[column_header] = column.config['uuid']
                preloaded_columns[column.config['uuid']] = Column()
                preloaded_columns[column.config['uuid']].load(column.config['uuid'])
        # turn nans into blank strings
        df_data.fillna('', inplace=True)
        # save data
        success_count = 0
        student_data = StudentData(table_uuid)
        for index, row in df_data.iterrows():
            student_data._reset()
            student_found = False
            if expected_identifier_in_use != '':
                student_found = student_data.find_student({
                    expected_identifier_in_use: row[expected_identifier_in_use]
                })
            if not student_found or expected_identifier_in_use == '':
                for expected_identifier in expected_identifiers:
                    if expected_identifier in row.keys():
                        student_found = student_data.find_student({
                            expected_identifier: row[expected_identifier]
                        })
                        if student_found:
                            expected_identifier_in_use = expected_identifier
                            break
            if student_found:
                destination_identifiers.append(student_data.config['sid'])
                for column_header in column_mappings:
                    current_data = row[column_header]
                    # set
                    student_data.set_data(
                        column_uuid=column_mappings[column_header],
                        data=current_data,
                        auth_user_override=self.override_username,
                        skip_auth_checks=True,
                        commit_immediately=False,
                        ignore_active=True,
                        only_save_history_if_delta=True, # Important so changelog isn't flooded
                        skip_aggregation=True, # Important to save resources until final aggregation,
                        preloaded_column=preloaded_columns[column_mappings[column_header]]
                    )
                # commit to db
                if student_data.save():
                    self.data_logger.info("Data committed in _import_data for {} {}".format(student_data.config['sid'], str(student_data._id)))
                    success_count += 1
            elif expected_identifier_in_use == '':
                # problem
                print('Could not find student AAA')
                pass
            else:
                # cannot find student
                print('Could not find student BBB')
                ret['messages'].append(("Could not find student.", "warning"))
                logging.warning("Could not find student, in _import_data")
            pass
        # run aggregators if necessary
        bulk_aggregation_results = run_aggregation_bulk(
            source_column_uuids=destination_column_uuids,
            target_identifiers=destination_identifiers,
            override_username=self.override_username
        )
        # update column description to be last updated date, if necessary/appropriate
        for column_header, column_uuid in column_mappings.items():
            if column_uuid in preloaded_columns.keys():
                column = preloaded_columns[column_uuid]
            else:
                column = Column()
                if not column.load(column_mappings[column_header]):
                    logging.error("Could not load column {} while running Zoom connect for table {}".format(column_mappings[column_header], table_uuid))
                    continue
            column.config['description'] = 'Last updated {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            column.config['active']['from'] = datetime.now()
            column.config['active']['to'] = datetime.now()
            column.config['datasource']['mode'] = 'sync'
            column.config['datasource']['type'] = 'lms'
            column.config['datasource']['name'] = 'zoom'
            column.update(override_username=self.override_username)
        # return
        ret['messages'].append(("Successfully saved {} {} records.".format(str(success_count), data_type), "success"))
        logging.info("Successfully saved {} {} records to {}.".format(str(success_count), data_type, table_uuid))
        return ret

    # Generic functions
    
    def _send_request(self, url, data_key=None, method='GET', parameters=None, url_params=None, override_auth_header=None, failure_count=0, ignore_pagination=False):
        """
            Sends a request to Zoom API and follows all link rels as necessary.
            
            url (string)
            method (string)
            parameters (dict) for data payload i.e. form fields
            url_params (dict or None)
            override_auth_header (dict or None)
            failure_count (int)
            data_key (string or None) the key that actually holds the data; if None then assume no data key
            ignore_pagination (boolean)
        """
        #print('requesting', url)
        ret = {
            'data': None,
            'raw': None,
            'status_code': 0,
            'headers': None,
            'success': False
        }
        if parameters is None:
            parameters = {}
        if url_params is None:
            url_params = {}
        # get auth token
        auth_token = self.get_auth_token()
        # make auth header
        if override_auth_header is None:
            auth_header = {'Authorization': 'Bearer {}'.format(auth_token)}
        else:
            auth_header = override_auth_header
        #logging.debug(auth_header)
        # build the url with extra url_params if needed
        if url_params:
            _url = parse.urlparse(url)
            _query = parse.parse_qs(_url.query)
            _query = {**_query, **url_params}
            _url = _url._replace(query=parse.urlencode(_query, doseq=True))
            url = parse.urlunparse(_url)
        # run the request
        #logging.debug(f"sending request to url {url} with params {parameters}")
        if method.lower() == 'get':
            r = requests.get(
                url,
                headers={**auth_header},
                data=parameters,
                proxies=_get_proxies()
            )
        elif method.lower() == 'post':
            r = requests.post(
                url,
                headers={**auth_header},
                data=parameters,
                proxies=_get_proxies()
            )
        ret['status_code'] = r.status_code
        ret['headers'] = deepcopy(r.headers)
        ret['raw'] = r.text
        #logging.debug(f"r.text {r.text}")
        if r.status_code == 200:
            res = r.json()
            ret['success'] = res.get('success', True)
            if data_key is None:
                ret['data'] = deepcopy(r.json())
            else:
                ret['data'] = deepcopy(res[data_key])
            ret['next_page_token'] = res.get('next_page_token', None)
            # deal with pagination
            if res.get('page_count', 1) > 1 and not ignore_pagination:
                combined_data = deepcopy(res[data_key])
                next_page_token = res['next_page_token']
                for loop_counter in range(2, res['page_count'] + 1):
                    url_params['next_page_token'] = next_page_token
                    r_internal = self._send_request(
                        url=url,
                        method=method,
                        url_params=url_params,
                        ignore_pagination=True,
                        data_key=data_key
                    )
                    if r_internal['status_code'] == 200 and r_internal['success']:
                        next_page_token = r_internal['next_page_token']
                        combined_data.extend(r_internal['data'])
                ret['data'] = combined_data
            # return
            return ret
        elif r.status_code == 401:
            if failure_count > 5:
                return ret
            res = r.json()
            if res.get('code') == 124:
                # access token expired - need to request token refresh
                refresh_result = self.refresh_oauth2_token()
                if refresh_result and refresh_result['success']:
                    # retry request
                    return self._send_request(
                        url=url,
                        method=method,
                        parameters=parameters,
                        failure_count=(failure_count + 1),
                        url_params=url_params,
                        override_auth_header=override_auth_header,
                        ignore_pagination=ignore_pagination,
                        data_key=data_key
                    )
                else:
                    # problem refreshing token
                    logging.warning('Problem refreshing token')
                    return ret
            else:
                # unauthenticated
                return ret
        return ret
    
    def get_oauth2_login_url(self, table_uuid):
        state = {
            'table_uuid': table_uuid
        }
        state = base64.b64encode(pickle.dumps(state)).decode()
        return "{installation_url}&state={state}".format(
            installation_url=self.config['oauth2']['installation_url'],
            state=state
        )
    
    def process_oauth2_response(self, code, state):
        parameters = {
            'grant_type': "authorization_code",
            'redirect_uri': self.config['oauth2']['redirect_uri'],
            'code': code
        }
        ret = {
			'success': False,
			'table_uuid': "",
			'messages': []
		}
        basic_auth_credential = base64.b64encode(f"{self.config['oauth2']['client_id']}:{self.config['oauth2']['client_secret']}".encode()).decode()
        result = self._send_request(
            method='POST', 
            url=self.config['oauth2']['token_endpoint'],
            parameters=parameters,
            override_auth_header={'Authorization': f'Basic {basic_auth_credential}'}
        )
        if result['status_code'] == 200:
            auth_response = result['data']
            state = pickle.loads(base64.b64decode(state))
            self._save_tokens(auth_response)
            ret['success'] = True
            ret['table_uuid'] = state['table_uuid']
        else:
            # problem
            ret['messages'].append((result['raw'], "danger"))
        return ret
    
    def _save_tokens(self, auth_response):
        # save the tokens
        self.set_token(
            token=auth_response['access_token'],
            token_type="auth",
            expires=datetime.now() + timedelta(hours=1)
        )
        self.set_token(
            token=auth_response['refresh_token'],
            token_type="refresh",
            expires=datetime.now() + timedelta(days=15*365)
        )
    
    def refresh_oauth2_token(self):
        parameters = {
            'grant_type': "refresh_token",
            'refresh_token': self.get_token("refresh")
        }
        ret = {
            'success': False,
            'messages': [],
            'status_code': 0
        }
        basic_auth_credential = base64.b64encode(f"{self.config['oauth2']['client_id']}:{self.config['oauth2']['client_secret']}".encode()).decode()
        result = self._send_request(
            method='POST', 
            url=self.config['oauth2']['token_endpoint'],
            url_params=parameters,
            override_auth_header={'Authorization': f'Basic {basic_auth_credential}'}
        )
        if result['success']:
            auth_response = result['data']
            self._save_tokens(auth_response)
            ret['success'] = True
        elif result:
            # problem
            ret['messages'].append((result['raw'], "danger"))
        else:
            # big problem
            pass
        return ret
    