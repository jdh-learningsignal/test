from flask import g, session, current_app
from copy import deepcopy
import re
from datetime import datetime, time, date
import collections
from dateutil import parser
from bs4 import BeautifulSoup
from natsort import natsorted, ns
from hashlib import sha256
import logging
import json

from sres.db import _get_db
from sres.auth import is_user_administrator, get_auth_user, get_auth_user_oid
from sres import utils
from sres.jobs import APSJob
from bson import ObjectId


SYSTEM_COLUMNS = [
    {
        'display': 'Preferred name', 
        'name': 'preferred_name', 
        'show': True,
        'simple': True,
        'insertable': True,
        'insert_value': 'PREFERREDNAME'
    },
    {
        'display': 'Given names', 
        'name': 'given_names', 
        'show': False, 
        'simple': True,
        'insertable': True,
        'insert_value': 'GIVENNAMES'
    },
    {
        'display': 'Surname', 
        'name': 'surname', 
        'show': True, 
        'simple': True,
        'insertable': True,
        'insert_value': 'SURNAME'
    },
    {
        'display': 'SID', 
        'name': 'sid', 
        'show': True, 
        'simple': True,
        'insertable': True,
        'insert_value': 'SID'
    },
    {
        'display': 'Email', 
        'name': 'email', 
        'show': True, 
        'simple': True,
        'insertable': True,
        'insert_value': 'EMAIL'
    },
    {
        'display': 'Username', 
        'name': 'username', 
        'show': False, 
        'simple': True,
        'insertable': False,
        'insert_value': 'USERNAME'
    },
    {
        'display': 'Alternative ID 1', 
        'name': 'alternative_id1', 
        'show': False, 
        'simple': True,
        'insertable': False,
        'insert_value': 'ALTERNATIVEID1'
    },
    {
        'display': 'Alternative ID 2', 
        'name': 'alternative_id2', 
        'show': False, 
        'simple': True,
        'insertable': False,
        'insert_value': 'ALTERNATIVEID2'
    }
]

MAGIC_FORMATTERS_LIST = [
    {
        'name': 'display',
        'display': 'What is shown on the form (\'text to display\')',
        'hint': 'Shows the text that is shown when selecting options e.g. on a button, in a dropdown',
        'for': ['multiEntry', 'mark']
    },
    {
        'name': 'description',
        'display': 'Longer description',
        'hint': 'Shows the long description',
        'for': ['multiEntry', 'mark']
    },
    {
        'name': 'image',
        'display': 'Present as image',
        'hint': 'Shows the data as an image',
        'for': ['image', 'imgurl']
    },
    {
        'name': 'image.w150',
        'display': 'Present as thumbnail image',
        'hint': 'Shows the data as a thumbnail image (150 pixels wide)',
        'for': ['image', 'imgurl']
    },
    {
        'name': 'audio_player',
        'display': 'Audio player',
        'hint': 'Shows an audio player',
        'for': ['multiEntry.audio-recording']
    },
    {
        'name': 'file_download_links',
        'display': 'Link(s) to file(s)',
        'hint': 'Shows the data as link(s) to download file(s)',
        'for': ['file']
    },
    {
        'name': 'file_download_links_bullets',
        'display': 'Bulleted link(s)to file(s)',
        'hint': 'Shows the data as a bulleted list of link(s) to download file(s)',
        'for': ['file']
    },
    {
        'name': 'join_space',
        'display': 'Join up list elements',
        'hint': 'Connects elements of a list with a single space',
        'for': ['multiEntry', 'mark'],
        'enabled_for_global_magic': True
    },
    {
        'name': 'join_bullets',
        'display': 'Make a bulleted list from list elements',
        'hint': 'Displays elements of a list as a bulleted list',
        'for': ['multiEntry', 'mark'],
        'enabled_for_global_magic': True
    },
    {
        'name': 'join_paragraphs',
        'display': 'Turn list elements into a sequence of paragraphs',
        'hint': 'Displays elements of a list as a series of paragraphs',
        'for': ['multiEntry', 'mark'],
        'enabled_for_global_magic': True
    },
    {
        'name': 'tabulate_reports',
        'display': 'Make a table out of multiple reports',
        'hint': 'Only relevant if multiple reports mode has been enabled',
        'for': ['multiEntry'],
        'enabled_for_global_magic': False
    },
    {
        'name': 'round0',
        'display': 'Round to integer',
        'hint': 'Only works for numeric data',
        'for': ['multiEntry', 'mark'],
        'enabled_for_global_magic': False
    },
    {
        'name': 'round1',
        'display': 'Round to 1 decimal place',
        'hint': 'Only works for numeric data',
        'for': ['multiEntry', 'mark'],
        'enabled_for_global_magic': False
    }
]

COLUMN_DATA_TYPES_META = {
    'submission': {
        'name': 'Timestamp',
        'description': 'A simple YYYY-MM-DD HH:MM:SS timestamp is stored for each student.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'multiEntry': {
        'name': 'Multi-entry',
        'description': 'Multiple pieces of related data are saved together. Provides a range of input types including dropdowns, buttons, textboxes, signature panels, audio, etc.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'mark': {
        'name': 'Simple entry',
        'description': 'A simple data point is stored for each student. This can be through a button or a simple textbox.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'attendance': {
        'name': 'One entry over many students',
        'description': 'The same, simple, data is stored for a series of students. After data is entered, the system will ask for students for whom to apply the data.',
        'user_configurable': False,
        'direct_data_entry': True
    },
    'counter': {
        'name': 'Counter',
        'description': 'A number is incremented each time a student is identified to the system.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'toggle': {
        'name': 'Toggle',
        'description': 'Each time a student is identified to the system, the data toggles/rotates through a set of values.',
        'user_configurable': False,
        'direct_data_entry': True
    },
    'signinout': {
        'name': 'Sign-in/out',
        'description': 'Signing in and out, complete with welcome and goodbye messages. Data is stored in one column only, and repeated sign-in overwrites previous sign in/out information.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'signinoutmemory': {
        'name': 'Sign-in/out with memory',
        'description': 'Signing in and out, with sign-in/out information being saved for each week.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'image': {
        'name': 'Image',
        'description': 'Save an image for each student.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'imgurl': {
        'name': 'Image URL',
        'description': 'Stores a URL to an image.',
        'user_configurable': False,
        'direct_data_entry': False
    },
    'file': {
        'name': 'File',
        'description': 'Save a file for each student.',
        'user_configurable': True,
        'direct_data_entry': True
    },
    'aggregator': {
        'name': 'Aggregator',
        'description': 'Data is calculated based on the values in other columns.',
        'user_configurable': True,
        'direct_data_entry': False
    },
    'teacherallocation': {
        'name': 'Teacher allocation',
        'description': 'Special system column for allocating teacher(s) to students',
        'user_configurable': True,
        'direct_data_entry': True
    }
}

TEXT_ENTRY_REGEX_OPTIONS = [
    ("Any text", ".*"), 
    ("Whole numbers only", "^[0-9]*$"), 
    ("Numbers only (decimals ok)", "^[0-9]*(.[0-9]+)?$"), 
    ("Numbers only (increments of 0.5 only)", "^[0-9]*(.[05])?$")
]

MULTI_ENTRY_SUBFIELD_TYPES = [
    {
        'name': 'regex',
        'description': 'One-line textbox'
    },
    {
        'name': 'regex-long',
        'description': 'Multi-line text entry'
    },
    {
        'name': 'html-simple',
        'description': 'Simple rich text editor'
    },
    {
        'name': 'select',
        'description': 'Buttons'
    },
    {
        'name': 'dropdown',
        'description': 'Dropdown list'
    },
    {
        'name': 'slider',
        'description': 'Slider'
    },
    {
        'name': 'audio-recording',
        'description': 'Audio recording'
    },
    {
        'name': 'sketch-small',
        'description': 'Sketch (small)'
    },
    {
        'name': 'label-only',
        'description': 'Just display label'
    },
    {
        'name': 'timestamp',
        'description': 'Timestamp (YYYY-MM-DD HH:MM:SS)'
    },
    {
        'name': 'authuser',
        'description': 'Logged in username'
    },
    {
        'name': 'geolocation',
        'description': 'Geolocation'
    }
]

def column_uuid_to_oid(uuid):
    db = _get_db()
    if uuid:
        columns = list(db.columns.find({"uuid": uuid}))
        if len(columns) == 1:
            return columns[0]["_id"]
    return None

def column_oid_to_uuid(oid):
    db = _get_db()
    columns = list(db.columns.find({"_id": oid}))
    if len(columns) == 1:
        return columns[0]["uuid"]
    else:
        return None

def table_uuids_from_column_uuids(column_uuids=[], user_must_be_admin=False, order_by_prevalence=True):
    db = _get_db()
    if not column_uuids:
        return []
    filter = {
        'uuid': {'$in': column_uuids}
    }
    results = db.columns.find(filter, ['uuid', 'table_uuid'])
    results = list(results)
    # parse out just table_uuid, maintaining order
    _map = { c['uuid']: c['table_uuid'] for c in results }
    table_uuids = []
    for column_uuid in column_uuids:
        if column_uuid in _map.keys():
            table_uuids.append(_map[column_uuid])
    # order by prevalence
    if order_by_prevalence:
        table_uuids = sorted(table_uuids, key=collections.Counter(table_uuids).get, reverse=True)
    # de-duplicate
    table_uuids = list(dict.fromkeys(table_uuids))
    # if user must be admin... additional checks
    if user_must_be_admin:
        tables = db.tables.find({
            'uuid': {'$in': table_uuids},
            'staff.administrators': get_auth_user_oid()
        }, ['uuid'])
        tables = list(tables)
        table_uuids = [t['uuid'] for t in tables]
    # return
    return table_uuids

def table_uuids_from_column_references(column_references=[], user_must_be_admin=False):
    if not column_references:
        return []
    column_uuids = [c.split('.')[0] for c in column_references if c.split('.')[0]]
    return table_uuids_from_column_uuids(column_uuids, user_must_be_admin)

def get_friendly_column_name(show_table_info=True, get_text_only=False, table=None, column=None):
    """
        Returns string with friendly name of specified column (dict) in specified table (dict).
    """
    s = ''
    if show_table_info and table is not None and table['_id']:
        s = s + '{} {} S{} - '.format(table['config']['code'], table['config']['year'], table['config']['semester'])
    s = s + '{}'.format(column['config']['name'])
    if column.get('subfield', None) is not None:
        if 'subfield_label' in column.keys() and column['subfield_label'] != '':
            # already provided
            subfield_label = column['subfield_label']
        else:
            # need to figure it out
            subfield_labels = []
            for option in column['config']['multi_entry']['options']:
                subfield_labels.append(option['label'] if 'label' in option else '')
            subfield_label = subfield_labels[column['subfield']]
        if get_text_only:
            subfield_label = BeautifulSoup(subfield_label, 'html.parser').get_text()
        s = s + ' >> {}'.format(subfield_label)
    if (column['config']['type'] == 'multiEntry' or column['config'].get('multientry_data_format') == True) and column.get('subfield', None) is None:
        s = s + ' [Full dataset]'
    return s

def parse_multientry_column_reference(column_reference):
    """
        Parses a multi-part column reference.
        
        Returns a dict:
        {
            simple (boolean)
            base_column_uuid (string)
            field_number (int or empty string)
            base_column_reference (string)
        }
    """
    ret = {}
    if '.' in column_reference:
        ret['simple'] = False
        ret['base_column_uuid'] = re.sub('[^A-Z0-9a-z_-]', '', column_reference.split('.')[0])
        field_number = column_reference.split('.')[1]
        if utils.is_number(field_number):
            ret['field_number'] = int(field_number)
        else:
            ret['field_number'] = ''
            ret['simple'] = True
    elif '->>"$[' in column_reference:
        # legacy only
        ret['simple'] = False
        ret['base_column_uuid'] = re.sub('[^A-Z0-9a-z_-]', '', column_reference.split('-')[0])
        ret['field_number'] = int(column_reference.split('[')[1].split(']')[0])
    else:
        ret['simple'] = True
        ret['base_column_uuid'] = re.sub('[^A-Z0-9a-z_-]', '', column_reference)
    if ret['simple']:
        ret['base_column_reference'] = ret['base_column_uuid']
    else:
        ret['base_column_reference'] = '{}.{}'.format(ret['base_column_uuid'], ret['field_number'])
    return ret

def parse_string_for_column_references(input_string, pattern=utils.DELIMITED_COLUMN_REFERENCE_PATTERN, uuids_only=False, remove_delimiters=True):
    column_references = re.findall(pattern, input_string)
    if remove_delimiters:
        column_references = utils.clean_delimiter_from_column_references(column_references)
    if uuids_only:
        column_uuids = [ r.split('.')[0] for r in column_references ]
        column_uuids = list(dict.fromkeys(column_uuids))
        return column_uuids
    else:
        return column_references

def get_db_results_for_columns_and_tables(column_references):
    """Goes directly to the db to get db results of column configs and table configs for the
        specified column references.
    """
    db = _get_db()
    ret = {
        'column_results': {}, # keyed by column reference
        'table_results': {}, # keyed by table_uuid
        'column_reference_to_table_uuid': {} # mapping, keyed by column_reference
    }
    
    column_uuids_needed = set()
    system_column_names = set([ c['name'] for c in SYSTEM_COLUMNS ])
    # keep uniques and ignore system columns
    column_references = set(column_references)
    column_references = column_references - system_column_names
    column_references = list(column_references)
    # grab the column uuids needed
    for column_reference in column_references:
        _column_uuid = column_reference.split('.')[0]
        column_uuids_needed.add(_column_uuid)
    column_uuids_needed = list(column_uuids_needed)
    # get the configs from db
    column_results = list(db.columns.find({
        'uuid': { '$in': column_uuids_needed }
    }))
    # make a dict keyed by column uuid
    column_results_by_column_uuid = { c['uuid']: c for c in column_results }
    # put the configs into the return dict 
    for column_reference in column_references:
        _column_uuid = column_reference.split('.')[0]
        ret['column_results'][column_reference] = column_results_by_column_uuid[_column_uuid]
        ret['column_reference_to_table_uuid'][column_reference] = column_results_by_column_uuid[_column_uuid]['table_uuid']
    # get table configs
    table_uuids_needed = list(set( [ c['table_uuid'] for c in column_results ] ))
    # get the configs from db
    table_results = list(db.tables.find({
        'uuid': { '$in': table_uuids_needed }
    }))
    ret['table_results'] = { t['uuid']: t for t in table_results }
    
    # return
    return ret

def get_config_from_column_uuids(column_uuids):
    """
        Gets the column metadata/config from db.columns for the specified column_uuids (list of string uuids).
        Returns a dict of dicts, keyed by column_uuid.
    """
    ret = {}
    db = _get_db()
    if isinstance(column_uuids, list):
        column_uuids = [utils.clean_uuid(c) for c in column_uuids]
        results = list(db.columns.find({
            'uuid': {'$in': column_uuids}
        }))
        for result in results:
            ret[result['uuid']] = result
    return ret

def find_column_by_name(term, table_uuid=None, exact=True, return_loaded_instances=False, find_only_active=True, term_is_system_name=False):
    """
        Searches db.columns for the term.
        Returns a list. Elements are column_uuids (strings) [default], or loaded Column instances.
        
        table_uuid (str|None)
        exact (boolean)
        return_loaded_instances (boolean) If True, returns loaded Column instances. If False, returns str uuid.
        find_only_active (boolean) Whether to restrict to only columns that are active
        term_is_system_name (boolean or str) True, False, 'either'
    """
    ret = []
    db = _get_db()
    
    filter = {}
    # active only?
    if find_only_active:
        filter['workflow_state'] ='active'
    # which table
    if table_uuid:
        filter['table_uuid'] = table_uuid
    # name
    if term_is_system_name == True or term_is_system_name == False:
        if term_is_system_name:
            name_key = 'system_name'
        else:
            name_key = 'name'
        # exact match?
        if exact:
            filter[name_key] = {'$regex': '^{}$'.format(term), '$options': 'i'}
        else:
            filter[name_key] = {'$regex': term, '$options': 'i'}
    elif term_is_system_name == 'either':
        # exact match?
        if exact:
            filter['$or'] = [
                { 'name': {'$regex': f'^{term}$', '$options': 'i'} },
                { 'system_name': {'$regex': f'^{term}$', '$options': 'i'} }
            ]
        else:
            filter['$or'] = [
                { 'name': {'$regex': term, '$options': 'i'} },
                { 'system_name': {'$regex': term, '$options': 'i'} }
            ]
    # search
    results = list(db.columns.find(filter))
    # prioritise the result with a matching system_name if needed
    if term_is_system_name == 'either' and len(results) > 1:
        # two or more columns with name and/or system_name matching the term
        _priority_i = None
        for i, result in enumerate(results):
            if result['system_name'] == term:
                _priority_i = i
                break
        if _priority_i is not None:
            _results = [ results[_priority_i] ]
            for i, result in enumerate(results):
                if i != _priority_i:
                    _results.append( result )
            results = _results
    # return
    for result in results:
        if return_loaded_instances:
            column = Column()
            column.load(result['uuid'])
            ret.append(column)
            del column
        else:
            ret.append(result['uuid'])
    return ret

def _get_scheduled_job_id(table_uuid, column_uuid, type):
    return 'sres_column_{}_t{}_c{}'.format(type, table_uuid, column_uuid)

def cron_reset(column_uuid, data):
    column = Column()
    if column.load(column_uuid):
        job_id = _get_scheduled_job_id(column.table.config['uuid'], column.config['uuid'], 'reset')
        job = APSJob(job_id)
        logging.debug(f"cron_reset trying to claim {job_id}")
        if not job.claim_job():
            logging.info('cron_reset Job already running, not starting again [{}]'.format(job_id))
            return False
        from sres.studentdata import StudentData
        identifiers = column.table.get_all_students_sids()
        student_data = StudentData(column.table)
        # apply to all
        logging.debug('cron_reset starting [{}] [{}]'.format(column.config['uuid'], column.table.config['uuid']))
        #logging.debug('cron_reset identifiers ' + str(identifiers))
        for identifier in identifiers:
            student_data._reset()
            t0 = datetime.now()
            if student_data.find_student(identifier):
                #logging.debug('cron_reset setting student ' + str(identifier) + ' to ' + str(data))
                result = student_data.set_data(
                    column_uuid=column.config['uuid'],
                    data=data,
                    commit_immediately=True,
                    preloaded_column=column,
                    skip_auth_checks=True
                )
            else:
                logging.debug('cron_reset could not find student ' + str(identifier))
        logging.debug(f"cron_reset releasing claim {job_id}")
        job.release_claim()
        return True
    else:
        return False

def cron_backup_data_email(column_uuid, email):
    from flask_mail import Message
    if not current_app:
        from sres import create_app
        app = create_app()
    else:
        app = current_app
    column = Column()
    if column.load(column_uuid):
        job_id = _get_scheduled_job_id(column.table.config['uuid'], column.config['uuid'], 'backup_data_email')
        job = APSJob(job_id)
        if not job.claim_job():
            logging.info('Job already running, not starting again [{}]'.format(job_id))
            return False
        # get the export
        export_data = column.table.export_data_to_df(only_column_uuids=[column.config['uuid']])
        # compose the message
        msg = Message(
            subject="Auto backup for {}".format(column.config['name']),
            recipients=email,
            body="Data backup from SRES for column {} in list {} is attached.".format(
                column.config['name'],
                column.table.get_full_name()
            ),
            sender=("SRES NoReply", app.config['SRES']['NOREPLY_EMAIL'])
        )
        msg.attach(
            filename='{}_{}'.format(
                utils.clean_uuid(column.config['name']),
                datetime.now().strftime('%Y%m%d_%H%M%S')
            ),
            content_type='text/csv',
            data=export_data['buffer'].getvalue()
        )
        # send message
        with app.mail.record_messages() as outbox:
            send_result = app.mail.send(msg)
            print(outbox)
            print(outbox[0].html)
        del app
        job.release_claim()
        return True
    else:
        del app
        return False


def get_all_tags_for_a_multi_entry_column(column_uuid):
  db = _get_db()
  column = db.columns.find_one({'uuid': column_uuid})
  multi_entry_labels_to_tags = {}
  for multi_entry_option in column['multi_entry']['options']:
    if 'tags' in multi_entry_option:
      multi_entry_labels_to_tags[multi_entry_option['label']] = list(db.tags.find({'_id': {'$in': multi_entry_option['tags']}}).sort([('name',1)]))
    else:
      multi_entry_labels_to_tags[multi_entry_option['label']] = []
  return multi_entry_labels_to_tags

def get_all_tag_ids_for_a_set_of_columns(column_uuids):
  db = _get_db()
  list_of_tag_ids = []
  for coluuid in column_uuids:
    mongotag = db.columns.find_one({'uuid': coluuid})
    if 'tags' in mongotag:
      tag_ids = mongotag['tags']
      for tag_id in tag_ids:
        if tag_id not in list_of_tag_ids:
          list_of_tag_ids.append(tag_id)
    if mongotag['type'] == 'multiEntry':
      for multientry in mongotag['multi_entry']['options']:
        if 'tags' in multientry:
          for tag_id in multientry['tags']:
            if tag_id not in list_of_tag_ids:
              list_of_tag_ids.append(tag_id)
  return list_of_tag_ids

def get_all_tag_names_for_a_set_of_columns(column_uuids):
  db = _get_db()
  list_of_tag_ids = get_all_tag_ids_for_a_set_of_columns(column_uuids)
  list_of_tag_names = []
  tags = db.tags.find({'_id': {'$in': list_of_tag_ids}}).sort([('_id', 1)])
  for tag in tags:
    list_of_tag_names.append(tag['name'])
  return list_of_tag_names



def sum_and_save_tags_for_a_set_of_columns(tableuuid, column_uuids, new_column_uuid):
  db = _get_db()

  # first - get all tags possible for these columns
  list_of_tag_ids = get_all_tag_ids_for_a_set_of_columns(column_uuids)
  list_of_tag_ids.sort()

  tag_to_maximumValue = {}
  for coluuid in column_uuids:
    thecolumn = db.columns.find_one({'uuid': coluuid})
    if 'maximumValue' in thecolumn['custom_options']:
      maximumValue = thecolumn['custom_options']['maximumValue']
    else:
      maximumValue = 0
    if 'tags' in thecolumn:
      tag_ids      = thecolumn['tags']
    else:
      tag_ids      = []
    for tag_id in tag_ids:
      if tag_id not in tag_to_maximumValue.keys():
        tag_to_maximumValue[tag_id] = 0
      try:
        tag_to_maximumValue[tag_id] += float(maximumValue)
      except:
        print("maximumValue isn't a float")
    if thecolumn['type'] == 'multiEntry':
      for multientry in thecolumn['multi_entry']['options']:
        if 'tags' in multientry:
          for tag_id in multientry['tags']:
            if tag_id not in tag_to_maximumValue.keys():
              tag_to_maximumValue[tag_id] = 0
            if 'maximumValue' in multientry:
              try:
                tag_to_maximumValue[tag_id] += float(multientry['maximumValue'])
              except:
                print("maximumValue isn't a float")


  # then - add up data and put in database
  dict_of_cols_to_tags = {}
  for coluuid in column_uuids:
    thecolumn = db.columns.find_one({'uuid': coluuid})
    if 'tags' in thecolumn:
      dict_of_cols_to_tags[coluuid] = thecolumn['tags']
    if thecolumn['type'] == 'multiEntry':
      ii=0
      for multientry in thecolumn['multi_entry']['options']:
        if 'tags' in multientry:
          dict_of_cols_to_tags[coluuid + '.' + str(ii)] = multientry['tags']
        ii+=1

  data_dict = {}
  for coluuid, tags in dict_of_cols_to_tags.items():
    tag_number=0
    for tag in list_of_tag_ids:
      if tag in tags: # this column is tagged with this tag
        for data in db.data.find({'table_uuid': tableuuid}):
          if data['_id'] not in data_dict:
            data_dict[data['_id']] = [0] * len(list_of_tag_ids) * 2
          if coluuid in data:
            thevalue = data[coluuid]
            try:
              data_dict[data['_id']][2*tag_number] += float(thevalue)
            except:
              print('Not a float')
          split_coluuid = coluuid.split('.')
          if len(split_coluuid) == 2:
            plain_column_uuid =     split_coluuid[0]
            multientry_number = int(split_coluuid[1])
            if plain_column_uuid in data:
              mydata = data[plain_column_uuid]
              if type(mydata) is str:
                mylist = mydata[1:-2].replace('"','').split(',') # convert string to list
              elif type(mydata) is list:
                mylist = mydata
              if mylist:
                try:
                  data_dict[data['_id']][2*tag_number] += float(mylist[multientry_number])
                except:
                  print('Not a float')
              else:
                print('mydata is neither a string nor a list')
          if tag_to_maximumValue[tag] != 0: # don't divide by zero
            data_dict[data['_id']][2*tag_number+1] = round(data_dict[data['_id']][2*tag_number] / tag_to_maximumValue[tag] * 100, 1)
      tag_number+=1
  for student, summedtagvalues in data_dict.items():
    # TODO REFACTOR - nothing should write directly to db.data but instead must run through StudentData.set_data()
    db.data.update({'_id': student}, {'$set': {new_column_uuid: json.dumps(summedtagvalues)}})


def add_tag_to_column(column_uuid, tag_objectid):
    db = _get_db()
    db.columns.update({'uuid': column_uuid}, {'$push': {'tags': ObjectId(tag_objectid)}})

def remove_tag_from_column(column_uuid, tag_objectid):
    db = _get_db()
    db.columns.update({'uuid': column_uuid}, {'$pull': {'tags': ObjectId(tag_objectid)}})


def add_tag_to_multientry(column_uuid, tag_objectid, multientry_label):
    db = _get_db()
    multientry_label_number=0
    for a_multientry_label in db.columns.find({'uuid': column_uuid})[0]['multi_entry']['options']:
      if a_multientry_label['label'] == multientry_label:
        db.columns.update({'uuid': column_uuid}, {'$push': {'multi_entry.options.' + str(multientry_label_number) + '.tags': ObjectId(tag_objectid)}})
      multientry_label_number+=1

def remove_tag_from_multientry(column_uuid, tag_objectid, multientry_label):
    db = _get_db()
    multientry_label_number=0
    for a_multientry_label in db.columns.find_one({'uuid': column_uuid})['multi_entry']['options']:
      if a_multientry_label['label'] == multientry_label:
        db.columns.update({'uuid': column_uuid}, {'$pull': {'multi_entry.options.' + str(multientry_label_number) + '.tags': ObjectId(tag_objectid)}})
      multientry_label_number+=1


def get_columns_with_tag_in_table(table_uuid, tag_objectid):
    db = _get_db()
    results = []
    for column in db.columns.find({'table_uuid': table_uuid, 'tags': {'$all': [tag_objectid]}}):
      results.append(column['name'])
    i=0
    while i<50: # multi entry columns with more than 50 multi entry components won't work properly here
      for column in db.columns.find({'table_uuid': table_uuid, 'type': 'multiEntry', 'multi_entry.options.' + str(i) + '.tags': {'$all': [tag_objectid]}}):
        results.append(column['name'] + ' >> ' + column['multi_entry']['options'][i]['label'])
      i+=1
    return results

def load_multiple_columns_by_uuids(uuids, default_table):
    """Loads multiple Column instances.
    
        uuids (list) List of string uuids (not column references)
        default_table (Table) Loaded Table instance
        
        Returns a dict of loaded Column instances, keyed by uuid
    """
    loaded_columns = {}
    db = _get_db()
    _uuids = []
    for uuid in uuids:
        _uuids.append(utils.clean_uuid(uuid))
    _uuids = list(set(_uuids))
    # get from db
    results = db.columns.find({
        'uuid': {'$in': _uuids}
    })
    results = list(results)
    # load
    for result in results:
        _uuid = result['uuid']
        loaded_columns[_uuid] = Column(default_table)
        if loaded_columns[_uuid].load(_uuid, preloaded_db_result=[result]):
            # all good
            pass
        else:
            del loaded_columns[_uuid]
    return loaded_columns

def _is_student_direct_access_allowed(config, mode='single'):
    if mode == '__any__':
        return _is_student_direct_access_allowed(config, 'single') or _is_student_direct_access_allowed(config, 'roll')
    else:
        if mode == 'single':
            return config['custom_options']['student_direct_access'] == 'allow' or 'single' in config['custom_options']['student_direct_access']
        elif mode == 'roll':
            return 'roll' in config['custom_options']['student_direct_access']
    return False

class Column:
    
    default_config = {
        '_referenced_column_references': [],
        'uuid': None,
        'table_uuid': None,
        'type': None,
        'multientry_data_format': False,
        'name': '',
        'system_name': '',
        'tags': [],
        'description': '',
        'datasource': {
            'mode': 'manual', # manual|sync
            'type': '', # e.g. manual or lms
            'name': '' # e.g. canvas
        },
        'active': {
            'from': None, # will be a datetime
            'to': None, # will be a datetime
            'from_time': '00:00:00',
            'to_time': '23:59:59',
            'range_from_time': '00:00:00',
            'range_to_time': '23:59:59'
        },
        'aes_key': None,
        'simple_input': {
            'allow_free': 'false',
            'allow_free_regex': '.*',
            'options': []
        },
        'notify_email': {
            'active': 'false',
            'body': '<p>Dear $PREFERREDNAME$</p><p>A record of $DATA$ has been saved.</p>',
            'subject': 'Data saved for $UOSCODE$'
        },
        'coversheet': {
            'html': ''
        },
        'apply_to_others': {
            'active': 'false',
            'other_columnuuid': ''
        },
        'counter': {
            'max': 4,
            'increment': 1
        },
        'auto_proceed': 'false',
        'quick_info': {
            'single': '<h3>$PREFERREDNAME$ $SURNAME$</h3><p>$COLUMNNAME$</p>',
            'bulk': '$PREFERREDNAME$ $SURNAME$'
        },
        'multi_entry': {
            'options': [{
                'type': 'regex',
                'regex': '.*',
                'label': '',
                'required': '0',
                'selectmode': 'single',
                'select_display_mode': 'btn-group',
                'select': [],
                'editing_allowed_by': 'anyone'
            }]
        },
        'file_link': 'false',
        'sign_in_out': {
            'week_start': None,
            'on_out': 'clear',
            'message_welcome': "<h1>Welcome, you have been signed in.</h1>",
            'message_goodbye': "<h1>Goodbye, you have been signed out.</h1>"
        },
        'auto_reset': {
            'active': 'false',
            'time': datetime(2019,1,1,2,0),
            'value': ''
        },
        'auto_backup_email': {
            'active': 'false',
            'interval_minutes': 59,
            'start_time': datetime(2019,1,1,9,0),
            'end_time': datetime(2019,1,1,17,0),
            'email_target': ''
        },
        'custom_options': {
            'quickinfo_rollview': '',
            'quickinfo_rollview_header': 'Info',
            'quickinfo_rollview_priority': '',
            'reload_quickinfo_upon_saving': 'false',
            'select_from_list_mode': 'single',
            'textentry_size': 'textbox',
            'datatype_image_max_dimension': 200,
            'datatype_file_multiple_number': 1,
            'datatype_file_allowed_extensions': '',
            'datatype_file_max_bytes': 5242880,
            'grouping_column': '',
            'student_direct_access': 'deny',
            'restrict_by_username_column': '',
            'only_show_condition_column': '',
            'only_show_condition_operator': '',
            'only_show_condition_value': '',
            'time_delay_lockout_duration': 0,
            'entry_page_navbar_brand_text': "Enter data",
            'show_scan_identifier_button': 'true',
            'show_identifier_entry_box': 'true',
            'allow_identifier_entry_search': 'true',
            'show_name_when_searching': 'show',
            'show_person_navigation_buttons': 'false',
            'hide_clear_record_button': 'false',
            'hide_data_exists_notice': 'false',
            'focus_identifier_entry_box_after_save': 'focus',
            'rollview_popout_editor': 'inline',
            'rollview_display_raw_data_column': 'false',
            'rollview_display_name_columns': 'show',
            'rollview_display_group_column': 'show',
            'rollview_popout_editor_title': '$SID$',
            'rollview_display_change_edit_button_colour': 'false',
            'rollview_reload_quickinfo_upon_saving': 'false',
            'rollview_display_identifier_column': 'show',
            'rollview_pagination_page_length_default': '30',
            'rollview_data_entry_header': 'Data entry',
            'additional_identifier': 'false',
            'allow_html': 'false',
            'load_existing_data': 'latest',
            #'peer_data_entry': 'disabled',
            'student_data_entry_trigger_apply_to_others': 'disabled',
            'student_data_entry_if_student_unknown': 'disallow',
            'peer_data_entry_condition_column': '',
            'peer_data_entry_condition_operator': '',
            'peer_data_entry_condition_value': '',
            'peer_data_entry_match_column': '',
            'peer_data_entry_match_operator': 'same',
            # tag aggregation options
            'maximumValue': '',
            'use_for_tag_aggregation': 'false',
            'multiple_reports_mode': 'disabled',
            'newline_character_conversion': 'disabled', # what to do with \r \n characters
            'mojibake_conversion': 'enabled'
        },
        'aggregated_by': [],
        'aggregation_options': {
			'method': 'average',
			'attributes': [],
			'recalculate_trigger': 'onset',
			'aggregator_type_mapper_inputs': [], # stored as parsed lists to save parsing during aggregation calculation
			'aggregator_type_mapper_outputs': [],
			'aggregator_type_mathematical_operations_formula': '',
            'aggregator_type_case_builder_cases': [],
            'aggregator_type_self_peer_review_grouping_column': '',
            'aggregator_type_self_peer_review_score_column': '',
			'rounding': '',
            'rounding_direction': 'nearest',
            'post_aggregation_arithmetic_operator': '',
            'post_aggregation_arithmetic_value': '',
            'regex_replace_pattern': '',
            'regex_replace_replacement': '',
            'regex_replace_mode': 'text',
            'axes': ['x'], # x (row-wise, normal approach), t (change_history-wise)...
            't_axis_source': 'all',
            't_axis_source_limit': 'no',
            't_axis_source_limit_from': datetime.now(),
            't_axis_source_limit_to': datetime.now(),
            'blank_handling': 'leave'
        },
        'permissions': {
			'edit': {
				'user': {
					'mode': 'allow',
					'except': []
				},
				'student': {
					'mode': 'deny',
					'except': []
				}
			},
			'view': {
				'user': {
					'mode': 'allow',
					'except': []
				},
				'student': {
					'mode': 'allow',
					'except': []
				}
			}
        },
        'workflow_state': 'active'
    }
    
    def __init__(self, preloaded_table=None):
        from sres.tables import Table
        self.db = _get_db()
        # define instance variables
        self.preloaded_table = preloaded_table
        if preloaded_table:
            self.table = preloaded_table
        else:
            self.table = Table()
        self.config = deepcopy(self.default_config)
        self._id = None
        self.subfield = None # 0-based
        self.subfield_label = ''
        self.subfield_type = ''
        self.magic_formatter = None
        self.is_system_column = False
        self.column_reference = None
        self.column_loaded = False
        self.is_collective_asset = False
        self.has_tags = False

    def load(self, column_reference=None, column_oid=None, default_table_uuid=None, preloaded_db_result=None):
        filter = {}
        if column_reference and column_reference.lower() in [col['name'].lower() for col in SYSTEM_COLUMNS] + [col['insert_value'].lower() for col in SYSTEM_COLUMNS] + ['status']:
            self.is_system_column = True
            self.column_reference = column_reference
        else:
            if column_reference:
                # parse any multientry subfields
                parsed_reference = parse_multientry_column_reference(column_reference)
                if not parsed_reference['simple']:
                    self.subfield = parsed_reference['field_number']
                    column_uuid = parsed_reference['base_column_uuid']
                else:
                    column_uuid = column_reference
                # parse any magic formatters
                if len(column_reference.split('.')) >= (3 if self.subfield is not None else 2):
                    self.magic_formatter = column_reference.split('.')[(2 if self.subfield is not None else 1)]
                    column_uuid = column_reference.split('.')[0]
            # search db
            if column_reference and column_uuid is not None:
                filter['uuid'] = column_uuid
            if column_oid is not None:
                filter['_id'] = column_oid
            if preloaded_db_result is None:
                result = self.db.columns.find(filter)
                result = list(result)
            else:
                result = preloaded_db_result
            if len(result) == 1:
                if not self.preloaded_table or self.preloaded_table.config['uuid'] != result[0].get('table_uuid'):
                    self.table.load(result[0]['table_uuid'])
                self._id = result[0]['_id']
                for key, value in self.default_config.items():
                    try:
                        if isinstance(self.config[key], collections.Mapping):
                            # is dict-type so try and merge
                            self.config[key] = {**value, **result[0][key]}
                        else:
                            self.config[key] = result[0][key]
                    except:
                        self.config[key] = value
                if 'tags' in result[0]:
                    self.has_tags = True
            else:
                return False
            # final multientry configs
            if self.subfield is not None and (self.config['type'] == 'multiEntry' or self.config.get('multientry_data_format') == True):
                if len(self.config['multi_entry']['options']) <= self.subfield:
                    return False
                self.subfield_label = self.get_multientry_labels()[self.subfield]
                self.subfield_type = self.get_multientry_subfield_types()[self.subfield]
            # fixing some config types
            if isinstance(self.config['aggregation_options']['attributes'], str):
                self.config['aggregation_options']['attributes'] = self.config['aggregation_options']['attributes'].split(',')
            # correcting for active dates/times to accommodate legacy where only date was set
            if self.config['active']['from_time'] == '00:00:00' and self.config['active']['to_time'] == '23:59:59':
                if self.config['active']['from'] is not None and self.config['active']['to'] is not None:
                    try:
                        self.config['active']['from'] = datetime.combine(
                            self.config['active']['from'].date(),
                            time(0,0,0)
                        )
                        self.config['active']['to'] = datetime.combine(
                            self.config['active']['to'].date(),
                            time(23,59,59)
                        )
                    except Exception as e:
                        logging.exception(e)
            # save column reference
            self.column_reference = column_reference
        # check if table is loaded
        if self.is_system_column:
            if not self.table.config['uuid']:
                # need to load a table
                if default_table_uuid:
                    self.table.load(default_table_uuid)
                else:
                    # problem because no table is specified!
                    return False
            else:
                # OK
                pass
        else:
            if not self.table.config['uuid']:
                if default_table_uuid:
                    self.table.load(default_table_uuid)
            elif self.table.config['uuid'] != self.config['table_uuid']:
                self.table.load(self.config['table_uuid'])

        # return
        self.column_loaded = True
        return True
    
    def update(self, override_username=None):
        if self.table.is_user_authorised(username=override_username):
            self.config['_referenced_column_references'] = self.get_referenced_column_references()
            result = self.db.columns.update_one({'uuid': self.config['uuid']}, {'$set': self.config})
            return result.acknowledged
        else:
            return False
    
    def create(self, table_uuid, override_username=None):
        """
            Creates a new column in the provided table_uuid (string) and 
            returns the new column uuid (string) if successful or None if not
        """
        if self.table.load(table_uuid):
            if self.table.is_user_authorised(username=override_username):
                # build and save column into db.columns
                self.config['table_uuid'] = self.table.config['uuid']
                self.config['uuid'] = 'COL_{}'.format(utils.create_uuid())
                result = self.db.columns.insert_one({**self.config, **{'table_uuid': self.table.config['uuid']}})
                self._id = result.inserted_id
                # add column into db.data because why not
                result = self.db.data.update_one(
                    {'table_uuid': self.config['table_uuid']}, 
                    {'$set': {self.config['uuid']: ''}}
                )
                # return
                self.load(self.config['uuid'])
                return self.config['uuid']
            else:
                return None
        else:
            return None
    
    def delete(self, override_username=None):
        if self.table.is_user_authorised(username=override_username):
            result = self.db.columns.update_one({'uuid': self.config['uuid']}, {'$set': {'workflow_state': 'deleted'}})
            return result.acknowledged
        return False
    
    def clone(self, target_table_uuid=None, add_cloned_notice=True, set_user_as_sole_administrator=False, override_username=None):
        """
            Clones the current column into current column's list, or an alternate table
            specified by target_table_uuid (string, uuid).
            
            target_table_uuid (str uuid)
            add_cloned_notice (boolean)
            set_user_as_sole_administrator (boolean) actually ignored
            
            Returns None if unsuccessful, otherwise returns uuid (string) of newly cloned column.
        """
        column_clone = Column()
        if add_cloned_notice:
            cloned_notice = " [Cloned column]"
        else:
            cloned_notice = ''
        if column_clone.create(target_table_uuid or self.config['table_uuid'], override_username=override_username):
            source_column_config = deepcopy(self.config)
            # remove keys that should not be cloned
            del source_column_config['table_uuid']
            del source_column_config['uuid']
            source_column_config['name'] = '{}{}'.format(
                source_column_config['name'],
                cloned_notice
            )
            source_column_config['description'] = '{}{}'.format(
                source_column_config['description'],
                cloned_notice
            )
            column_clone.config = {**column_clone.config, **source_column_config}
            if column_clone.update(override_username=override_username):
                return column_clone.config['uuid']
        return None

    def get_tags(self):
        # this assumes that this is NOT a multi-entry column
        db = _get_db()
        if self.has_tags:
          tag_ids = db.columns.find_one({'_id': self._id})['tags']
          return list(db.tags.find({'_id': {'$in': tag_ids}}))
    
    def get_friendly_name(self, show_table_info=True, get_text_only=False):
        """
            Returns string with friendly name of current column.
        """
        table_dict = {
            'config': self.table.config,
            '_id': self.table._id
        }
        column_dict = {
            'subfield': self.subfield,
            'subfield_label': self.subfield_label,
            'config': self.config,
        }
        return get_friendly_column_name(
            show_table_info=show_table_info,
            get_text_only=get_text_only,
            table=table_dict,
            column=column_dict
        )
        #s = ''
        #if show_table_info and self.table._id:
        #    s = s + '{} {} S{} - '.format(self.table.config['code'], self.table.config['year'], self.table.config['semester'])
        #s = s + '{}'.format(self.config['name'])
        #if self.subfield is not None:
        #    if get_text_only:
        #        subfield_label = BeautifulSoup(self.subfield_label, 'html.parser').get_text()
        #    else:
        #        subfield_label = self.subfield_label
        #    s = s + ' >> {}'.format(subfield_label)
        #if self.config['type'] == 'multiEntry' and self.subfield is None:
        #    s = s + ' [Full dataset]'
        #return s
    
    def is_active(self):
        active_from = parser.parse(self.config['active']['from']) if isinstance(self.config['active']['from'], str) else self.config['active']['from']
        active_to = parser.parse(self.config['active']['to']) if isinstance(self.config['active']['to'], str) else self.config['active']['to']
        try:
            range_from_time = datetime.strptime(self.config['active']['range_from_time'], '%H:%M:%S').time()
            range_to_time = datetime.strptime(self.config['active']['range_to_time'], '%H:%M:%S').time()
        except Exception as e:
            try:
                range_from_time = parser.parse(self.config['active']['range_from_time']).time()
                range_to_time = parser.parse(self.config['active']['range_to_time']).time()
            except Exception as e:
                logging.exception(e)
                range_from_time = time(0,0,0)
                range_to_time = time(23,59,59)
        if active_from is None or active_to is None:
            # print('error is_active', active_from, active_to, self.config['uuid'], self.config['name'])
            return False
        if (active_from <= datetime.now() <= active_to) and (range_from_time <= datetime.now().time() <= range_to_time):
            return True
        else:
            return False
    
    def get_multientry_labels(self, get_text_only=False):
        ret = []
        for option in self.config['multi_entry']['options']:
            _label = option['label'] if 'label' in option else ''
            if get_text_only:
                _label = BeautifulSoup(_label, 'html.parser').get_text()
            ret.append(_label)
        return ret

    def get_multientry_information(self):
        return self.config['multi_entry']['options']
    
    def get_multientry_subfield_types(self):
        ret = []
        for subfield in self.config['multi_entry']['options']:
            ret.append(subfield.get('type', ''))
        return ret
    
    def get_datatype_friendly(self):
        if self.config['type'] in COLUMN_DATA_TYPES_META.keys():
            return COLUMN_DATA_TYPES_META[self.config['type']]
        return {
            'name': 'Unknown',
            'description': 'Could not determine column type'
        }
    
    def get_select_from_list_elements(self, multi_entry_subfield=None):
        """
            Gets the elements/items for 'select from list' buttons or dropdowns
            Returns a list of dicts {display, value}.
        """
        ret = []
        if self.config['type'] == 'teacherallocation':
            auth_users = self.table.get_authorised_usernames()
            auth_users = sorted(auth_users['administrators'] + auth_users['users'])
            for auth_user in auth_users:
                ret.append({
                    'display': auth_user,
                    'value': auth_user
                })
        elif (self.config['type'] == 'multiEntry' or self.config.get('multientry_data_format') == True):
            if multi_entry_subfield is None and self.subfield is not None:
                multi_entry_subfield = self.subfield
            if multi_entry_subfield < len(self.config['multi_entry']['options']):
                if self.config['multi_entry']['options'][multi_entry_subfield].get('select') is None:
                    pass # do nothing, nothing there
                elif isinstance(self.config['multi_entry']['options'][multi_entry_subfield]['select'], list):
                    ret = self.config['multi_entry']['options'][multi_entry_subfield]['select']
                    for i, r in enumerate(ret):
                        for k, v in r.items():
                            if v is None:
                                ret[i][k] = ''
                else:
                    for select_element in self.config['multi_entry']['options'][multi_entry_subfield]['select'].split(','):
                        ret.append({
                            'display': select_element,
                            'value': select_element
                        })
        else:
            if isinstance(self.config['simple_input']['options'], list):
                ret = self.config['simple_input']['options']
            else:
                for option in self.config['simple_input']['options'].split(','):
                    ret.append({
                        'display': option,
                        'value': option
                    })
        # return
        return ret
    
    def get_select_from_list_element_index(self, value, multi_entry_subfield=None):
        elements = self.get_select_from_list_elements(multi_entry_subfield=multi_entry_subfield)
        for i, element in enumerate(elements):
            logging.debug('comparing {} to {}'.format(element['value'], value))
            if str(element['value']) == str(value):
                logging.debug('ok')
                return i
        return None
    
    def is_user_authorised(self, username=None, authorised_roles=[], _request=None, sdak=None, sda_mode='single', user_oid=None, skip_global_admin_check=False):
        """
            Returns (boolean) if specified username (string) is authorised
            for role(s) in authorised_roles (list of strings user|auditor|administrator)
            
            username (string|None)
            authorised_roles (list of strings) user|auditor|administrator
            [deprecated] _request (Flask Request object) Used for passing through request.args['sdak']
            sdak (string|None) 
            user_oid
            skip_global_admin_check (bool)
        """
        if not username:
            username = get_auth_user()
        # if user is table admin, allow all
        if self.table.is_user_authorised(username=username, user_oid=user_oid, skip_global_admin_check=skip_global_admin_check):
            # is table administrator
            return True
        # if this is a collective asset, allow according to permissions of collective asset
        if self.config['workflow_state'] == 'collective':
            from sres.collective_assets import CollectiveAsset
            collective_asset = CollectiveAsset()
            if collective_asset.load_from_source_asset('column', self.config['uuid']) and (collective_asset.is_user_authorised_viewer() or collective_asset.is_user_authorised_editor()):
                return True
            else:
                return False
        # if column is a system column, disallow
        if self.is_system_column:
            return False
        # if user-level staff are being checked
        if 'user' in authorised_roles:
            if self.table.is_user_authorised(username=username, user_oid=user_oid, categories=['user'], skip_global_admin_check=skip_global_admin_check):
                if self.config['permissions']['edit']['user']['mode'] == 'deny' and username in self.config['permissions']['edit']['user']['except']:
                    return True
                elif self.config['permissions']['edit']['user']['mode'] == 'allow' and username not in self.config['permissions']['edit']['user']['except']:
                    return True
        # check student direct access
        if self.is_user_authorised_for_sda(sdak, sda_mode):
            return True
        return False
    
    def is_user_authorised_for_sda(self, sdak, sda_mode, override_user_identifier=None):
        if override_user_identifier is None:
            override_user_identifier = get_auth_user()
        # check student direct access
        if (sdak is not None and sdak != '') and isinstance(sdak, str) and self.is_student_direct_access_allowed(mode=sda_mode):
            # if user is student and student_direct_access enabled and column is student editable then true
            if (self.is_student_direct_access_allowed(mode=sda_mode) or self.is_peer_data_entry_enabled()) and self.is_writeable_by_students():
                if sdak.upper() == self.get_student_direct_access_key().upper():
                    from sres.studentdata import StudentData
                    student_data = StudentData(self.table)
                    if student_data.find_student(override_user_identifier):
                        return True
                    else:
                        pass
                else:
                    pass
        return False
    
    def get_grouping_column_unique_groups(self, restrict_by_username_column='', sdak=None, override_column_reference=None):
        """Returns a list of strings of the unique groups in this column's grouping column.
            
            restrict_by_username_column (string, column UUID)
            sdak (string|None
            override_column_reference (string | None) Some other column reference to use instead of the default one.
            
        """
        if override_column_reference is not None:
            grouping_column_reference = override_column_reference
        else:
            grouping_column_reference = self.config['custom_options']['grouping_column']
        restrict_by_username_column = utils.clean_uuid(restrict_by_username_column)
        grouping_column = Column()
        restrictor_column = Column()
        # fetch groups from db
        if grouping_column_reference and grouping_column.load(grouping_column_reference):
            filters = []
            filters.append({'status': 'active'})
            filters.append({'table_uuid': self.table.config['uuid']})
            if restrict_by_username_column and restrictor_column.load(restrict_by_username_column):
                filters.append({
                    restrictor_column.config['uuid']: {'$regex': '{}'.format(get_auth_user()), '$options': 'i'}
                })
            if sdak and self.is_peer_data_entry_restricted():
                from sres.studentdata import StudentData
                student_data = StudentData(self.table)
                if student_data.find_student(get_auth_user()):
                    filters.extend(self.get_db_filter_restrictors_for_peer_data_entry(preloaded_student_data=student_data))
                else:
                    # problem...
                    logging.warning('Student [{}] tried to access columns.get_grouping_column_unique_groups for [{}] but failed to load'.format(get_auth_user(), self.config['uuid']))
                    return []
            # parse groups
            results = self.db.data.find({'$and': filters}, [ grouping_column.config['uuid'] ])
            results = list(results)
            if len(results) > 0:
                # parse multientry if needed
                return_array = []
                if grouping_column.subfield is not None:
                    values_array = []
                    for result in results:
                        try:
                            values_array.append(json.loads(result[grouping_column.config['uuid']])[grouping_column.subfield])
                        except:
                            logging.error(f'Could not get grouping value from subfield. {result[override_column_reference]}')
                    values_array = list(set(values_array))
                    return_array = values_array
                else:
                    # get array of potential values
                    for result in results:
                        return_array = utils.force_interpret_str_to_list(result.get(grouping_column.config['uuid'], ''), return_array)
                # sort and return
                return_array = natsorted(return_array, alg=ns.IGNORECASE)
                return return_array
            else:
                return []
        else:
            return []
    
    def get_referenced_column_references(self, order_by_prevalence=True, deduplicate=True, uuids_only=False):
        """
            Returns a list of string references of columns that are referenced in the current filter.
        """
        all_column_references = self.get_references_to_other_columns()['_raw']
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
    
    def get_referenced_table_uuids(self, order_by_prevalence=True, deduplicate=True, user_must_be_admin=False):
        """
            Returns a list of string uuids of tables that correspond to columns referenced
            in the current column.
        """
        all_column_references = self.get_referenced_column_references(order_by_prevalence, deduplicate)
        # figure out tables
        all_table_uuids = table_uuids_from_column_references(all_column_references, user_must_be_admin)
        return all_table_uuids
    
    def get_references_to_other_columns(self, direction='in'):
        """
            Finds columns that this column references/uses/depends on.
            
            direction (string):
                in = find columns that this column references/uses/depends on
                out (not fully implemented) = find columns that reference/use/depend on this column
                all (not fully implemented) = both
        """
        from sres.aggregatorcolumns import AggregatorColumn
        ret = {
            '_all': [],
            '_all_uuids': [],
            '_all_uuids_from_other_tables': [],
            '_for_js': [],
            '_raw': []
        }
        column_pattern = utils.BASE_COLUMN_REFERENCE_PATTERN
        raw_references = {}
        # notify email
        raw_references['notify_email_body'] = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['notify_email']['body'])
        raw_references['notify_email_subject'] = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['notify_email']['subject'])
        # coversheet
        raw_references['coversheet_html'] = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['coversheet']['html'])
        # quick info
        raw_references['quick_info'] = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['quick_info']['single'])
        raw_references['quick_info_bulk'] = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['quick_info']['bulk'])
        raw_references['quick_info_roll'] = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.config['custom_options']['quickinfo_rollview'])
        # apply to others
        raw_references['apply_to_others'] = re.findall(utils.BASE_COLUMN_REFERENCE_PATTERN, self.config['apply_to_others']['other_columnuuid'])
        # restrict_by_username_column
        raw_references['restrict_by_username_column'] = re.findall(utils.BASE_COLUMN_REFERENCE_PATTERN, self.config['custom_options']['restrict_by_username_column'])
        # grouping_column
        raw_references['quick_info_roll'] = re.findall(utils.BASE_COLUMN_REFERENCE_PATTERN, self.config['custom_options']['grouping_column'])
        # aggregated by i.e. aggregator column(s) that use the current column as a source
        if direction != 'in':
            raw_references['aggregated_by'] = self.config['aggregated_by']
        # aggregation options
        aggregator_column = AggregatorColumn()
        if aggregator_column.load(self.config['uuid']):
            raw_references['aggregation_sources'] = aggregator_column._get_aggregated_column_references()
        
        # parse them all
        for raw_reference_type, references in raw_references.items():
            references = utils.clean_delimiter_from_column_references(references)
            ret[raw_reference_type] = []
            ret['_raw'].extend(references)
            for raw_reference in references:
                column = Column()
                if column.load(raw_reference):
                    column_struct = {
						'columnuuid': column.config['uuid'],
						'subfield': column.subfield
					}
                    ret[raw_reference_type].append(column_struct)
                    ret['_all'].append(column_struct)
                    ret['_all_uuids'].append(column.config['uuid'])
                    # connection objects for js
                    current_connection_obj = {
                        'target': self.config['uuid'],
                        'source': column.config['uuid']
                    }
                    if current_connection_obj not in ret['_for_js']:
                        ret['_for_js'].append(current_connection_obj)
                    # see if from another table
                    if self.config['table_uuid'] != column.config['table_uuid']:
                        ret['_all_uuids_from_other_tables'].append(column.config['uuid'])
                del column
        
        return ret
    
    def is_writeable_by_students(self, username=None):
        if self.is_system_column:
            return False
        if not username:
            username = get_auth_user()
        if self.is_active():
            if self.config['permissions']['edit']['student']['mode'] == 'deny' and username in self.config['permissions']['edit']['student']['except']:
                return True
            elif self.config['permissions']['edit']['student']['mode'] != 'deny' and username not in self.config['permissions']['edit']['student']['except']:
                return True
            else:
                return False
        return False
    
    def is_student_editable(self):
        return self.config['permissions']['edit']['student']['mode'] != 'deny'
    
    def is_student_direct_access_allowed(self, mode='single'):
        return _is_student_direct_access_allowed(self.config, mode)
        #if mode == 'single':
        #    return self.config['custom_options']['student_direct_access'] == 'allow' or 'single' in self.config['custom_options']['student_direct_access']
        #elif mode == 'roll':
        #    return 'roll' in self.config['custom_options']['student_direct_access']
        #return False
    
    def is_self_data_entry_enabled(self):
        if 'self' in self.config['permissions']['edit']['student']['mode'] or 'allow' in self.config['permissions']['edit']['student']['mode']:
            return True
        return False
    
    def can_student_trigger_apply_to_others(self):
        return self.config['custom_options']['student_data_entry_trigger_apply_to_others'] == 'enabled'
    
    def is_peer_data_entry_enabled(self):
        return 'peer' in self.config['permissions']['edit']['student']['mode']
    
    def is_peer_data_entry_restricted(self):
        if self.config['custom_options']['peer_data_entry_condition_column'] != '' or self.config['custom_options']['peer_data_entry_match_column'] != '':
            return True
        return False
    
    def is_only_show_condition_enabled(self):
        """Are we only showing students who have certain data in a specified column?"""
        if self.config['custom_options']['only_show_condition_column']:
            return True
        return False
    
    def is_peer_data_entry_allowed(self, student_data_requestor, student_data_target):
        """Checks to see if the 'saver/requestor' (current user) is allowed to save data for the 'target'.
        
            student_data_requestor (StudentData, loaded)
            student_data_target (StudentData, loaded)
        """
        # check if requestor is allowed to enter data for themselves
        if student_data_requestor.config['sid'] == student_data_target.config['sid'] and not self.is_self_data_entry_enabled():
            return False
        # check if requestor is allowed to enter data for others
        allowed = {}
        # check condition column
        _condition_column = self.config['custom_options']['peer_data_entry_condition_column']
        if _condition_column:
            allowed['condition_column'] = False
            _value = self.config['custom_options']['peer_data_entry_condition_value']
            _operator = self.config['custom_options']['peer_data_entry_condition_operator']
            _current_data = student_data_target.get_data(_condition_column, do_not_deserialise=True).get('data')
            if _operator == 'eq' and _current_data == _value:
                allowed['condition_column'] = True
            elif _operator == 'neq' and _current_data != _value:
                allowed['condition_column'] = True
            elif _operator == 'contains' and _value in _current_data:
                allowed['condition_column'] = True
            elif _operator == 'ncontains' and _value not in _current_data:
                allowed['condition_column'] = True
            elif _operator == 'regex' and len(re.findall(_value, _current_data)):
                allowed['condition_column'] = True
        # check match column
        _match_column = self.config['custom_options']['peer_data_entry_match_column']
        if _match_column:
            allowed['match_column'] = False if self.config['custom_options']['peer_data_entry_match_operator'] == 'same' else True
            if student_data_requestor.get_data(_match_column, do_not_deserialise=True).get('data') == student_data_target.get_data(_match_column, do_not_deserialise=True).get('data'):
                allowed['match_column'] = True if self.config['custom_options']['peer_data_entry_match_operator'] == 'same' else False
        # whether student should only be allowed to edit self
        if self.config['permissions']['edit']['student']['mode'] == 'self':
            if student_data_requestor.config['sid'] != student_data_target.config['sid']:
                allowed['peer_data_entry'] = False
        # check overall
        if False in [ v for k,v in allowed.items() ]:
            return False
        else:
            return True
    
    def _get_db_filter_condition_restrictor(self, operator, condition_column, value):
        if operator == 'eq':
            return {
                condition_column: value
            }
        elif operator == 'neq':
            return {
                condition_column: {'$ne': value}
            }
        elif operator == 'contains':
            return {
                condition_column: {'$regex':'.*' + str(value) + '.*'}
            }
        elif operator == 'ncontains':
            return {
                condition_column: {'$regex':'^(?!.*' + str(value) + ').*$'}
            }
        elif operator == 'regex':
            return {
                condition_column: {'$regex': value}
            }
    
    def get_db_filter_restrictors_for_peer_data_entry(self, preloaded_student_data):
        """Returns a (potentially empty) list of dicts for further filtering db.data.find
            in the case of peer data entry
        """
        additional_filters = []
        # condition column
        _condition_column = self.config['custom_options']['peer_data_entry_condition_column']
        if _condition_column:
            _value = self.config['custom_options']['peer_data_entry_condition_value']
            additional_filters.append(self._get_db_filter_condition_restrictor(
                self.config['custom_options']['peer_data_entry_condition_operator'],
                _condition_column,
                _value
            ))
        # matching
        _match_column = self.config['custom_options']['peer_data_entry_match_column']
        _match_operator = self.config['custom_options']['peer_data_entry_match_operator']
        if _match_column:
            _value = preloaded_student_data.get_data(_match_column, do_not_deserialise=True).get('data')
            if _value is not None:
                if _match_operator == 'different':
                    additional_filters.append({
                        _match_column: {'$ne': _value}
                    })
                else:
                    additional_filters.append({
                        _match_column: _value
                    })
        # whether student should be allowed to see self
        if ('self' not in self.config['permissions']['edit']['student']['mode']) or (not self.is_self_data_entry_enabled()):
            # shouldn't be able to access self
            additional_filters.append({
                'sid': {'$ne': preloaded_student_data.config['sid']}
            })
        # whether student should only be allowed to see self
        if self.config['permissions']['edit']['student']['mode'] == 'self':
            additional_filters.append({
                'sid': preloaded_student_data.config['sid']
            })
        # return
        return additional_filters
        
    def get_db_filter_restrictors_for_only_show(self):
        """Returns a (potentially empty) list of dicts for further filtering db.data.find
            in the case only_show custom options are set.
        """
        additional_filters = []
        # condition column
        _condition_column = self.config['custom_options']['only_show_condition_column']
        if _condition_column:
            _value = self.config['custom_options']['only_show_condition_value']
            additional_filters.append(self._get_db_filter_condition_restrictor(
                self.config['custom_options']['only_show_condition_operator'],
                _condition_column,
                _value
            ))
        # return
        return additional_filters
    
    def get_student_direct_access_key(self):
        return sha256(bytes(
            '{}{}{}'.format(
                self.table.config['uuid'],
                self.config['uuid'],
                current_app.config['SRES']['DEFAULT_SALT']
            )
        , 'utf-8')).hexdigest().upper()
    
    def _get_scheduled_job_id(self, type):
        return _get_scheduled_job_id(self.table.config['uuid'], self.config['uuid'], type)
    
    def check_scheduled_job_exists(self, type):
        job_id = self._get_scheduled_job_id(type)
        job = current_app.scheduler.get_job(job_id)
        if job is not None:
            return True
        else:
            return False
    
    def update_scheduled_job(self, type, **kwargs):
        """
            type (str) reset|backup_data_email
        """
        if type not in ['reset', 'backup_data_email']:
            return False
        job_id = self._get_scheduled_job_id(type)
        if type == 'reset':
            if self.config['auto_reset']['active'] == 'true':
                current_app.scheduler.add_job(
                    cron_reset,
                    args=(self.config['uuid'], kwargs['data']),
                    #trigger='cron',
                    #hour=kwargs['hour'],
                    #minute=kwargs['minute'],
                    trigger='interval',
                    start_date=datetime.combine(date.today(), time(kwargs['hour'], kwargs['minute'])),
                    hours=24,
                    max_instances=1,
                    coalesce=True,
                    id=job_id,
                    replace_existing=True,
                    misfire_grace_time=1800
                )
            else:
                if current_app.scheduler.get_job(job_id):
                    current_app.scheduler.remove_job(job_id)
        elif type == 'backup_data_email':
            if self.config['auto_backup_email']['active'] == 'true':
                every_x_minutes = int(kwargs['interval_minutes'])
                hour_start = kwargs['start_time'].hour
                hour_end = kwargs['end_time'].hour
                if hour_start > hour_end:
                    hour_start, hour_end = hour_end, hour_start
                current_app.scheduler.add_job(
                    cron_backup_data_email,
                    args=(self.config['uuid'], kwargs['email_target']),
                    trigger='cron',
                    hour='{}-{}'.format(hour_start, hour_end),
                    minute='*/{}'.format(every_x_minutes),
                    max_instances=1,
                    coalesce=True,
                    id=job_id,
                    replace_existing=True,
                    misfire_grace_time=1800
                )
            else:
                if current_app.scheduler.get_job(job_id):
                    current_app.scheduler.remove_job(job_id)
        else:
            # job type not recognised
            # log
            logging.warning("Job type not recognised [{}] [{}] [{}]".format(
                type,
                self.table.config['uuid'],
                self.config['uuid']
            ))
            return False
        return True
    
    def get_slider_subfields_config(self):
        slider_subfields = {}
        for i, subfield in enumerate(self.config['multi_entry']['options']):
            if subfield.get('type') == 'slider':
                slider_subfields[i] = {
                    'slider_mode': subfield.get('slider_mode', 'textual'),
                    'step': subfield.get('slider_step', 1.0)
                }
                # parse values and labels
                values = []
                labels = []
                descriptions = []
                for element in self.get_select_from_list_elements(multi_entry_subfield=i):
                    if subfield.get('slider_mode') == 'textual':
                        values.append(str(element['value']))
                        labels.append(str(element['display']))
                        descriptions.append(str(element['description']))
                    elif subfield.get('slider_mode') in ['numeric-snap', 'numeric-free']:
                        if utils.is_number(element['value']):
                            values.append(float(element['value']))
                            labels.append(str(element['display']))
                            descriptions.append(str(element['description']))
                        else:
                            # just ignore?!
                            pass
                if subfield.get('slider_mode') in ['numeric-snap', 'numeric-free'] and len(values):
                    # sort
                    zipped = list(zip(values, labels, descriptions))
                    zipped.sort()
                    values, labels, descriptions = zip(*zipped)
                slider_subfields[i]['values'] = values
                slider_subfields[i]['labels'] = labels
                slider_subfields[i]['descriptions'] = descriptions
                # snap?
                slider_subfields[i]['grid_snap'] = True if subfield.get('slider_mode') in ['numeric-snap', 'textual'] else False
        return slider_subfields
    
    def get_magic_formatter_meta(self):
        ret = {}
        if self.magic_formatter:
            try:
                return next(e for e in MAGIC_FORMATTERS_LIST if e['name'] == self.magic_formatter)
            except:
                return ret
        return ret
    
    def has_multiple_report_mode_enabled(self):
        return True if self.config['custom_options']['multiple_reports_mode'] == 'enabled' else False
    
    def is_multiple_selection_allowed(self):
        if self.config['type'] == 'mark':
            if self.config['custom_options']['select_from_list_mode'] == 'multiple':
                return True
        elif self.config['type'] == 'multiEntry':
            if self.subfield is not None:
                if self.subfield < len(self.config['multi_entry']['options']):
                    if self.config['multi_entry']['options'][self.subfield].get('selectmode') == 'multiple':
                        return True
        return False

class ColumnReferences:
    
    _REF_PLACEHOLDER = '_$_SRESTEMP_$_'
    
    def __init__(self, override_username=None):
        self.db = _get_db()
        self.override_username = override_username or get_auth_user()
        pass
    
    def parse_subfield_shift(self, old_multientry_subfields, new_multientry_subfields):
        from Levenshtein import distance
        ret = {
            'shift_needed': False,
            'difference_found': False,
            'some_unmatched': False,
            'old_to_new_mapping': {}
        }
        old_to_new_mapping = {}
        for n, new_multientry_subfield in enumerate(new_multientry_subfields):
            match_found = False
            # see if there is a direct match
            try:
                if new_multientry_subfields[n]['label'] == old_multientry_subfields[n]['label']:
                    # direct match 
                    old_to_new_mapping[n] = {
                        'new_index': n,
                        'new_label': new_multientry_subfields[n]['label'],
                        'old_index': n,
                        'old_label': old_multientry_subfields[n]['label']
                    }
                    match_found = True
                    continue
            except:
                pass
            # go looking for a match
            ret['difference_found'] = True
            ret['shift_needed'] = True
            # look for a direct match first
            for o, old_multientry_subfield in enumerate(old_multientry_subfields):
                if new_multientry_subfields[n]['label'] == old_multientry_subfields[o]['label']:
                    old_to_new_mapping[n] = {
                        'new_index': n,
                        'new_label': new_multientry_subfields[n]['label'],
                        'old_index': o,
                        'old_label': old_multientry_subfields[o]['label']
                    }
                    match_found = False
                    continue
            if match_found: continue
            # if no direct match found, calculate and order by all levenshtein distances
            try:
                levenshtein_distances = {}
                for o, old_multientry_subfield in enumerate(old_multientry_subfields):
                    levenshtein_distances[o] = {
                        'levenshtein_distance': distance(
                            new_multientry_subfields[n]['label'],
                            old_multientry_subfields[o]['label']
                        )
                    }
                ordered_old_indexes = dict(natsorted(levenshtein_distances.items(), key=lambda kv: kv[1]['levenshtein_distance']))
                if levenshtein_distances[ordered_old_indexes[0]]['levenshtein_distance'] > 0.85:
                    # close enough; map it
                    old_to_new_mapping[n] = {
                        'new_index': n,
                        'new_label': new_multientry_subfields[n]['label'],
                        'old_index': ordered_old_indexes[0],
                        'old_label': old_multientry_subfields[ordered_old_indexes[0]]['label']
                    }
                    if n != ordered_old_indexes[0]:
                        ret['shift_needed'] = True
                else:
                    ret['some_unmatched'] = True
                    raise
            except:
                # log
                logging.debug("parse_subfield_shift error or could not find equivalent [{}]".format(new_multientry_subfields[n]['label']))
                old_to_new_mapping[n] = {
                    'new_index': n,
                    'new_label': new_multientry_subfields[n]['label'],
                    'old_index': -1
                }
        ret['old_to_new_mapping'] = old_to_new_mapping
        return ret
        
    def perform_subfield_shifts(self, column_uuid, old_to_new_mapping, elements_to_process=['portals','filters','columns','alerts'], override_username=None):
        
        from sres.portals import Portal
        from sres.filters import Filter
        
        ret = {
            'portals_updated': [],
            'filters_updated': []
        }
        
        # portals
        if 'portals' in elements_to_process:
            relevant_portals = self.db.portals.find({
                '_referenced_column_references': {'$regex': column_uuid, '$options': 'i'}
            }, ['uuid'])
            relevant_portals = list(relevant_portals)
            if len(relevant_portals):
                for relevant_portal in relevant_portals:
                    portal = Portal()
                    if portal.load(relevant_portal['uuid']):
                        # make a copy of panels
                        panels_copy = deepcopy(portal.config['panels'])
                        # loop through panels to see if replacements are needed
                        for p, panel in enumerate(portal.config['panels']):
                            for n, old_to_new_map in old_to_new_mapping.items():
                                # content
                                panels_copy[p]['content'] = self._remap_subfield_references(
                                    input=panel['content'], 
                                    column_uuid=column_uuid, 
                                    old_to_new_map=old_to_new_map, 
                                    find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])', 
                                    replace_prefix='$', replace_separator='.', replace_suffix=''
                                )
                                # conditions
                                panels_copy[p]['conditions'] = self._remap_subfield_references(
                                    input=panel['conditions'], 
                                    column_uuid=column_uuid, 
                                    old_to_new_map=old_to_new_map, 
                                    find_prefix='', find_separator='\.', find_suffix='', 
                                    replace_prefix='', replace_separator='.', replace_suffix='',
                                    use_json=True
                                )
                            # remove placeholders
                            panels_copy[p]['content'] = self._remove_placeholder(panels_copy[p]['content'])
                            panels_copy[p]['conditions'] = self._remove_placeholder(panels_copy[p]['conditions'], use_json=True)
                        # save to db
                        portal.config['panels'] = panels_copy
                        portal.update(override_username=self.override_username)
        
        # filters
        if 'filters' in elements_to_process:
            relevant_filters = self.db.filters.find({
                '_referenced_column_references': {'$regex': column_uuid, '$options': 'i'}
            }, ['uuid'])
            relevant_filters = list(relevant_filters)
            if len(relevant_filters):
                for relevant_filter in relevant_filters:
                    filter = Filter()
                    if filter.load(relevant_filter['uuid']):
                        # make a copy of config
                        config_copy = deepcopy(filter.config)
                        for n, old_to_new_map in old_to_new_mapping.items():
                            # conditions
                            config_copy['conditions'] = self._remap_subfield_references(
                                input=filter.config['conditions'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='', find_separator='\.', find_suffix='', 
                                replace_prefix='', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            # advanced conditions expression
                            config_copy['advanced_conditions']['expression'] = self._remap_subfield_references(
                                input=filter.config['advanced_conditions']['expression'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix=''
                            )
                            # email sender
                            config_copy['email']['sender'] = self._remap_subfield_references(
                                input=filter.config['email']['sender'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            # email addresses
                            config_copy['email']['addresses'] = self._remap_subfield_references(
                                input=filter.config['email']['addresses'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            # subject
                            config_copy['email']['subject'] = self._remap_subfield_references(
                                input=filter.config['email']['subject'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix=''
                            )
                            # body_first
                            config_copy['email']['body_first'] = self._remap_subfield_references(
                                input=filter.config['email']['body_first'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix=''
                            )
                            # body_last
                            config_copy['email']['body_last'] = self._remap_subfield_references(
                                input=filter.config['email']['body_last'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix=''
                            )
                        # remove placeholders
                        config_copy['conditions'] = self._remove_placeholder(config_copy['conditions'], use_json=True)
                        config_copy['advanced_conditions']['expression'] = self._remove_placeholder(config_copy['advanced_conditions']['expression'])
                        config_copy['email']['sender'] = self._remove_placeholder(config_copy['email']['sender'], use_json=True)
                        config_copy['email']['addresses'] = self._remove_placeholder(config_copy['email']['addresses'], use_json=True)
                        config_copy['email']['subject'] = self._remove_placeholder(config_copy['email']['subject'])
                        config_copy['email']['body_first'] = self._remove_placeholder(config_copy['email']['body_first'])
                        config_copy['email']['body_last'] = self._remove_placeholder(config_copy['email']['body_last'])
                        # loop through sections to see if replacements are needed
                        for s, section in enumerate(filter.config['sections']):
                            for n, old_to_new_map in old_to_new_mapping.items():
                                # content
                                config_copy['sections'][s]['content'] = self._remap_subfield_references(
                                    input=filter.config['sections'][s]['content'], 
                                    column_uuid=column_uuid, 
                                    old_to_new_map=old_to_new_map, 
                                    find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])', 
                                    replace_prefix='$', replace_separator='.', replace_suffix='$'
                                )
                                # conditions
                                config_copy['sections'][s]['conditions'] = self._remap_subfield_references(
                                    input=filter.config['sections'][s]['conditions'], 
                                    column_uuid=column_uuid, 
                                    old_to_new_map=old_to_new_map, 
                                    find_prefix='', find_separator='\.', find_suffix='', 
                                    replace_prefix='', replace_separator='.', replace_suffix='',
                                    use_json=True
                                )
                            # remove placeholders
                            config_copy['sections'][s]['content'] = self._remove_placeholder(config_copy['sections'][s]['content'])
                            config_copy['sections'][s]['conditions'] = self._remove_placeholder(config_copy['sections'][s]['conditions'], use_json=True)
                        # save to db
                        filter.config = config_copy
                        filter.update(override_username=self.override_username)
        
        # columns
        if 'columns' in elements_to_process:
            relevant_columns = self.db.columns.find({
                '_referenced_column_references': {'$regex': column_uuid, '$options': 'i'}
            }, ['uuid'])
            relevant_columns = list(relevant_columns)
            if len(relevant_columns):
                for relevant_column in relevant_columns:
                    column = Column()
                    if column.load(relevant_column['uuid']):
                        # make a copy of config
                        config_copy = deepcopy(column.config)
                        for n, old_to_new_map in old_to_new_mapping.items():
                            # notify_email
                            config_copy['notify_email'] = self._remap_subfield_references(
                                input=filter.config['notify_email'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            # coversheet
                            config_copy['coversheet'] = self._remap_subfield_references(
                                input=filter.config['coversheet'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            # apply_to_others
                            config_copy['apply_to_others']['other_columnuuid'] = self._remap_subfield_references(
                                input=filter.config['apply_to_others']['other_columnuuid'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='', find_separator='\.', find_suffix='',
                                replace_prefix='', replace_separator='.', replace_suffix=''
                            )
                            # quick_info
                            config_copy['quick_info'] = self._remap_subfield_references(
                                input=filter.config['quick_info'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            # custom_options.quickinfo_rollview
                            config_copy['custom_options']['quickinfo_rollview'] = self._remap_subfield_references(
                                input=filter.config['custom_options']['quickinfo_rollview'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix=''
                            )
                            # custom_options.grouping_column
                            config_copy['custom_options']['grouping_column'] = self._remap_subfield_references(
                                input=filter.config['custom_options']['grouping_column'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='', find_separator='\.', find_suffix='',
                                replace_prefix='', replace_separator='.', replace_suffix=''
                            )
                            # aggregation_options
                            config_copy['aggregation_options']['attributes'] = self._remap_subfield_references(
                                input=filter.config['aggregation_options']['attributes'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='', find_separator='\.', find_suffix='',
                                replace_prefix='', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                            config_copy['aggregation_options']['aggregator_type_mathematical_operations_formula'] = self._remap_subfield_references(
                                input=filter.config['aggregation_options']['aggregator_type_mathematical_operations_formula'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='\$', find_separator='\.', find_suffix='(?=[\.|\$])',
                                replace_prefix='$', replace_separator='.', replace_suffix=''
                            )
                            for c, case in enumerate(filter.config['aggregation_options']['aggregator_type_case_builder_cases']):
                                config_copy['aggregation_options']['aggregator_type_case_builder_cases'][c]['rules'] = self._remap_subfield_references(
                                    input=case['rules'], 
                                    column_uuid=column_uuid, 
                                    old_to_new_map=old_to_new_map, 
                                    find_prefix='', find_separator='\.', find_suffix='',
                                    replace_prefix='', replace_separator='.', replace_suffix='',
                                    use_json=True
                                )
                            # conditions
                            config_copy['conditions'] = self._remap_subfield_references(
                                input=filter.config['conditions'], 
                                column_uuid=column_uuid, 
                                old_to_new_map=old_to_new_map, 
                                find_prefix='', find_separator='\.', find_suffix='', 
                                replace_prefix='', replace_separator='.', replace_suffix='',
                                use_json=True
                            )
                        # remove placeholders
                        config_copy['notify_email'] = self._remove_placeholder(config_copy['notify_email'])
                        config_copy['coversheet'] = self._remove_placeholder(config_copy['coversheet'])
                        config_copy['apply_to_others']['other_columnuuid'] = self._remove_placeholder(config_copy['apply_to_others']['other_columnuuid'])
                        config_copy['quick_info'] = self._remove_placeholder(config_copy['quick_info'])
                        config_copy['custom_options']['quickinfo_rollview'] = self._remove_placeholder(config_copy['custom_options']['quickinfo_rollview'])
                        config_copy['custom_options']['grouping_column'] = self._remove_placeholder(config_copy['custom_options']['grouping_column'])
                        config_copy['aggregation_options']['attributes'] = self._remove_placeholder(config_copy['aggregation_options']['attributes'])
                        config_copy['aggregation_options']['aggregator_type_mathematical_operations_formula'] = self._remove_placeholder(config_copy['aggregation_options']['aggregator_type_mathematical_operations_formula'])
                        config_copy['aggregation_options']['aggregator_type_case_builder_cases'] = self._remove_placeholder(config_copy['aggregation_options']['aggregator_type_case_builder_cases'])
                        config_copy['conditions'] = self._remove_placeholder(config_copy['conditions'])
                        # save to db
                        column.config = config_copy
                        column.update(override_username=self.override_username)
            
        # insight alerts
        if 'alerts' in elements_to_process:
            pass # TODO
        
        
        
        
    def _remap_subfield_references(self, input, column_uuid, old_to_new_map, mapping_key, find_prefix, find_separator, find_suffix, replace_prefix, replace_separator, replace_suffix, use_json=False):
        ret = None
        if use_json:
            input = json.dumps(input)
        if old_to_new_map['old_index'] == -1:
            # skip
            ret = input
        elif old_to_new_map['old_index'] == old_to_new_map['new_index']:
            # also skip, no change needed
            ret = input
        else:
            # update subfield references
            ret = re.sub(
                find_prefix + column_uuid + find_separator + old_to_new_map['old_index'] & find_suffix,
                replace_prefix + column_uuid + replace_separator + _REF_PLACEHOLDER + old_to_new_map['new_index'] & replace_suffix,
                input
            )
        if use_json:
            return json.loads(ret)
        else:
            return ret
    
    def _remove_placeholder(self, input, use_json=False):
        if use_json:
            input = json.dumps(input)
        ret = input.replace(_REF_PLACEHOLDER, '')
        if use_json:
            return json.loads(ret)
        else:
            return ret
    
    
