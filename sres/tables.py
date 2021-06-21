from flask import g, session, current_app, escape, flash
from flask_babel import gettext
from bson.objectid import ObjectId
from copy import deepcopy, copy
from natsort import natsorted, ns
import json
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser
import mimetypes
import os
import re
from hashlib import sha512
import logging

from sres.db import _get_db, DbCookie
from sres.auth import is_user_administrator, get_auth_user, get_auth_user_oid
from sres import utils
from sres.users import User, oids_to_usernames, usernames_to_oids
from sres.columns import SYSTEM_COLUMNS, Column, column_uuid_to_oid, get_friendly_column_name, MAGIC_FORMATTERS_LIST, _is_student_direct_access_allowed
from sres.files import get_file_access_url, GridFile
from sres.studentdata import StudentData, NAME_FIELDS, IDENTIFIER_FIELDS, NON_DATA_FIELDS, run_aggregation_bulk, substitute_text_variables, _preload_columns
from sres.anonymiser import anonymise, is_identity_anonymiser_active

USER_ROLES = [
	{
		'name': 'administrator',
		'display': 'List administrator',
		'hint': 'View/edit all data and list, and change list and column settings'
	},
	{
		'name': 'user',
		'display': 'List user',
		'hint': 'View/edit data only when allowed'
	},
	{
		'name': 'auditor',
		'display': 'List auditor',
		'hint': 'View-only access to list and all data'
	}
]

AUTOLIST_MAPPINGS = [
	{
		"name": "sid",
		"dbname": "SID",
		"display": gettext('Unique ID'),
		"required": True,
		"hint": gettext('e.g. student ID number'),
		"like": ['sid','studentid','studentnumber','id','student_code']
	},
	{
		"name": "email",
		"dbname": "Email",
		"display": "Email address",
		"required": True,
		"like": ['email','mail','emailaddress','mbox','mailbox']
	},
	{
		"name": "given_names",
		"dbname": "Given_Names",
		"display": "Given names",
		"required": True,
		"like": ['givennames','givenname','firstname','firstnames','forename','christianname']
	},
	{
		"name": "preferred_name",
		"dbname": "Preferred_Name",
		"display": "Preferred name",
		"required": True,
		"like": ['preferredname'],
        "show_secondary_field": True
	},
	{
		"name": "surname",
		"dbname": "Family_Name",
		"display": "Surname",
		"required": True,
		"like": ['surname','lastname','familyname','sn']
	},
	{
		"name": "username",
		"dbname": "username",
		"display": gettext('Username'),
		"required": False,
		"hint": gettext('e.g. unikey'),
		"like": ['username','uname','user']
	},
	{
		"name": "alternative_id1",
		"dbname": "alternative_id1",
		"display": "Alternative ID1",
		"required": False,
		"like": ['alternative_id1','alternativeid1','alternativeID1', 'AlternativeID1','altid1']
	},
	{
		"name": "alternative_id2",
		"dbname": "alternative_id2",
		"display": "Alternative ID2",
		"required": False,
		"like": ['alternative_id2','alternativeid2','alternativeID2', 'AlternativeID2','altid2']
	}
]

def list_authorised_tables(show_archived=False, filter_years=[], filter_semesters=[], code=None, name=None, only_where_user_is=['administrator', 'auditor'], ignore_authorisation_state=False, show_deleted=False, override_user_oid=None):
    """
        Gets all the tables that the current user is authorised to view.
        
        show_archived (bool)
        filter_years (list of ints)
        filter_semesters (list of str)
        code (str)
        name (str)
        only_where_user_is (list of str)
        ignore_authorisation_state (bool) If True, overrides the premise of this function
            i.e. performs list_tables instead of list_authorised_tables
        show_deleted (bool)
        override_user_oid (ObjectId or None)
        
        Returns a list of dicts, straight from db.tables
    """
    db = _get_db()
    db_filter = {}
    # archived or not
    if show_archived and show_deleted:
        db_filter['workflow_state'] = {'$in': ['archived', '', 'active', 'deleted']}
    elif show_archived:
        db_filter['workflow_state'] = {'$in': ['archived', '', 'active']}
    else:
        db_filter['workflow_state'] = {'$in': ['', 'active']}
    # find by year(s)
    if len(filter_years) > 0:
        db_filter['year'] = {'$in': []}
        for filter_year in filter_years:
            db_filter['year']['$in'].append(str(filter_year))
            db_filter['year']['$in'].append(int(filter_year))
            #if isinstance(filter_year, str) and utils.is_number(filter_year):
            #    db_filter['year']['$in'].append(int(filter_year))
    # find by semester(s)
    if len(filter_semesters) > 0:
        db_filter['semester'] = {'$in': []}
        for filter_semester in filter_semesters:
            db_filter['semester']['$in'].append(filter_semester)
            if isinstance(filter_semester, str) and utils.is_number(filter_semester):
                db_filter['semester']['$in'].append(int(filter_semester))
    # find by code
    if code is not None:
        db_filter['code'] = {'$regex': code, '$options': 'i'}
    # find by code
    if name is not None:
        db_filter['name'] = {'$regex': name, '$options': 'i'}
    # find by role
    if ignore_authorisation_state:
        pass
    else:
        if only_where_user_is:
            user_oid = override_user_oid or get_auth_user_oid()
            role_filter = []
            for role in only_where_user_is:
                if role in [r['name'] for r in USER_ROLES]:
                    role_filter.append({
                        'staff.{}s'.format(role): user_oid
                    })
            db_filter['$or'] = role_filter
    # find!
    #logging.debug(f'db_filter: {db_filter}')
    return list(db.tables.find(db_filter).sort([('year', -1), ('semester', -1), ('code', 1)]))
    
def list_authorised_periods():
    """Gets years and semesters (i.e. time periods) of all tables that the current user is authorised to view"""
    # TODO
    pass

def load_authorised_tables(show_archived=False, filter_years=[], filter_semesters=[], code=None, name=None, only_where_user_is=[]):
    """
        Returns a list of class instances of Table.
    """
    tables = list_authorised_tables(show_archived=show_archived, filter_years=filter_years, filter_semesters=filter_semesters, code=code, name=name,only_where_user_is=only_where_user_is)
    ret = []
    for table in tables:
        this_table = Table()
        if this_table.load(table_oid=table['_id']):
            ret.append(this_table)
    return ret

def table_oid_to_uuid(oid):
    db = _get_db()
    tables = db.tables.find({"_id": oid})
    tables = list(tables)
    if len(tables) == 1:
        return tables[0]["uuid"]
    else:
        return None
        
def table_uuid_to_oid(uuid):
    db = _get_db()
    if uuid:
        tables = db.tables.find({"uuid": uuid})
        tables = list(tables)
        if len(tables) == 1:
            return tables[0]["_id"]
    return None

def format_full_name(meta):
    ret = {}
    ret['complete'] = '{code} {name} ({year} semester {semester})'.format(
        name=meta['name'],
        code=meta['code'],
        year=meta['year'],
        semester=meta['semester']
    )
    ret['code_and_name'] = f"{meta['code']} {meta['name']}"
    ret['year_and_semester'] = f"{meta['year']} semester {meta['semester']}"
    return ret

def _swap_column_references(input, reference_mapping, swap_unmapped_references=True, unmapped_replacement='?FIELD?', delimiter='$', use_json=False):
    if delimiter:
        column_pattern = '(?<=\\' + delimiter + ')' + utils.BASE_COLUMN_REFERENCE_PATTERN + '(?=\\' + delimiter + ')'
    else:
        column_pattern = utils.BASE_COLUMN_REFERENCE_PATTERN
    if use_json:
        input = json.dumps(input)
    column_references = re.findall(column_pattern, input)
    for column_reference in column_references:
        column_uuid = column_reference.split('.')[0]
        if column_uuid in reference_mapping.keys():
            input = input.replace(column_uuid, reference_mapping[column_uuid])
            continue
        if swap_unmapped_references:
            input = input.replace(column_reference, unmapped_replacement)
            continue
    if use_json:
        input = json.loads(input)
    return input

def _get_column_meta_for_select_array(column, table, ret=None, show_collapsed_multientry_option=False, hide_multientry_subfields=False, get_text_only=False):
    if ret is None:
        ret = []
    record = {}
    if (column['config']['type'] == 'multiEntry' or column['config'].get('multientry_data_format') == True):
        if show_collapsed_multientry_option:
            record['value'] = column['config']['uuid']
            record['display'] = get_friendly_column_name(show_table_info=False, column=column, table=table, get_text_only=get_text_only)
            record['display_text'] = record['display']
            record['full_display'] = get_friendly_column_name(show_table_info=True, column=column, table=table, get_text_only=get_text_only)
            record['full_display_text'] = record['full_display']
            record['datatype'] = column['config']['type']
            record['type'] = ''
            ret.append(record)
        if not hide_multientry_subfields:
            i = 0
            for multientry_field in column['config']['multi_entry']['options']:
                i += 1
                record = {}
                record['value'] = '{}.{}'.format(column['config']['uuid'], i - 1)
                #subfield = Column()
                column['subfield'] = i - 1
                #if subfield.load(record['value']):
                if True:
                    record['subfield'] = i - 1
                    record['base_column_uuid'] = column['config']['uuid']
                    record['display'] = get_friendly_column_name(show_table_info=False, column=column, table=table, get_text_only=get_text_only)
                    record['display_text'] = BeautifulSoup(record['display'], 'html.parser').get_text()
                    if 'maximumValue' in column['config']['multi_entry']['options'][i-1]:
                        record['maximumValue'] = column['config']['multi_entry']['options'][i-1]['maximumValue']
                    record['full_display'] = get_friendly_column_name(show_table_info=True, column=column, table=table, get_text_only=get_text_only)
                    record['full_display_text'] = BeautifulSoup(record['full_display'], 'html.parser').get_text()
                    record['datatype'] = column['config']['type']
                    record['type'] = multientry_field['type']
                    record['selectable_options_length'] = len(multientry_field.get('select', []))
                    ret.append(record)
                else:
                    print('xxxxx', record['value'])
    else:
        record['value'] = column['config']['uuid']
        record['display'] = get_friendly_column_name(show_table_info=False, column=column, table=table, get_text_only=get_text_only)
        record['display_text'] = record['display']
        record['full_display'] = get_friendly_column_name(show_table_info=True, column=column, table=table, get_text_only=get_text_only)
        record['full_display_text'] = record['full_display']
        record['datatype'] = column['config']['type']
        record['type'] = ''
        record['selectable_options_length'] = len(column['config']['simple_input'].get('options', []))
        if 'maximumValue' in column['config']['custom_options']:
            record['maximumValue'] = column['config']['custom_options']['maximumValue']
        ret.append(record)
    return ret

def _unpack_multientry_json(unpacked_data, n, _id):
    try:
        return utils.replace_mojibake(unpacked_data[_id][n])
    except:
        return ''

def rereference_columns_suggestions(target_table_uuid, existing_columns):
    """
        target_table_uuid (str uuid)
        existing_columns (list of dicts of column references)
    """
    ret = {
        'success': False,
        'suggested_columns': {}
    }
    target_table = Table()
    if target_table.load(target_table_uuid):
        existing_column_references = {}
        for existing_column in existing_columns:
            column = Column()
            if column.load(existing_column['column_reference']):
                existing_column_references[existing_column['column_reference']] = {
                    'friendly_name': column.get_friendly_name(show_table_info=False),
                    'friendly_name_full': column.get_friendly_name(),
                    'column_reference': existing_column['column_reference'],
                    'magic_formatter': column.magic_formatter if column.magic_formatter is not None else '',
                    'reference_type': existing_column['type'],
                    'source_element_id': existing_column['source_element_id'],
                    'source_element_selector': existing_column['source_element_selector']
                }
        # find the similar columns
        similar_columns = target_table.find_similar_columns_by_name(existing_column_references)
        # parse and load for return payload
        for existing_column in existing_columns:
            suggestion = {
                'suggestion_found': False,
                'current_column_reference': existing_column_references[existing_column['column_reference']]['column_reference'],
                'reference_type': existing_column_references[existing_column['column_reference']]['reference_type'],
                'magic_formatter': existing_column_references[existing_column['column_reference']]['magic_formatter'],
                'source_element_id': existing_column_references[existing_column['column_reference']]['source_element_id'],
                'source_element_selector': existing_column_references[existing_column['column_reference']]['source_element_selector'],
                'original_column_display': existing_column_references[existing_column['column_reference']]['friendly_name'],
                'original_column_display_full': existing_column_references[existing_column['column_reference']]['friendly_name_full'],
                'suggested_target_column_references': []
            }
            if existing_column['column_reference'] in similar_columns.keys():
                suggestion['suggestion_found'] = True
                suggestion['suggested_target_column_references'] = similar_columns[existing_column['column_reference']]['suggested_target_column_references']
            ret['suggested_columns'][existing_column['column_reference']] = suggestion
        ret['success'] = True
    return ret

def _file_import_read_file_to_df(table_uuid, filename, skiprows=None, nrows=None, make_pickle=False, use_pickle=False):
    """
        Generic input spreadsheet reader.
        
        table_uuid (string) Associated uuid of table that data will be placed into.
            Used to form part of the pickled data filename.
        filename (string) The source filename; this is typically already a UUID.
        skiprows (int) Row of the input file to start on, 0-indexed.
        nrows (int) Rows to read.
        make_pickle (bool) Whether to save the parsed spreadsheet as a pickle.
        use_pickle (bool) Whether to use a saved pickle instead of re-parsing the spreadsheet.
        
        Returns 'df' which is actually a list of dicts. Naming is a hangover from when pandas was 
        used for this method.
    """
    import pickle
    pickle_fn = utils.encrypt_to_hex(filename + table_uuid)
    
    if use_pickle:
        
        logging.debug("Loading pickle for [{}]".format(filename))
        gf = GridFile('temp')
        if gf.find_and_load(pickle_fn):
            df = pickle.loads(gf.get_file().read())
            
    else:
    
        from chardet.universaldetector import UniversalDetector
        # get gridfs
        gf = GridFile('temp')
        gf.find_and_load(filename)
        # guess mimetype from filename
        mime_type = utils.guess_mime_type(filename)
        ext = filename.split('.')[-1].lower()
        # read
        if ext != 'csv' and (mime_type == 'application/vnd.ms-excel' or mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or ext == 'xlsx' or ext == 'xls'):
            logging.debug('read excel')
            import xlrd
            wb = xlrd.open_workbook(file_contents=gf.get_file().read())
            sheet = wb.sheet_by_index(0)
            file_data = []
            headers = [cell.value for cell in sheet.row(0)]
            for row in range(1, sheet.nrows):
                row_data = []
                for cell in sheet.row(row):
                    if cell.ctype == xlrd.XL_CELL_TEXT:
                        row_data.append(cell.value)
                    elif cell.ctype == xlrd.XL_CELL_DATE:
                        dt = datetime(*xlrd.xldate_as_tuple(cell.value, wb.datemode))
                        row_data.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                    else:
                        c = str(cell.value)
                        if c.endswith('.0'):
                            c = c[:-2]
                        row_data.append(c)
                value_dict = dict(zip(headers, row_data))
                if len(''.join(row_data)) == 0:
                    continue
                file_data.append(value_dict)
            df = file_data
        else:
            logging.debug('read dsv')
            import csv
            # try detect encoding, and read file lines into temp var
            detector = UniversalDetector()
            file_lines = []
            _first_line = ''
            try:
                s = gf.open_stream()
                line = s.readline()
                _first_line = deepcopy(line)
                while line:
                    if not detector.done:
                        detector.feed(line)
                    file_lines.append(line)
                    line = s.readline()
            except Exception as e:
                logging.exception(e)
                raise
            detector.close()
            encoding = detector.result['encoding']
            logging.debug('encoding ' + str(encoding))
            # read
            file_data = []
            try:
                logging.debug('opening dsv')
                # decode 
                decoded_file_lines = []
                for line in file_lines:
                    decoded_file_lines.append(line.decode(encoding))
                # sniff dialect
                logging.debug('sniffing dialect')
                try:
                    dialect = csv.Sniffer().sniff('\r'.join(decoded_file_lines[:5]))
                except csv.Error:
                    # try just with the first line
                    dialect = csv.Sniffer().sniff(_first_line.decode(encoding))
                #logging.debug('dialect ' + str(dialect.doublequote) + ' ' + str(dialect.escapechar) + ' ' + str(dialect.quoting))
                # read
                reader = csv.DictReader(decoded_file_lines, dialect=dialect)
                logging.debug('iterating file')
                for row in reader:
                    file_data.append(row)
            except Exception as e:
                logging.exception(e)
                raise
            logging.debug('making df')
            df = file_data
    
    if make_pickle:
        logging.debug("Making pickle for [{}]".format(filename))
        gf = GridFile('temp')
        if not gf.save_file(file=pickle.dumps(df), filename=pickle_fn, content_type='text/plain'):
            logging.error("Failed making pickle in _file_import_read_file_to_df [{}]".format(filename))
    
    logging.debug('returning')
    if skiprows is not None and nrows is not None:
        return df[skiprows:skiprows+nrows]
    else:
        return df

class Table:
    
    default_config = {
        'uuid': '',
        'code': '',
        'name': '',
        'year': None,
        'semester': '',
        'contact': {
            'name': '',
            'email': ''
        },
        'staff': {
            'administrators': [], # oids
            'users': [], # oids
            'auditors': [] # oids
        },
        'views': [],
        'workflow_state': 'active',
        'printout_templates': {}
    }
    
    def __init__(self):
        self.db = _get_db()
        # define instance variables
        self.config = deepcopy(self.default_config)
        self._id = None
    
    def load(self, table_uuid=None, table_oid=None, preloaded_db_result=None):
        if preloaded_db_result is None:
            db = _get_db()
            filter = {}
            if table_uuid is not None:
                filter['uuid'] = table_uuid
            if table_oid is not None:
                filter['_id'] = table_oid
            result = db.tables.find(filter)
            result = list(result)
        else:
            result = preloaded_db_result
        if len(result) == 1:
            self._id = result[0]['_id']
            for key, value in self.default_config.items():
                try:
                    self.config[key] = result[0][key]
                except:
                    self.config[key] = value
            return True
        else:
            return False
    
    def update(self, override_username=None):
        if self.is_user_authorised(username=override_username):
            result = self.db.tables.update_one({'uuid': self.config['uuid']}, {'$set': self.config})
            return result.acknowledged
        return False
    
    def create(self, override_uuid=None, override_username=None):
        if is_user_administrator('list', username=override_username) or is_user_administrator('super', username=override_username):
            # ok
            pass
        else:
            return False
        if override_uuid is not None:
            new_uuid = utils.clean_uuid(override_uuid)
        else:
            new_uuid = utils.create_uuid()
        self.config['uuid'] = new_uuid
        result = self.db.tables.insert_one(self.config)
        if result.acknowledged:
            if self.load(new_uuid):
                return new_uuid
        return None
    
    def clone(self, mode='new', new_list_details=None, timeshift_add_days=0, existing_table_uuid=None, column_list=None):
        ret = {
            'success': False,
            'messages': [],
            'destination_table_uuid': '',
            'column_map': {}
        }
        target_table = Table()
        if new_list_details is None:
            new_list_details = {}
        if column_list is None:
            column_list = []
        # create or load table
        if mode == 'new' and new_list_details:
            new_table_uuid = target_table.create()
            if new_table_uuid:
                target_table.config = deepcopy(self.config)
                target_table.config['uuid'] = new_table_uuid
                target_table.config['code'] = new_list_details['code']
                target_table.config['name'] = new_list_details['name']
                target_table.config['year'] = new_list_details['year']
                target_table.config['semester'] = new_list_details['semester']
                target_table.update()
            else:
                ret['messages'].append(("Could not create new list.", "danger"))
        elif mode == 'existing' and existing_table_uuid:
            if target_table.load(existing_table_uuid):
                pass
            else:
                ret['messages'].append(("Could not load source list.", "danger"))
                return ret
        # clone columns
        column_map = {}
        cloned_columns = [] # Column() instances
        for column_uuid in column_list:
            source_column = Column(preloaded_table=self)
            if source_column.load(column_uuid):
                cloned_column_uuid = source_column.clone(
                    target_table_uuid=target_table.config['uuid'],
                    add_cloned_notice=False
                )
                if cloned_column_uuid:
                    cloned_column = Column(preloaded_table=target_table)
                    if cloned_column.load(cloned_column_uuid):
                        # perform timeshifts
                        if timeshift_add_days != 0:
                            if cloned_column.config['active']['from'] and cloned_column.config['active']['to']:
                                cloned_column.config['active']['from'] = cloned_column.config['active']['from'] + timedelta(days=timeshift_add_days)
                                cloned_column.config['active']['to'] = cloned_column.config['active']['to'] + timedelta(days=timeshift_add_days)
                                if isinstance(cloned_column.config['sign_in_out']['week_start'], datetime):
                                    cloned_column.config['sign_in_out']['week_start'] = cloned_column.config['sign_in_out']['week_start'] + timedelta(days=timeshift_add_days)
                                if cloned_column.update():
                                    column_map[column_uuid] = cloned_column_uuid
                                    cloned_columns.append(cloned_column)
                                else:
                                    ret['messages'].append(("Error updating cloned column {}".format(cloned_column_uuid), "warning"))
                            else:
                                # cloned_column active dates are improperly set, so let's set them to today
                                cloned_column.config['active']['from'] = datetime.now()
                                cloned_column.config['active']['to'] = datetime.now()
                    else:
                        ret['messages'].append(("Error loading clone of column {}".format(column_uuid), "warning"))
                else:
                    ret['messages'].append(("Error cloning column {}".format(column_uuid), "warning"))
            else:
                ret['messages'].append(("Could not load column {}".format(column_uuid), "warning"))
        ret['column_map'] = column_map
        # update column mapping
        for cloned_column in cloned_columns:
            # notify_email
            cloned_column.config['notify_email']['body'] = _swap_column_references(
                input=cloned_column.config['notify_email']['body'],
                reference_mapping=column_map)
            cloned_column.config['notify_email']['subject'] = _swap_column_references(
                input=cloned_column.config['notify_email']['subject'],
                reference_mapping=column_map)
            # coversheet.html
            cloned_column.config['coversheet']['html'] = _swap_column_references(
                input=cloned_column.config['coversheet']['html'],
                reference_mapping=column_map)
            # quickinfo
            cloned_column.config['quick_info']['single'] = _swap_column_references(
                input=cloned_column.config['quick_info']['single'],
                reference_mapping=column_map)
            cloned_column.config['quick_info']['bulk'] = _swap_column_references(
                input=cloned_column.config['quick_info']['bulk'],
                reference_mapping=column_map)
            cloned_column.config['custom_options']['quickinfo_rollview'] = _swap_column_references(
                input=cloned_column.config['custom_options']['quickinfo_rollview'],
                reference_mapping=column_map)
            # apply_to_others.other_columnuuid
            cloned_column.config['apply_to_others']['other_columnuuid'] = _swap_column_references(
                input=cloned_column.config['apply_to_others']['other_columnuuid'],
                reference_mapping=column_map, delimiter='')
            # custom options
            cloned_column.config['custom_options']['restrict_by_username_column'] = _swap_column_references(
                input=cloned_column.config['custom_options']['restrict_by_username_column'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='')
            cloned_column.config['custom_options']['grouping_column'] = _swap_column_references(
                input=cloned_column.config['custom_options']['grouping_column'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='')
            # aggregation
            cloned_column.config['aggregated_by'] = _swap_column_references(
                input=cloned_column.config['aggregated_by'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='', use_json=True)
            cloned_column.config['aggregation_options']['attributes'] = _swap_column_references(
                input=cloned_column.config['aggregation_options']['attributes'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='', use_json=True)
            cloned_column.config['aggregation_options']['aggregator_type_mathematical_operations_formula'] = _swap_column_references(
                input=cloned_column.config['aggregation_options']['aggregator_type_mathematical_operations_formula'],
                reference_mapping=column_map, unmapped_replacement='')
            cloned_column.config['aggregation_options']['aggregator_type_case_builder_cases'] = _swap_column_references(
                input=cloned_column.config['aggregation_options']['aggregator_type_case_builder_cases'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='', use_json=True)
            cloned_column.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column'] = _swap_column_references(
                input=cloned_column.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='')
            cloned_column.config['aggregation_options']['aggregator_type_self_peer_review_score_column'] = _swap_column_references(
                input=cloned_column.config['aggregation_options']['aggregator_type_self_peer_review_score_column'],
                reference_mapping=column_map, delimiter='', unmapped_replacement='')
            # update
            if not cloned_column.update():
                ret['messages'].append(("Error updating cloned column {}".format(cloned_column.config['uuid']), "warning"))
        # update columns referred to in custom views
        for i, custom_view in enumerate(target_table.config.get('views', [])):
            custom_view_config_columns = [] # new array
            for view_column in custom_view['config']['columns']:
                _view_column = deepcopy(view_column)
                if _view_column['type'] == 'user' and _view_column['name'] != '_actions':
                    if column_map.get(_view_column['name']):
                        # do the replacement
                        _view_column['name'] = column_map.get(_view_column['name'])
                        custom_view_config_columns.append(_view_column)
                    else:
                        # replacement does not exist...
                        pass
                else:
                    custom_view_config_columns.append(_view_column)
            target_table.config['views'][i]['config']['columns'] = deepcopy(custom_view_config_columns)
            target_table.update()
        # return
        ret['success'] = True
        ret['destination_table_uuid'] = target_table.config['uuid']
        return ret
    
    def archive(self, override_username=None):
        if self.is_user_authorised(username=override_username):
            self.config['workflow_state'] = 'archived'
            return self.update(override_username=override_username)
        return False
    
    def unarchive(self):
        if self.is_user_authorised():
            self.config['workflow_state'] = 'active'
            return self.update()
        return False
    
    def delete(self, override_username=None):
        if self.is_user_authorised(username=override_username):
            self.config['workflow_state'] = 'deleted'
            return self.update(override_username=override_username)
        return False
    
    def get_full_name(self):
        return format_full_name(self.config)['complete']
    
    def count_inactive_students(self):
        return self.db.data.find(
            {
                'table_uuid': self.config['uuid'],
                'status': 'inactive'
            }
        ).count()
        
    def get_all_students_oids(self, only_active=True, as_strings=False):
        students = self.get_all_student_identifiers(identifier_types=['_id'], only_active=only_active)
        if as_strings:
            return [str(s['_id']) for s in students]
        else:
            return [s['_id'] for s in students]
    
    def get_all_students_sids(self, only_active=True):
        """
            Returns a list of 'sid' identifiers (strings).
        """
        students = self.get_all_student_identifiers(identifier_types=['sid'], only_active=only_active)
        return [s['sid'] for s in students]
    
    def get_all_student_identifiers(self, identifier_types=['sid'], only_active=True):
        """
            Gets identifier(s) specified for all students in the current table.
            Returns a list of dicts. Each key of each dict corresponds to the identifier type.
        """
        filter = {
            'table_uuid': self.config['uuid']
        }
        if only_active:
            filter['status'] = 'active'
        cleaned_identifier_types = [i for i in identifier_types if i in IDENTIFIER_FIELDS + ['_id']]
        results = self.db.data.find(filter, cleaned_identifier_types)
        results = list(results)
        ret = []
        for result in results:
            record = {}
            for cleaned_identifier_type in cleaned_identifier_types:
                record[cleaned_identifier_type] = result[cleaned_identifier_type]
            ret.append(record)
        return ret
    
    def load_all_students(self, only_active=True, restrict_by_username_column='', groups_to_check=[], grouping_column='', target_column=None, show_quickinfo=False, show_everyone=True, sdak=None, get_email=False, override_user_identifier=None):
        """Specialty method for bulk loading a representation of StudentData for roll view 
            and student search methods. API uses this too.
            
            only_active (boolean)
            restrict_by_username_column (str) column uuid
            groups_to_check
            grouping_column
            target_column (Column instance or None)
            show_quickinfo (boolean)
            show_everyone (boolean)
            sdak (string|None) For student direct access.
            get_email (boolean) defaults false
            override_user_identifier (str or None)
            
            Returns list of dicts.
        """
        student_data = StudentData(self)
        students = []
        if override_user_identifier is None:
            current_user = get_auth_user()
        else:
            current_user = override_user_identifier
        # build filter to find students in list
        filters = []
        if only_active:
            filters.append({'status': 'active'})
        restrictor_column = Column()
        if restrict_by_username_column and not sdak and restrictor_column.load(restrict_by_username_column):
            filters.append({
                restrictor_column.config['uuid']: {'$regex': f'{current_user}', '$options': 'i'}
            })
            # check self table is the same as the table for restrictor column
            if self.config['uuid'] != restrictor_column.table.config['uuid']:
                self.load(restrictor_column.table.config['uuid'])
        filters.append({
            'table_uuid': self.config['uuid']
        })
        # if using student direct access, additional bits to filters as necessary
        if sdak is not None and sdak != '':
            if student_data.find_student(current_user):
                if target_column is not None:
                    if target_column.is_peer_data_entry_restricted():
                        filters.extend(target_column.get_db_filter_restrictors_for_peer_data_entry(preloaded_student_data=student_data))
                    if not target_column.is_self_data_entry_enabled():
                        # remove self
                        filters.append({
                            'sid': {'$ne': student_data.config['sid']}
                        })
            else:
                # problem...
                logging.warning(f"Student [{current_user}] tried to access tables.load_all_students for [{self.config['uuid']}] but failed to load")
                return students
        # process only_show when...
        if target_column is not None:
            filters.extend(target_column.get_db_filter_restrictors_for_only_show())
        # get students from db using filters
        results = self.db.data.find({'$and': filters}).sort([('surname', 1)])
        results = list(results)
        # load up students into list
        for result in results:
            student = {}
            student_data._reset()
            if student_data.load(db_result=result):
                #students.append(copy(student_data))
                student['sid'] = student_data.config['sid']
                student['display_sid'] = student_data.config['sid']
                student['preferred_name'] = student_data.config['preferred_name']
                student['surname'] = student_data.config['surname']
                student['given_names'] = student_data.config.get('given_names')
                student['status'] = student_data.config.get('status')
                student['username'] = student_data.config.get('username')
                student['alternative_id1'] = student_data.config.get('alternative_id1')
                student['alternative_id2'] = student_data.config.get('alternative_id2')
                # email?
                if get_email is True:
                    student['email'] = student_data.config['email']
                # is_in_group
                student['is_in_group'] = student_data.is_in_groups(groups_to_check=groups_to_check, grouping_column=grouping_column)
                # is_username_allowed_access
                if restrict_by_username_column != '' and not sdak and target_column is not None:
                    student['is_username_allowed_access'] = student_data.is_username_allowed_access(
                        restrict_by_username_column=restrict_by_username_column,
                        target_columnuuid=target_column.config['uuid']
                    )
                else:
                    student['is_username_allowed_access'] = True
                # data
                #student['_data'] = student_data.get_data(target_column.config['uuid'], preloaded_column=target_column).get('data', '')
                if target_column is not None:
                    student['_data'] = student_data.get_data_for_entry(target_column)['data']
                else:
                    student['_data'] = ''
                student['_data_json'] = json.dumps(student['_data'])
                # group
                if grouping_column != '':
                    student['groups'] = student_data.data.get(grouping_column, '')
                else:
                    student['groups'] = ''
                # show_quickinfo
                if target_column is not None:
                    if show_quickinfo and (show_everyone or student['is_in_group']):
                        student['quickinfo'] = student_data.get_quick_info(
                            column_uuid=target_column.config['uuid'],
                            type='roll',
                            preloaded_column=target_column,
                            preloaded_student_data=student_data,
                            cache_columns=True
                        )['quick_info_html']
                        #student['quickinfo'] = '' # essentially pass
                students.append(student)
        return students
    
    def is_user_authorised(self, categories=['administrator'], username=None, user_oid=None, skip_global_admin_check=False):
        if not skip_global_admin_check:
            if is_user_administrator('super', username):
                return True
        if username is None and user_oid is None:
            username = get_auth_user()
        if user_oid is None:
            try:
                _user_oid = session.get('user_oid')
            except:
                _user_oid = None
        else:
            _user_oid = user_oid
        if not _user_oid:
            user = User()
            if user.find_user(username=username, oid=user_oid):
                _user_oid = user._id
        for category in categories:
            if _user_oid in self.config['staff']['{}s'.format(category)]:
                return True
        return False

    def enumerate_views(self, username=None):
        views = []
        for table_view in self.config['views']:
            view = TableView(self)
            if view.load(table_view['uuid']):
                if view.is_authorised_viewer(username):
                    views.append({
                        'table_uuid': self.config['uuid'],
                        'view_uuid': view.config['uuid'],
                        'name': view.config['name'],
                        'description': view.config['description'],
                        'role': view.config['role'],
                        'auth_users': [v for k, v in oids_to_usernames(view.config['auth_users']).items()],
                        'config': view.config['config'],
                        'extra_data': view.config['extra_data'],
                        'column_count': len(view.config['config']['columns'])
                    })
        return views
    
    def get_default_view_uuid(self):
        views = self.enumerate_views()
        for view in views:
            if view.get('role') == 'default':
                return view['view_uuid']
        return None
    
    def get_available_columns(self, data_type=None, uuids_only=False, oids_only=False, exclude_uuids=[]):
        """
            Returns column metadata for all columns in this table.
            Only returns where workflow_state != deleted.
            Returns list of dicts, directly from db.columns
            
            data_type (str) Limit search to a particular config.type
            uuids_only (boolean) If True, returns only uuid and _id
            oids_only (boolean) If True, returns only _id
            exclude_uuids (list of string uuids) If set, excludes these uuids from the results
        """
        filter = {
            'table_uuid': self.config['uuid'],
            'workflow_state': {'$ne': 'deleted'}
        }
        if exclude_uuids:
            filter['uuid'] = {'$nin': exclude_uuids}
        if data_type is not None and data_type != '':
            filter['type'] = data_type
        # grab from db
        results = self.db.columns.find(filter)
        results = list(results)
        # sort
        results = natsorted(results, key=lambda i: str(i['name']) or '', alg=ns.IGNORECASE)
        # return
        if uuids_only:
            return [r['uuid'] for r in results]
        elif oids_only:
            return [r['_id'] for r in results] 
        else:
            return results
    
    def load_all_columns(self, data_type=None):
        """
            Returns a dict of Column instances that have been loaded, keyed by column_uuid.
        """
        column_uuids = self.get_available_columns(data_type=data_type, uuids_only=True)
        ret = {}
        for column_uuid in column_uuids:
            column = Column(self)
            if column.load(column_uuid):
                ret[column_uuid] = copy(column)
        # return
        return ret
    
    def get_all_columns_info(self, data_type=None, get_connections=False):
        """
            Returns a formatted basic set of metadata for columns in this table.
            Returns a dict, keyed by column_uuid
        """
        column_uuids = self.get_available_columns(data_type=data_type, uuids_only=True)
        # Load the metadata
        columns = {}
        column = Column(self)
        for column_uuid in column_uuids:
            if column.load(column_uuid):
                columns[column_uuid] = {
                    k: v for k, v in column.config.items() 
                    if k in [
                        'uuid', 'type', 'table_uuid', 'name', 'datasource', 'workflow_state',
                        'description', 'active', 'coversheet', 'file_link', 'apply_to_others',
                        'custom_options', 'permissions', 'notify_email', 'tags', 'multientry_data_format'
                    ]
                }
                columns[column_uuid]['x_editable'] = True if columns[column_uuid]['type'] in 'mark,attendance,submission,counter,toggle,aggregator,signinout,signinoutmemory'.split(',') + [None] else False
                if column.config.get('multientry_data_format') == True:
                    columns[column_uuid]['x_editable'] = False
                columns[column_uuid]['is_active'] = column.is_active()
                columns[column_uuid]['type_descriptor'] = column.get_datatype_friendly()
                columns[column_uuid]['coversheet_active'] = column.config['coversheet']['html'] is not None and len(column.config['coversheet']['html']) > 0
                columns[column_uuid]['student_editing_allowed'] = column.is_student_editable()
                columns[column_uuid]['student_direct_access_active'] = column.is_student_direct_access_allowed() and (column.is_self_data_entry_enabled() or column.is_peer_data_entry_enabled())
                columns[column_uuid]['student_direct_access_roll_active'] = column.is_student_direct_access_allowed(mode='roll') and (column.is_self_data_entry_enabled() or column.is_peer_data_entry_enabled())
                columns[column_uuid]['peer_data_entry_active'] = column.is_peer_data_entry_enabled()
                columns[column_uuid]['has_multiple_report_mode_enabled'] = column.has_multiple_report_mode_enabled()
                columns[column_uuid]['auto_reset'] = {
                    'active': True if column.config['auto_reset']['active'] == 'true' else False,
                    'job_exists': False
                }
                if columns[column_uuid]['type'] == 'aggregator':
                    columns[column_uuid]['aggregator_recalculation_is_manual'] = True if column.config['aggregation_options']['recalculate_trigger'] == 'manual' else False
                if columns[column_uuid]['auto_reset']['active']:
                    columns[column_uuid]['auto_reset']['job_exists'] = column.check_scheduled_job_exists('reset')
                columns[column_uuid]['auto_backup_email'] = {
                    'active': True if column.config['auto_backup_email']['active'] == 'true' else False,
                    'job_exists': False
                }
                if columns[column_uuid]['auto_backup_email']['active']:
                    columns[column_uuid]['auto_backup_email']['job_exists'] = column.check_scheduled_job_exists('backup_data_email')
                if get_connections:
                    columns[column_uuid]['references'] = {
                        '_for_js': column.get_references_to_other_columns()['_for_js']
                    }
        # Sort
        columns_sorted = natsorted(columns.items(), key=lambda kv: str(kv[1]['name']) or '', alg=ns.IGNORECASE)
        # Return
        return dict(columns_sorted)
    
    def get_additional_identifier_columns(self):
        """
            Finds columns in current table that have been set as containing an 'additional identifier'.
            Returns a list of db results.
        """
        results = self.db.columns.find({'table_uuid': self.config['uuid'], 'custom_options.additional_identifier': 'true'})
        return list(results)
    
    def get_select_array(self, show_collapsed_multientry_option=False, data_type=None, only_column_uuids=None, hide_multientry_subfields=False, sda_only=False, get_text_only=False):
        """
            Returns an array of dicts, specifically for display in column selection UI.
            For speed, this method skips the Column class and interprets db.columns directly.
            
            show_collapsed_multientry_option (bool)
            data_type
            only_column_uuids
            hide_multientry_subfields (bool)
            sda_only (bool)
        """
        ret = []
        columns = self.get_available_columns(data_type=data_type)
        if self._id is None:
            return ret
        # iterate columns
        for column_data in columns:
            if only_column_uuids and type(only_column_uuids) is list and column_data['uuid'] not in only_column_uuids:
                continue
            column = {
                'subfield': None,
                'config': {}
            }
            column['config'] = column_data
            table = {
                '_id': self._id,
                'config': self.config
            }
            # see if only accepting sda columns
            if sda_only and not _is_student_direct_access_allowed(column_data, '__any__'):
                # next column please
                continue
            # load it up
            ret = _get_column_meta_for_select_array(
                column=column,
                table=table,
                ret=ret,
                show_collapsed_multientry_option=show_collapsed_multientry_option,
                hide_multientry_subfields=hide_multientry_subfields,
                get_text_only=get_text_only
            )
        return ret
    
    def store_enrolment_update_status(self, updated_by, update_success, update_source, updated_active_student_count):
        key = "sres.table.enrolments.update.T{table_uuid}.{date_code}.{time_code}".format(
            table_uuid=self.config['uuid'],
            date_code=datetime.now().strftime('%Y-%m-%d'),
            time_code=datetime.now().strftime('%H:%M:%S')
        )
        value = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
			'updated_by': updated_by,
			'update_success': update_success,
			'update_source': update_source,
			'updated_active_student_count': updated_active_student_count
		}
        db_cookie = DbCookie(username_override=updated_by)
        return db_cookie.set(
            key=key,
            value=json.dumps(value),
            use_key_as_is=True
        )
    
    def get_enrolment_update_statuses(self, only_latest=False, only_latest_successful=False):
        key_pattern = 'sres.table.enrolments.update.T{}'.format(self.config['uuid'])
        db_cookie = DbCookie()
        records = db_cookie.get_like(key_pattern=key_pattern, ignore_username=True)
        ret = []
        for record in records:
            value = json.loads(record['value'])
            if only_latest_successful and value['update_success'] == True:
                only_latest = True # This will cause break
            elif only_latest_successful and value['update_success'] != True:
                continue
            else:
                pass
            ret.append({
                'timestamp': value['timestamp'],
                'updated_by': value['updated_by'],
                'update_success': value['update_success'],
                'update_source': value['update_source'],
                'updated_active_student_count': value['updated_active_student_count']
            })
            if only_latest:
                break
        return ret
    
    def get_authorised_usernames(self, roles=[]):
        """
            Returns dicts of lists with authorised usernames at different role types.
            
            roles (list of strings) user|administrator|auditor singular
            
            Returns {
                {role}: []
                {_role}: [{_id, username}]
            }
        """
        ret = {}
        oids = []
        if not roles:
            roles = [r['name'] for r in USER_ROLES]
        for role in roles:
            user_type = '{}s'.format(role)
            ret[user_type] = []
            ret['_{}'.format(user_type)] = []
            oids = oids + self.config['staff'][user_type]
        usernames = oids_to_usernames(oids)
        for role in roles:
            user_type = '{}s'.format(role)
            for oid in self.config['staff'][user_type]:
                if usernames.get(oid):
                    ret[user_type].append(usernames[oid])
                    ret['_{}'.format(user_type)].append({
                        '_id': oid,
                        'username': usernames[oid]
                    })
            ret[user_type] = sorted(ret[user_type])
            ret['_{}'.format(user_type)] = natsorted(ret['_{}'.format(user_type)], key=lambda e: e['username'], alg=ns.IGNORECASE)
        return ret
    
    def update_enrollments(self, filename, field_mapping, remove_not_present=True):
        
        ret = {
            'success': False,
            'original_active': 0,
            'now_active': 0,
            'messages': []
        }
        
        delimiter = field_mapping['delimiter']
        mapping = field_mapping['map']
        if ord(delimiter) == 44:
            sep = ','
        elif ord(delimiter) == 9:
            sep = '\t'
        else:
            return ret
        
        # read
        gf = GridFile('temp')
        if not gf.find_and_load(filename):
            return ret
        df = _file_import_read_file_to_df(self.config['uuid'], filename)
        
        # perform enrolment update
        result = self._update_enrollments(df=df, mapping=mapping)
        ret['messages'].extend(result['messages'])
        for message in result['messages']:
            flash(message[0], message[1])
        # get counts
        ret['original_active'] = result['original_active']
        ret['now_active'] = result['now_active']
        
        ret['success'] = True
        return ret
    
    def _update_enrollments(self, df, mapping, remove_not_present=True, overwrite_details=True):
        """
            Performs the enrolment list update.
            
            df (list of dicts) Containing the student details.
            mapping (dict) Keys are expected identifier types and values 
                are {'field': corresponding header in df}.
            remove_not_present (bool) Whether to make students 'inactive' if they are not present in df.
        """
        ret = {
            'messages': [],
            'original_active': 0,
            'now_active': 0,
            'save_results': {}
        }
        # get original active
        ret['original_active'] = len(self.get_all_students_oids())
        # make all inactive
        if remove_not_present:
            self.db.data.update_many({'table_uuid': self.config['uuid']}, {'$set': {'status': 'inactive'}})
        # iterate through input
        student = StudentData(self)
        fields = NAME_FIELDS + IDENTIFIER_FIELDS
        fields.remove('sid')
        for index, row in enumerate(df):
            student._reset()
            _student_found = False
            try:
                if row[mapping['sid']['field']] is not None and row[mapping['sid']['field']] != '':
                    _student_found = student.find_student(identifiers={ 'sid': row[mapping['sid']['field']] })
                    if overwrite_details:
                        if _student_found:
                            _fields = deepcopy(fields)
                        else:
                            _fields = deepcopy(NAME_FIELDS + IDENTIFIER_FIELDS)
                        for field in _fields:
                            if field in mapping.keys(): #### SAME SAME
                                if mapping[field]['field'] != 'NOIMPORT' and row[mapping[field]['field']] is not None:
                                    if field in NAME_FIELDS:
                                        student.config[field] = utils.titlecase(str(row[mapping[field]['field']])).strip()
                                    else:
                                        student.config[field] = str(row[mapping[field]['field']]).strip()
                                # try a secondary field if specified and if needed
                                if mapping[field].get('secondary_field'):
                                    secondary_field_name = mapping[field].get('secondary_field')
                                    if secondary_field_name != 'NOIMPORT':
                                        if student.config[field] == '' or student.config[field] == '-':
                                            student.config[field] = utils.titlecase(str(row[secondary_field_name]).strip())
                    else:
                        # don't overwrite details
                        pass
                    student.config['status'] = 'active'
                    save_result = student.save()
                    ret['save_results'][student.config['sid']] = save_result
                    if not save_result:
                        ret['messages'].append(('Unexpected error saving details for student {}.'.format(row[mapping['sid']['field']]), 'warning'))
                else:
                    ret['messages'].append(('A student did not have a student identifier so could not be added. Data: {}'.format(str(row)), 'warning'))
            except Exception as e:
                ret['messages'].append(('Unexpected error saving details for student {}.'.format(row[mapping['sid']['field']]), 'warning'))
                logging.error('_update_enrollments could not add student [{}] [{}]'.format(self.config['uuid'], str(row)))
                logging.exception(e)
        # get now active
        ret['now_active'] = len(self.get_all_students_oids())
        return ret
      
    def get_sid_username_map(self):
        ret = {
            'sid_to_username': {},
            'username_to_sid': {}
        }
        results = list(self.db.data.find({
            'table': self._id
        }, ['sid', 'username']))
        for r in results:
            _sid = r.get('sid')
            _username = r.get('username')
            if _sid is not None and _sid not in ret['sid_to_username'].keys():
                ret['sid_to_username'][_sid] = _username
            if _username is not None and _username not in ret['username_to_sid'].keys():
                ret['username_to_sid'][_username] = _sid
        return ret
      
    def preprocess_file_import(self, new_filename):
        # read file to df
        df = _file_import_read_file_to_df(self.config['uuid'], new_filename, nrows=10, make_pickle=True)
        # see if any column headers in dbcookie
        db_cookie = DbCookie()
        remembered_mappings = {}
        for i, header in enumerate(list(df[0].keys())):
            cleaned_source_column_header = re.sub('[^A-Z0-9a-z_]', '_', header)
            cookie_key = 'sres.importdata.mappings.T{table_uuid}.C{cleaned_source_column_header}'.format(
                table_uuid=self.config['uuid'],
                cleaned_source_column_header=cleaned_source_column_header
            )
            cookie = db_cookie.get(key=cookie_key, default=None)
            if cookie:
                remembered_mappings[i] = {
                    'source_header': header,
                    'target_column_uuid': cookie
                }
        # return
        _headers = list(df[0].keys())
        #_data_head = df.head().to_json()
        _data_head = json.dumps({k: [r[k] for r in df[:5]] for k in df[0]}, default=str)
        _row_count = len(df)
        return {
            'headers': _headers,
            'data_head': _data_head,
            'row_count': _row_count,
            'remembered_mappings': remembered_mappings
        }
    
    def process_file_import(self, filename=None, identifier_header_index=None, row_start=None, rows_to_process=None, mapper={}):
        """
            Worker for actually importing data from a file.
            
            filename (str) just the filename of the data file to be imported (not full path)
            identifier_header_index (str) header corresponding to the identifier in file
            row_start (int)
            rows_to_process (int)
            mapper (dict) Determines whether columns will be imported or not, and where.
            
            Returns dict
            {
                records_saved: (int)
                student_data_save: {identifier: {success (boolean), messages (list of tuples)}
                aggregation_results: {aggregator_column_uuid: {successfully_aggregated (int), unsuccessfully_aggregated (int)}}
            }
        """
        df = _file_import_read_file_to_df(
            table_uuid=self.config['uuid'],
            filename=filename,
            skiprows=row_start,
            nrows=rows_to_process,
            use_pickle=True
        )
        
        ret = {}
        table = self
        student_data = StudentData(table)
        db_cookie = DbCookie()
        records_saved = 0
        records_error = 0
        
        # figure out which columns need importing, and update dbcookie with mappings as needed
        t0 = datetime.now()
        columns_to_import = {}
        for i, header in enumerate(list(df[0].keys())): # TODO !!! check if this has all headers if first row is missing data
            cleaned_source_column_header = re.sub('[^A-Z0-9a-z_]', '_', header)
            cookie_key = 'sres.importdata.mappings.T{table_uuid}.C{cleaned_source_column_header}'.format(
                table_uuid=table.config['uuid'],
                cleaned_source_column_header=cleaned_source_column_header
            )
            if mapper[f'column_import_action_{i}'][0] == 'noimport':
                # don't add to columns_to_import
                # but delete from cookies
                db_cookie.delete(key=cookie_key)
            elif mapper[f'column_import_action_{i}'][0] == 'existing':
                column = Column(preloaded_table=table)
                if column.load(mapper[f'column_destination_{i}'][0]):
                    columns_to_import[i] = {
                        'index': i,
                        'header': header,
                        'action': 'existing',
                        'column': column
                    }
                    # remember
                    db_cookie.set(key=cookie_key, value=column.config['uuid'] if not column.is_system_column else column.column_reference)
                else:
                    # uh oh
                    logging.debug(f'Could not load i {i} header {header}')
                    pass
        logging.debug('finished columns_to_import')
        
        identifier_header = list(df[0].keys())[identifier_header_index]
        auth_user_override = get_auth_user()
        t0 = datetime.now()
        for index, row in enumerate(df):
            student_data._reset()
            identifier = str(row[identifier_header]).strip()
            ret[identifier] = {
                'success': False,
                'messages': []
            }
            if student_data.find_student(identifier):
                for i, column_to_import in columns_to_import.items():
                    if row.get(column_to_import['header']):
                        data = row[column_to_import['header']]
                    else:
                        data = ''
                    student_data.set_data(
                        column_uuid=column_to_import['column'].config['uuid'] if not column_to_import['column'].is_system_column else column_to_import['column'].column_reference,
                        data=data,
                        skip_aggregation=True,
                        ignore_active=True,
                        commit_immediately=False,
                        preloaded_column=column_to_import['column'],
                        skip_auth_checks=True,
                        auth_user_override=auth_user_override
                    )
                # then save
                if student_data.save():
                    ret[identifier]['success'] = True
                    records_saved += 1
                else:
                    ret[identifier]['messages'].append(("Unexpected error saving data for identifier {}.".format(identifier), "warning"))
                    records_error += 1
            else:
                ret[identifier]['messages'].append(("Could not find student with identifier {}.".format(identifier), "warning"))
                records_error += 1
        logging.debug('finished student_data.save')
        
        t0 = datetime.now()
        # then calculate aggregations all together now
        #print('requesting bulk aggregation')
        bulk_aggregation_results = run_aggregation_bulk(
            source_column_uuids=[c['column'].config['uuid'] for i, c in columns_to_import.items()],
            target_identifiers=[ r.get(identifier_header) for r in df if r.get(identifier_header) ]
        )
        logging.debug('finished run_aggregation_bulk')
        
        return {
            'records_saved': records_saved,
            'records_error': records_error,
            'student_data_save': ret,
            'aggregation_results': bulk_aggregation_results
        }

    def export_data_to_df(self, export_inactive_students=False, deidentify=False, only_column_uuids=[], classlist=False, identifiers=[], return_just_df=False, do_not_rename_headers=False, view_uuid=None):
        """Exports table data.
            
            export_inactive_students (bool) If True, also exports students marked as 'inactive'.
            deidentify (bool) If True, removes identifiers and instead uses a one-way hashed value as identifier.
            only_column_uuids (list of str)
            classlist (bool)
            identifiers (list of str)
            return_just_df (bool) If True, exits the method early and only returns the raw machine-readable list of data.
            do_not_rename_headers (bool) If True, leaves headers in machine-readable format.
            view_uuid (str | None)
        """
        from io import StringIO
        import csv
        
        filter = {
            'table_uuid': self.config['uuid']
        }
        if not export_inactive_students:
            filter['status'] = 'active'
        if len(identifiers):
            filter['sid'] = {'$in': identifiers}
        # grab column headers
        if view_uuid is not None:
            view = TableView(self)
            if view.load(view_uuid):
                _all_columns_info = view.get_all_columns_info(respect_visibility_and_ordering=True)
                all_columns_info = { c['uuid']:c for c in _all_columns_info }
            else:
                view_uuid = None
        if view_uuid is None:
            all_columns_info = self.get_all_columns_info()
        columns_wanted = []
        if classlist:
            columns_wanted = NAME_FIELDS + ['sid']
        else:
            if not deidentify:
                columns_wanted.extend(NAME_FIELDS)
                columns_wanted.extend(IDENTIFIER_FIELDS)
            if only_column_uuids:
                # is a grouping column active?
                if len(only_column_uuids) == 1 and only_column_uuids[0] in all_columns_info.keys():
                    grouping_column_uuid = all_columns_info[only_column_uuids[0]]['custom_options'].get('grouping_column')
                    if grouping_column_uuid and grouping_column_uuid in all_columns_info.keys():
                        columns_wanted.append(grouping_column_uuid)
                # add to columns_wanted
                columns_wanted.extend(
                    [c for c in only_column_uuids if c in all_columns_info.keys()]
                )
            else:
                columns_wanted.extend(all_columns_info.keys())
        # grab data
        results = self.db.data.find(filter, columns_wanted)
        
        # put together into df
        df = list(results) # not really a dataframe, just a hangover from when pandas was used for this
        
        #if return_just_df is True:
        #    return {
        #        'data': df
        #    }
        
        column_headers = columns_wanted + ['_id']
        rename_mapper = {}
        # accommodate for multientry expansion
        # loop all_columns_info, find where type == 'multiEntry' and then.. insert at location for each subfield
        # use dot column reference as header, and also add to rename_mapper with the subfield label...
        for column_uuid, column_info in all_columns_info.items():
            if only_column_uuids and column_uuid not in only_column_uuids:
                continue
            root_column_loc = column_headers.index(column_uuid)
            if (column_info['type'] == 'multiEntry' or column_info.get('multientry_data_format') == True):
                column = Column()
                if column.load(column_uuid):
                    labels = column.get_multientry_labels()
                    # unpack data
                    unpacked_data = {}
                    for idx, row in enumerate(df):
                        try:
                            unpacked_data[row['_id']] = json.loads(row[column_uuid])
                        except:
                            unpacked_data[row['_id']] = []
                    # add columns and save headers to rename_mapper and add data
                    for l, label in enumerate(labels):
                        header = '{}.{}'.format(column_uuid, l)
                        column_headers.insert(root_column_loc + 1 + l, header)
                        rename_mapper[header] = '{name} >> {subfield_label} {code}'.format(
                            name=column_info['name'],
                            subfield_label=label,
                            code=column_uuid[-8:]
                        )
                        for idx, row in enumerate(df):
                            df[idx][header] = _unpack_multientry_json(unpacked_data, l, row['_id'])
                    # clean mojibake in the full data
                    for idx, row in enumerate(df):
                        try:
                            df[idx][column_uuid] = utils.replace_mojibake(df[idx][column_uuid])
                        except:
                            pass
            elif column_info['type'] == 'signinoutmemory':
                # sign in/out with memory column type should be expanded
                column = Column()
                if column.load(column_uuid):
                    # prepare
                    start_date = column.config['sign_in_out']['week_start']
                    if start_date is not None and (utils.is_datetime(start_date) or isinstance(start_date, datetime)):
                        if not isinstance(start_date, datetime):
                            start_date = parser.parse(start_date)
                        # read db.change_history
                        ch_filter = {
                            'column_uuid': column_uuid,
                            'auth_user': {'$ne': '__system__'}
                        }
                        ch = self.db.change_history.find(
                            ch_filter,
                            ['new_value', 'timestamp', 'identifier', 'auth_user', 'table_uuid', 'column_uuid']
                        ).sort('timestamp', 1)
                        ch = list(ch)
                        # make headers
                        all_timestamps = [ r['timestamp'] for r in ch ]
                        all_timestamps = sorted(all_timestamps)
                        if len(all_timestamps) > 0 and all_timestamps[-1] >= start_date:
                            number_of_weeks = int((all_timestamps[-1] - start_date).total_seconds() / 604800) + 1
                            # iterate to collect data
                            data_by_student_by_week = {}
                            for r in ch:
                                if r['identifier'] not in data_by_student_by_week.keys():
                                    data_by_student_by_week[r['identifier']] = { w: {'in': [], 'out': []} for w in range(1, number_of_weeks + 1) }
                                current_week = int((r['timestamp'] - start_date).total_seconds() / 604800) + 1
                                if current_week < 1:
                                    continue
                                r_in = None
                                r_out = None
                                if utils.is_json(r['new_value']):
                                    try:
                                        r_in = json.loads(r['new_value']).get('in')
                                        r_out = json.loads(r['new_value']).get('out')
                                    except:
                                        r_in = str(r['new_value'])
                                        r_out = str(r['new_value'])
                                else:
                                    if r['new_value'] == '':
                                        r_out = r['timestamp']
                                    else:
                                        r_in = r['new_value']
                                if r_in is not None:
                                    if isinstance(r_in, datetime):
                                        r_in = r_in.strftime('%Y-%m-%d %H:%M:%S')
                                    data_by_student_by_week[r['identifier']][current_week]['in'].append(r_in)
                                if r_out is not None:
                                    if isinstance(r_out, datetime):
                                        r_out = r_out.strftime('%Y-%m-%d %H:%M:%S')
                                    data_by_student_by_week[r['identifier']][current_week]['out'].append(r_out)
                            # iterate weeks to build headers and insert data
                            for w in range(1, number_of_weeks + 1):
                                header = '{}-{}'.format(column_uuid, w)
                                # headers
                                column_headers.insert(root_column_loc + w, header)
                                rename_mapper[header] = '{name} >> week {w} {code}'.format(
                                    name=column_info['name'],
                                    w=w,
                                    code=column_uuid[-8:]
                                )
                                # insert into df
                                for idx, row in enumerate(df):
                                    if data_by_student_by_week.get(row['sid']) is not None:
                                        if len(data_by_student_by_week[row['sid']][w]['in']) > 0 or len(data_by_student_by_week[row['sid']][w]['out']) > 0: 
                                            df[idx][header] = data_by_student_by_week[row['sid']][w]
            else:
                # clean smart quotes and stupid things like that
                for idx, row in enumerate(df):
                    try:
                        df[idx][column_uuid] = utils.replace_mojibake(df[idx][column_uuid])
                    except:
                        pass
        # rename column headers
        final_column_headers = []
        if do_not_rename_headers is True and return_just_df is True:
            # do not rename
            final_column_headers = column_headers
        else:
            for header in column_headers:
                if header in NAME_FIELDS + IDENTIFIER_FIELDS:
                    rename_mapper[header] = next(c for c in SYSTEM_COLUMNS if c['name'] == header)['display']
                elif header in all_columns_info.keys():
                    rename_mapper[header] = '{name} {code}'.format(
                        name=all_columns_info[header]['name'],
                        code=all_columns_info[header]['uuid'][-8:]
                    )
                if header in rename_mapper.keys():
                    final_column_headers.append(rename_mapper[header])
            #df.rename(index=str, columns=rename_mapper, inplace=True)
            for idx, row in enumerate(df):
                for header, new_header in rename_mapper.items():
                    df[idx][new_header] = df[idx].pop(header, '')
        # hash identities
        if deidentify:
            for idx, row in enumerate(df):
                df[idx]['hashed_id'] = sha512((str(row['_id']) + current_app.config['SRES']['DEFAULT_SALT']).encode()).hexdigest()
            final_column_headers.insert(0, 'hashed_id')
        # drop
        for idx, row in enumerate(df):
            del df[idx]['_id']
        # return
        if return_just_df is True:
            return {
                'data': df,
                'headers': final_column_headers
            }
        else:
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, final_column_headers, extrasaction='ignore')
            writer.writeheader()
            for idx, row in enumerate(df):
                writer.writerow(row)
            return {
                'data': df,
                'buffer': csv_buffer,
                'headers': final_column_headers
            }
    
    def find_similar_columns_by_name(self, existing_column_references):
        from Levenshtein import distance
        ret = {}
        #logging.debug('looking for similar columns by name [{}]'.format(self.config['uuid']))
        all_columns_info = self.get_select_array(show_collapsed_multientry_option=True)
        for existing_column_reference, existing_column in existing_column_references.items():
            current_full_column_name = existing_column['friendly_name']
            levenshtein_scores = {}
            for column_info in all_columns_info:
                levenshtein_scores[column_info['value']] = {
                    'existing_column_reference': existing_column_reference,
                    'target_column_reference': column_info['value'], # this is the column reference
                    'levenshtein_score': distance(current_full_column_name, column_info['display_text'])
                }
            levenshtein_scores = dict(sorted(levenshtein_scores.items(), key=lambda kv: kv[1]['levenshtein_score'], reverse=False))
            sorted_keys = [k for k, v in levenshtein_scores.items()]
            #logging.debug(json.dumps(levenshtein_scores))
            suggestions = []
            if len(sorted_keys):
                sorted_keys = sorted_keys[:10]
                for sorted_key in sorted_keys:
                    column_info = next(c for i, c in enumerate(all_columns_info) if c['value'] == sorted_key)
                    suggestions.append({
                        'full_display': column_info['display_text'],
                        'column_reference': sorted_key,
                        'levenshtein_score': levenshtein_scores[sorted_key]['levenshtein_score']
                    })
            ret[existing_column_reference] = {
                'existing_column_reference': existing_column_reference,
                'suggested_target_column_references': suggestions
            }
        return ret
    
    def export_change_history(self, only_column_uuids=[], by=None, try_expand_multientry=False):
        """Exports the change history of the specified columns.
            
            by (str) Whether to limit the return to particular records.
                'multiple_reports_latest': return the latest record for each report
                'user_latest': return the latest record saved by each auth_user
            try_expand_multientry (boolean) Whether to expand multientry subfields also if possible.
        """
        from io import StringIO
        import csv
        headers = ['timestamp', 'identifier', 'auth_user', 'report_number', 'table_uuid', 'column_uuid', 'old_value', 'new_value']
        # determine filter
        filter = {}
        if only_column_uuids:
            filter['column_uuid'] = {'$in': only_column_uuids}
        filter['table_uuid'] = self.config['uuid']
        # determine sort
        sort_by = []
        if by == 'multiple_reports_latest':
            sort_by = [
                ('identifier', -1),
                ('timestamp', -1),
                ('report_number', 1)
            ]
        elif by == 'user_latest':
            sort_by = [
                ('identifier', -1),
                ('timestamp', -1),
                ('auth_user', 1)
            ]
        else:
            sort_by = [ ('timestamp', -1) ]
        # fetch from db.change_history
        results = self.db.change_history.find(filter, headers).sort(sort_by)
        df = list(results) # not really a dataframe, just a hangover from when pandas was used for this
        if len(df):
            # are we returning only specific records?
            if by is not None:
                returned_records = []
                if by == 'multiple_reports_latest' or by == 'user_latest':
                    headers.remove('old_value')
                    found_records_mapper = [] # list of tuples ( identifier, report_number )
                    for _row in df:
                        _identifier = _row.get('identifier')
                        if by == 'multiple_reports_latest':
                            _secondary_key = _row.get('report_number')
                        elif by == 'user_latest':
                            _secondary_key = _row.get('auth_user')
                        if _identifier is not None and _secondary_key is not None:
                            if (_identifier, _secondary_key) not in found_records_mapper:
                                _row.pop('old_value', None)
                                returned_records.append(_row)
                                found_records_mapper.append( (_identifier, _secondary_key) )
                        try_expand_multientry = True
                    df = returned_records
            if try_expand_multientry and len(only_column_uuids) == 1:
                # see if we need to expand multientry subfields
                # only works for single columns
                column = Column()
                if column.load(only_column_uuids[0]):
                    if (column.config['type'] == 'multiEntry' or column.config.get('multientry_data_format') == True):
                        labels = column.get_multientry_labels(get_text_only=True)
                        headers.extend( [f"[{l}] {label}" for l, label in enumerate(labels)] )
                        # iterate returned_records and modify with multientry expansion
                        unpacked_data = {}
                        for r, record in enumerate(df):
                            try:
                                unpacked_data[r] = json.loads(record['new_value'])
                            except:
                                unpacked_data[r] = []
                            additional_data = {}
                            for l, label in enumerate(labels):
                                additional_data[f"[{l}] {label}"] = _unpack_multientry_json(unpacked_data, l, r)
                            df[r] = { **record, **additional_data }
            # continue
            csv_buffer = StringIO()
            writer = csv.DictWriter(csv_buffer, headers, extrasaction='ignore')
            writer.writeheader()
            for idx, row in enumerate(df):
                writer.writerow(row)
            return {
                'df': df,
                'buffer': csv_buffer
            }
        else:
            return None

    def get_make_doc_document(self, identifiers, rows_per_page, columns_per_page, template, qr_width=200, barcode_width=None):
        #import pypandoc
        complete_html = '<html><head><meta charset="UTF-8"></head><body><table>'
        identifier_index = 0
        generate_code128 = '$CODE128$' in template
        generate_qrcode = '$QRCODE$' in template
        student_data = StudentData(self)
        preloaded_columns = _preload_columns(
            input_text=template,
            default_table_uuid=self.config['uuid']
        )
        #logging.debug(str(preloaded_columns.keys()))
        for r in range(0, len(identifiers), columns_per_page):
            complete_html += '<tr>'
            if identifier_index >= len(identifiers):
                break
            for c in range(0, columns_per_page):
                complete_html += '<td>'
                if identifier_index >= len(identifiers):
                    break
                identifier = identifiers[identifier_index]
                student_data._reset()
                if student_data.find_student(identifier):
                    substituted_template = template
                    # list/table info
                    substituted_template = substituted_template.replace('$UOSCODE$', str(self.config['code']))
                    substituted_template = substituted_template.replace('$UOSNAME$', str(self.config['name']))
                    substituted_template = substituted_template.replace('$SEMESTER$', str(self.config['semester']))
                    substituted_template = substituted_template.replace('$YEAR$', str(self.config['year']))
                    # qr and bacodes
                    if generate_code128:
                        barcode = utils.generate_barcode(student_data.config['sid'])
                        substituted_template = substituted_template.replace(
                            '$CODE128$', '<img src="{src}" {width}>'.format(
                                src=barcode['url'],
                                width='width="' + str(barcode_width) + '"' if barcode_width else ''
                            )
                        )
                    if generate_qrcode:
                        barcode = utils.generate_qrcode(student_data.config['sid'])
                        substituted_template = substituted_template.replace(
                            '$QRCODE$', '<img src="{src}" width="{dim}" height="{dim}">'.format(
                                src=barcode['url'],
                                dim=qr_width
                            )
                        )
                    # other details
                    substituted_template = substitute_text_variables(
                        input=substituted_template, 
                        identifier=identifier,
                        default_table_uuid=self.config['uuid'],
                        do_not_encode=True,
                        preloaded_student_data=student_data,
                        preloaded_columns=preloaded_columns
                    )['new_text']
                    complete_html += substituted_template
                # increment
                identifier_index += 1
                complete_html += '</td>'
                ## clean up
                #del student_data
            complete_html += '</tr>'
        complete_html += '</table></body></html>'
        return complete_html

    def save_make_doc_template(self, template_uuid, config):
        if not template_uuid:
            template_uuid = utils.create_uuid()
        self.config['printout_templates'][template_uuid] = config
        self.update()
        return template_uuid
    
    def delete_make_doc_template(self, template_uuid):
        if template_uuid in self.config['printout_templates'].keys():
            del self.config['printout_templates'][template_uuid]
            self.update()
        return True
    
    def list_make_doc_templates(self):
        return self.config['printout_templates']

class TableView:
    
    config = {}
    default_config = {
        'uuid': '',
        'name': 'All columns',
        'description': '',
        'role': '',
        'auth_users': [],
        'config': {},
        'extra_data': {
            'frozencolumns': 2,
            'pagelength': 50,
            'displayrestricted': 'show_all'
        }
    }
    
    def __init__(self, table):
        self.db = _get_db()
        self.table = table
        self.config = deepcopy(self.default_config)
        self.system_columns_overrides = []
        self.user_columns_overrides = []
    
    def load(self, view_uuid=None):
        if view_uuid is not None:
            view_uuid = utils.clean_uuid(view_uuid)
            view_index = self._find_view_index(view_uuid)
            if view_index is not None:
                table_view = self.table.config['views'][view_index]
                # load it up
                self.config['uuid'] = table_view['uuid']
                self.config['name'] = table_view['name']
                self.config['description'] = table_view['description']
                self.config['role'] = table_view['role']
                self.config['auth_users'] = table_view['auth_users'] # oids
                self.config['config'] = table_view['config'] if (isinstance(table_view['config'], dict) and 'columns' in table_view['config'].keys()) else {'columns': []}
                if table_view['extra_data']:
                    self.config['extra_data'] = {**deepcopy(self.default_config['extra_data']), **table_view['extra_data']}
                else:
                    self.config['extra_data'] = deepcopy(self.default_config['extra_data'])
                if 'frozencolumns' in self.config['extra_data'].keys() and utils.is_number(int(self.config['extra_data']['frozencolumns'])):
                    self.config['extra_data']['frozencolumns'] = int(self.config['extra_data']['frozencolumns'])
                # return
                return True
            # if we've reached here, we've iterated all the views and haven't found the right one
            pass
        return False
    
    def update(self):
        if not self.config['uuid']:
            return False
        view_index = self._find_view_index()
        if view_index is not None:
            # Save the default
            if self.config['role'] == 'default':
                self._set_as_default(view_index)
            # Save the page length
            if self.config['config'].get('length', None):
                try:
                    self.config['extra_data']['pagelength'] = int(self.config['config']['length'])
                except:
                    pass
            # Set the config
            self.table.config['views'][view_index] = deepcopy(self.config)
            # Return
            return self.table.update()
        return False
    
    def create(self):
        new_uuid = utils.create_uuid()
        self.config['uuid'] = new_uuid
        # insert into table.config
        self.table.config['views'].append(self.config)
        # update db
        if self.table.update():
            return new_uuid
        return False
    
    def delete(self):
        view_index = self._find_view_index()
        if view_index is not None:
            del self.table.config['views'][view_index]
            return self.table.update()
        return False
    
    def _set_as_default(self, view_index):
        """Set self as default view. This method unsets default on other views."""
        for i, view in enumerate(self.table.config['views']):
            if i != view_index and view['role'] == 'default':
                self.table.config['views'][i]['role'] = 'additional'
        return True
    
    def _find_view_index(self, view_uuid=None):
        if not view_uuid:
            view_uuid = self.config['uuid']
        for i, table_view in enumerate(self.table.config['views']):
            if table_view['uuid'] == view_uuid:
                return i
        return None
    
    def is_authorised_viewer(self, username=None, user_oid=None):
        # convert to oid
        if not user_oid and username:
            user_oid = usernames_to_oids([username])[0]
        elif not user_oid and not username:
            user_oid = get_auth_user_oid()
        if user_oid in self.table.config['staff']['auditors']:
            return True
        if user_oid in self.table.config['staff']['administrators']:
            return True
        if user_oid in self.config['auth_users']:
            return True
        if username is None:
            username = oids_to_usernames([user_oid]).get(user_oid)
        if username is not None:
            if is_user_administrator('super', username=username):
                return True
        return False

    def get_visible_columns_info(self):
        ret = {
            'system': [],
            'user': []
        }
        user_column_info = self.table.get_all_columns_info()
        system_column_info = {
            v['name']: v for v in SYSTEM_COLUMNS
        }
        # Parse depending on overrides
        if len(self.system_columns_overrides) > 0 or len(self.user_columns_overrides) > 0:
            for system_columns_override in self.system_columns_overrides:
                if system_columns_override['checked']:
                    ret['system'].append(system_column_info[system_columns_override['column']])
            for user_columns_override in self.user_columns_overrides:
                if user_columns_override['checked']:
                    ret['user'].append(user_column_info[user_columns_override['column']])
        elif len(self.config['config']) > 0 and (len(self.system_columns_overrides) == 0 and len(self.user_columns_overrides) == 0):
            for current_column in self.config['config']['columns']:
                if current_column['name'] == '_actions':
                    continue
                if current_column['type'] == 'system':
                    if current_column['name'] == 'unikey': # accommodate for legacy
                        current_column['name'] = 'email'
                    if current_column['name'] in system_column_info.keys():
                        ret['system'].append(system_column_info[current_column['name']])
                if current_column['type'] == 'user' and current_column['name'].upper() in user_column_info.keys():
                    ret['user'].append(user_column_info[current_column['name'].upper()])
        else:
            ret['system'] = [v for k, v in system_column_info.items() if v['show']]
            ret['user'] = [v for k, v in user_column_info.items()]
        return ret
    
    def get_all_system_columns_info(self, preloaded_get_visible_columns_info=None):
        if preloaded_get_visible_columns_info is None:
            visible_system_columns_info = self.get_visible_columns_info()['system']
        else:
            visible_system_columns_info = preloaded_get_visible_columns_info['system']
        ret = []
        for system_column in SYSTEM_COLUMNS:
            current_column_info = deepcopy(system_column)
            current_column_info['visible'] = True if system_column['name'] in [c['name'] for c in visible_system_columns_info] else False
            ret.append(current_column_info)
        return ret
    
    def get_all_columns_info(self, data_type=None, preloaded_get_visible_columns_info=None, respect_visibility_and_ordering=False):
        all_columns_info = self.table.get_all_columns_info(data_type)
        if preloaded_get_visible_columns_info is None:
            visible_user_columns_info = self.get_visible_columns_info()['user']
        else:
            visible_user_columns_info = preloaded_get_visible_columns_info['user']
        ret = []
        for column_uuid, user_column in all_columns_info.items():
            current_column_info = deepcopy(user_column)
            current_column_info['visible'] = True if column_uuid in [c['uuid'] for c in visible_user_columns_info] else False
            ret.append(current_column_info)
        if respect_visibility_and_ordering:
            # remove from ret if not visible
            ret = [ c for c in ret if c['visible'] ]
            # order
            ordering_mapper = { c['uuid']:i for i, c in enumerate(visible_user_columns_info) }
            ret = sorted(ret, key=lambda x: ordering_mapper[x['uuid']] if x['uuid'] in ordering_mapper.keys() else 1000)
        return ret
    
    def get_enrolment_update_status(self):
        ret = {
            'timestamp': '',
            'update_source': '',
            'success': False
        }
        records = self.table.get_enrolment_update_statuses(only_latest_successful=True)
        if len(records) > 0:
            ret['timestamp'] = records[0]['timestamp']
            ret['update_source'] = records[0]['update_source']
            ret['success'] = True
        return ret
    
    def load_data(self, dt_input, get_identifiers_only=None):
        
        column = Column(self.table)
        
        # Get available columns
        allowed_column_names = self.table.get_available_columns(uuids_only=True) + [c['name'] for c in SYSTEM_COLUMNS]
        
        # Parse the columns to select and per-column filtering/searching
        select_columns = {}
        search_columns = []
        restrict_by_username_columns = {}
        for requested_column in dt_input['columns']:
            current_column_cleaned = utils.clean_uuid(requested_column['name'])
            if current_column_cleaned in allowed_column_names:
                column.load(current_column_cleaned)
                # add to the columns to select
                select_columns['{}'.format(current_column_cleaned)] = {
                    'key': current_column_cleaned,
                    'type': column.config['type']
                }
                # add to per-column filtering/searching
                if requested_column['search'].get('value') != '':
                    search_columns.append({
                        'column': current_column_cleaned,
						'value': requested_column['search']['value']
                    })
                if column.config['custom_options']['restrict_by_username_column'] != '':
                    restrict_by_username_columns[current_column_cleaned] = {
                        'restrictor': column.config['custom_options']['restrict_by_username_column'],
						'restrictee': current_column_cleaned
                    }
        
        # Parse the ordering
        ordering_directives = []
        ordering_directives_keyed = {}
        for order_directive in dt_input['order']:
            order_by_column = dt_input['columns'][int(order_directive['column'])]['name']
            if order_by_column in allowed_column_names:
                ordering_directives.append({
                    'column': order_by_column,
                    'direction': 1 if order_directive['dir'] == 'asc' else -1,
                    'reverse': False if order_directive['dir'] == 'asc' else True
                })
                ordering_directives_keyed[order_by_column] = ''
        
        # parse the global filtering
        global_search = dt_input['search']['value']
        
        # parse other query elements
        query_start = int(dt_input['start'])
        query_length = int(dt_input['length'])
        show_inactive = True if str(dt_input['show_inactive']) == '1' else False
        show_inactive_only = True if str(dt_input['show_inactive_only']) == '1' else False
        
        # build the filter
        filters = []
        filters.append({
            'table_uuid': self.table.config['uuid']
        })
        if show_inactive_only:
            filters.append({
                'status': 'inactive'
            })
        elif not show_inactive:
            filters.append({
                'status': 'active'
            })
        if global_search != '':
            global_filters = []
            for allowed_column_name in allowed_column_names:
                global_filters.append({
                    str(allowed_column_name): {'$regex': '{}'.format(global_search), '$options': 'i'}
                })
            filters.append({
                '$or': global_filters
            })
        for search_column in search_columns:
            filters.append({
                search_column['column']: {'$regex': '{}'.format(search_column['value']), '$options': 'i'}
            })
        if (not self.table.is_user_authorised() 
            and self.config['extra_data']['displayrestricted'] == 'hide_if_all_restricted' 
            and len(restrict_by_username_columns) > 0):
            restrictor_directives = []
            for restrictee_columnuuid in restrict_by_username_columns:
                restrictor_directives.append({
                    restrict_by_username_columns[restrictee_columnuuid]['restrictor']: {'$regex': get_auth_user(), '$options': 'i'}
                })
            filters.append({
                '$or': restrictor_directives
            })
        filter = {'$and': filters}
        
        # run the query
        results = list(self.db.data.find(filter))
        all_results = self.db.data.find({'table_uuid': self.table.config['uuid']})
        all_results = list(all_results)
        
        # sort if needed
        if len(ordering_directives) > 0:
            for ordering_directive in ordering_directives:
                results = natsorted(results, key=lambda i: i[ordering_directive['column']] if ordering_directive['column'] in i.keys() else '', reverse=ordering_directive['reverse'], alg=ns.IGNORECASE)
        
        # if just getting identifiers, return early
        if get_identifiers_only:
            ret = [ r['sid'] for r in results ]
            return ret
        
        # paginate
        if query_length != -1:
            results = results[query_start:query_start + query_length]
        
        # format the output
        ret = {}
        ret['draw'] = dt_input['draw']
        ret['recordsTotal'] = len(all_results)
        ret['recordsFiltered'] = self.db.data.find(filter).count()
        # format the row data
        results_all_keys = set().union(*(d.keys() for d in results))
        ret['data'] = []
        access_denied_rows = {}
        identity_anonymiser_active = is_identity_anonymiser_active()
        anonymisable_fields = NAME_FIELDS + IDENTIFIER_FIELDS
        for row in results:
            restrict_by_username = {
				'total_restricted_columns': 0,
				'count_restricted_columns': 0
            }
            result_data = {}
            result_data_append_mode = 'append'
            for input_column in dt_input['columns']:
                if input_column['name'] in select_columns.keys():
                    # check for access restrictions by username
                    if input_column['name'] in restrict_by_username_columns and not self.table.is_user_authorised():
                        restrict_by_username['total_restricted_columns'] += 1
                        # TODO PORT
                        
                    # process data to show
                    if input_column['name'] in row.keys():
                        current_data = row[input_column['name']]
                        if select_columns[input_column['name']]['type'] in 'image,file,imgurl'.split(',') and len(current_data) > 0:
                            if select_columns[input_column['name']]['type'] == 'image':
                                result_data[input_column['name']] = '<img class="sres-td-image" src="{}" alt="">'.format(get_file_access_url(escape(current_data)))
                            elif select_columns[input_column['name']]['type'] == 'imgurl':
                                result_data[input_column['name']] = '<img class="sres-td-imgurl" src="{}" alt="">'.format(escape(current_data))
                            elif select_columns[input_column['name']]['type'] == 'file':
                                try:
                                    if current_data != '' and utils.is_json(current_data):
                                        files = json.loads(current_data)
                                        file_links = []
                                        for file in files:
                                            if 'url' in file.keys():
                                                url = file['url']
                                                saved_filename = file['saved_filename']
                                                original_filename = file.get('original_filename', saved_filename)
                                                file_links.append(f'<a href="{url}" target="_blank" aria-label="Download file" title="{escape(original_filename)}"><span class="fa fa-file"></span></a>')
                                            else:
                                                file_links.append(
                                                    '<a href="{}" target="_blank" aria-label="Download file"><span class="fa fa-file"></span></a>'.format(escape(file['filename']))
                                                )
                                        result_data[input_column['name']] = ('&nbsp;').join(file_links)
                                    else:
                                        raise
                                except:
                                    result_data[input_column['name']] = '<a href="{}" target="_blank" aria-label="Download file"><span class="fa fa-file"></span></a>'.format(get_file_access_url(escape(current_data)))
                        else:
                            if identity_anonymiser_active and input_column['name'] in anonymisable_fields:
                                current_data = anonymise(input_column['name'], current_data)
                            result_data[input_column['name']] = escape(current_data)
                    else:
                        result_data[input_column['name']] = ''
            
            result_data['_sid'] = row['sid']
            result_data['DT_RowAttr'] = {
				'data-sres-oid': str(row['_id']),
				'data-sres-sid': row['sid'],
				'data-sres-qr-encoded-id': '{}{}'.format(
                    row['sid'], 
                    row['email'] if 'email' in row.keys() else ''
                ),
				'data-sres-student-status': row['status'],
				'data-sres-session-day': '',
				'data-sres-session-time': '',
				'data-sres-session-otherinformation': '',
				'data-sres-session-fix': ''
            }
            result_data['DT_RowClass'] = 'sres-row-{}'.format(row['status'])
            if result_data_append_mode == 'insert-before':
                ret['data'].append(result_data) #TODO PORT
            else:
                ret['data'].append(result_data)
            
        return ret
        
        
    
