import functools
from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, abort, jsonify, current_app, send_from_directory, make_response, Markup)
from flask.json import htmlsafe_dumps
from werkzeug.utils import secure_filename
import os
import json
import re
from datetime import datetime, timedelta
from dateutil import parser
import urllib
from bs4 import BeautifulSoup
import bleach
import pandas
import base64
from natsort import natsorted, ns
import logging
import itertools
from copy import deepcopy

from sres.auth import login_required, get_auth_user_oid, is_user_administrator, get_auth_user
from sres.tables import list_authorised_tables, load_authorised_tables, Table, TableView, USER_ROLES, AUTOLIST_MAPPINGS, rereference_columns_suggestions
from sres.columns import Column, COLUMN_DATA_TYPES_META, TEXT_ENTRY_REGEX_OPTIONS, MULTI_ENTRY_SUBFIELD_TYPES, SYSTEM_COLUMNS, MAGIC_FORMATTERS_LIST, load_multiple_columns_by_uuids, get_columns_with_tag_in_table, remove_tag_from_column, add_tag_to_column, add_tag_to_multientry, remove_tag_from_multientry, sum_and_save_tags_for_a_set_of_columns, get_all_tag_names_for_a_set_of_columns, get_all_tags_for_a_multi_entry_column
from sres.users import usernames_to_oids
from sres.studentdata import StudentData, search_students, enumerate_distinct_data_by_column
from sres import utils
from sres.aggregatorcolumns import SIMPLE_AGGREGATORS, AggregatorColumn
from sres.connector_canvas import CanvasConnector, CONNECTION_META
from sres.connector_zoom import ZoomConnector, CONNECTION_META as ZOOM_CONNECTION_META
from sres.files import get_file_access_key, get_file_access_url, GridFile
from sres.collective_assets import CollectiveAsset
from sres.tags import get_all_tags, get_all_tags_except_these, get_these_tags
from sres.tag_groups import get_all_tag_groups
from sres.access_logs import add_access_event
from sres import change_history
from sres.anonymiser import is_identity_anonymiser_active, name_to_pseudo
from sres.search import find_students_from_tables_by_term
from sres.go import make_go_url

from bson import ObjectId

bp = Blueprint('table', __name__, url_prefix='/tables')

### TABLE-SPECIFIC ###

@bp.route('/<table_uuid>', methods=['GET', 'POST'])
@login_required
def view_table(table_uuid=None):
    vars = {}
    table = Table()
    if table_uuid is not None and table.load(table_uuid):
        vars['table_uuid'] = table.config['uuid']
        if request.method == 'GET' or (request.method == 'POST' and request.form.get('action', '') == 'set_columns_showing'):
            # if just plain GET-ing, see if we need to load up the default view if a view argument is not specified.
            is_view_specified = 'view' in request.args.keys()
            if not is_view_specified and request.method == 'GET':
                default_view_uuid = table.get_default_view_uuid()
                if default_view_uuid is not None:
                    return redirect(url_for('table.view_table', table_uuid=table.config['uuid'], view=default_view_uuid))
            # otherwise keep loading
            view = TableView(table)
            view_uuid = request.args.get('view', '')
            view.load(view_uuid)
            add_access_event(asset_type='table', asset_uuid=table_uuid, action='view', related_asset_type='view', related_asset_uuid=view_uuid)
            if view.is_authorised_viewer():
                # view overrides
                if request.form.get('action', '') == 'set_columns_showing':
                    view.system_columns_overrides = json.loads(request.form.get('system_columns'))
                    view.user_columns_overrides = json.loads(request.form.get('user_columns'))
                    view.config['extra_data']['frozencolumns'] = int(request.form.get('frozencolumns')) if utils.is_number(request.form.get('frozencolumns')) else 2
                    view.config['extra_data']['displayrestricted'] = request.form.get('displayrestricted', 'show_all')
                # view parameters
                vars['view_unsaved'] = True if len(request.form.get('system_columns', '')) + len(request.form.get('user_columns', '')) > 4 else False
                vars['visible_columns'] = view.get_visible_columns_info()
                vars['visible_columns_uuids'] = [c['uuid'] for c in vars['visible_columns']['user']]
                vars['all_columns_info'] = view.get_all_columns_info(preloaded_get_visible_columns_info=vars['visible_columns'])
                vars['all_system_columns_info'] = view.get_all_system_columns_info(preloaded_get_visible_columns_info=vars['visible_columns'])
                vars['enrolment_update_status'] = view.get_enrolment_update_status()
                vars['is_admin'] = table.is_user_authorised()
                # page title
                vars['page_title'] = table.config['code']
                if view.config.get('role') == 'additional':
                    vars['page_title'] += f" - {view.config.get('name', '')}"
                # LMS
                vars['lms_available'] = []
                if current_app.config['SRES'].get('LMS', {}).get('canvas', {}).get('enabled', False) is True:
                    vars['lms_available'].append('canvas')
                if current_app.config['SRES'].get('LMS', {}).get('zoom', {}).get('enabled', False) is True:
                    vars['lms_available'].append('zoom')
                # render
                return render_template('table-view.html', table=table, view=view, vars=vars)
            else:
                flash("Sorry, you are not authorised to view this.", "warning")
                return render_template('denied.html')
    else:
        flash("Sorry, this list could not be loaded. You may not have the right permissions, or the list may not exist.", "warning")
        return render_template('denied.html')

@bp.route('/<table_uuid>/columns', methods=['GET'])
@login_required
def column_summary(table_uuid):
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        all_columns_info = table.get_all_columns_info()
        vars = {}
        vars['table_uuid'] = table_uuid
        return render_template('column-summary.html', table=table, vars=vars, all_columns_info=all_columns_info)
    else:
        flash("Sorry, you are not authorised to view this.", "warning")
        return render_template('denied.html')

@bp.route('/<table_uuid>/edit', methods=['GET', 'POST'])
@login_required
def edit_table(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='edit')
    # set up variables
    table = Table()
    vars = {}
    vars['mode'] = 'edit'
    vars['table_uuid'] = table_uuid
    vars['user_roles'] = USER_ROLES
    vars['AUTOLIST_MAPPINGS'] = AUTOLIST_MAPPINGS
    vars['DEFAULT_POPULATE_FROM_OPTION'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('LIST_EDIT', {}).get('DEFAULT_POPULATE_FROM_OPTION', 'autoList')
    # check table and permissions
    try:
        if request.method == 'POST' and request.form['action'] == 'new':
            # adding a new list
            if is_user_administrator('list') or is_user_administrator('super'):
                pass
            else:
                flash("Sorry, you do not have the necessary permissions to complete this action.", "warning")
                return render_template('denied.html')
        else:
            table.load(table_uuid)
            if not table.is_user_authorised():
                raise
    except:
        flash("Sorry, this list could not be loaded. You may not have the right permissions, or the list may not exist.", "warning")
        return render_template('denied.html')
    # continue
    if request.method == 'GET':
        return render_template('table-edit.html', table=table, vars=vars)
    elif request.method == 'POST':
        if request.form['action'] == 'new':
            add_access_event(asset_type='table', asset_uuid=table_uuid, action='create')
            new_uuid = table.create()
            if new_uuid:
                # great!
                pass
            else:
                flash("Sorry, there was an error creating the list.", "warning")
                return render_template('table-edit.html', table=table, vars=vars)
        # save the details
        table.config['code'] = request.form['uoscode']
        table.config['name'] = request.form['uosname']
        table.config['year'] = request.form['theyear']
        table.config['semester'] = request.form['thesemester']
        table.config['contact']['name'] = request.form['staffEmailName']
        table.config['contact']['email'] = request.form['staffEmailAddress']
        request_dict = request.form.to_dict(flat=False)
        for user_role in USER_ROLES:
            if 'authorised_{}s'.format(user_role['name']) in request_dict.keys():
                table.config['staff']['{}s'.format(user_role['name'])] = usernames_to_oids(request_dict['authorised_{}s'.format(user_role['name'])])
            else:
                # not present, so clear it
                table.config['staff'][f"{user_role['name']}s"] = []
        table.config['workflow_state'] = 'active'
        try:
            #logging.info("XX updating table " + table.config['uuid'])
            add_access_event(asset_type='table', asset_uuid=table_uuid, action='update')
            if table.update():
                # now update enrolments if necessary
                if 'populate_student_list_from' in request.form.keys():
                    if request.form['populate_student_list_from'] == 'none':
                        pass
                    elif request.form['populate_student_list_from'] == 'autoList':
                        if request.form['action'] == 'new' or (request.form['action'] == 'edit' and 'chkRepopulate' in request.form):
                            # need a file
                            if 'autoListFiles' in request.files and request.files['autoListFiles'].filename:
                                # proceed with loading file
                                file = request.files['autoListFiles']
                                field_mapping = json.loads(request.form['autoList_mapping_map'])
                                new_filename = utils.create_uuid() + '.' + file.filename.split('.')[-1]
                                clean_filename = re.sub('[^A-Z0-9a-z_]', '_', file.filename)
                                gf = GridFile('temp')
                                if gf.save_file(file, new_filename):
                                    logging.info("XX updating enrollments... " + table.config['uuid'])
                                    result = table.update_enrollments(
                                        filename=new_filename, 
                                        field_mapping=field_mapping[clean_filename], 
                                        remove_not_present=True if 'chkRemoveNonExistantStudents' in request.form.keys() else False
                                    )
                                else:
                                    flash('There was a problem saving the temporary file.', 'warning')
                            else:
                                flash('A file was not provided so the enrollments were not saved.', 'warning')
                # return
                flash(Markup("Successfully updated list details. <a href=\"{}\">View list</a>.".format(
                    url_for('table.view_table', table_uuid=table.config['uuid'])
                )), "success")
                return redirect(url_for('table.edit_table', table_uuid=table.config['uuid']))
            else:
                flash('Error updating list.', 'warning')
                return render_template('table-edit.html', table=table, vars=vars)
        except:
            raise
            flash('Error updating list.', 'warning')
            return render_template('table-edit.html', table=table, vars=vars)

@bp.route('/<table_uuid>/clone', methods=['GET', 'POST'])
@login_required
def clone_table(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='clone')
    vars = {}
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised() and (is_user_administrator('list') or is_user_administrator('super')):
        if request.method == 'GET':
            vars['table_uuid'] = table.config['uuid']
            vars['now'] = datetime.now()
            vars['authorised_tables'] = list_authorised_tables()
            vars['source_table_columns'] = table.get_all_columns_info(get_connections=True)
            vars['earliest_date'] = datetime.now()
            for column_uuid, column in vars['source_table_columns'].items():
                if column['active']['from'] and column['active']['from'] < vars['earliest_date']:
                    vars['earliest_date'] = column['active']['from']
            return render_template('table-clone.html', table=table, vars=vars)
        elif request.method == 'POST':
            if request.form.get('column_timeshift', 'noshift') == 'shift':
                timeshift_add_days = (parser.parse(request.form.get('timeshift_to')) - parser.parse(request.form.get('timeshift_from'))).days
            else:
                timeshift_add_days = 0
            if request.form.get('clone_mode') == 'new':
                new_list_details = {
                    'code': request.form.get('list_code'),
                    'name': request.form.get('list_name'),
                    'year': request.form.get('list_year'),
                    'semester': request.form.get('list_semester')
                }
                result = table.clone(
                    mode='new',
                    new_list_details=new_list_details,
                    timeshift_add_days=timeshift_add_days,
                    column_list=request.form.getlist('select_column')
                )
            else:
                result = table.clone(
                    mode='existing',
                    timeshift_add_days=timeshift_add_days,
                    existing_table_uuid=request.form.get('clone_mode_existing_list'),
                    column_list=request.form.getlist('select_column')
                )
            for message in result['messages']:
                flash(message[0], message[1])
            if result['success']:
                flash("Cloned successfully. Viewing clone now.", "success")
                return redirect(url_for('table.view_table', table_uuid=result['destination_table_uuid']))
            else:
                return redirect(url_for('table.view_table', table_uuid=self.config['uuid']))
    else:
        flash("Sorry, you do not have the necessary permissions to complete this action.", "warning")
        return render_template('denied.html')

@bp.route('/new', methods=['GET'])
@login_required
def new_table():
    add_access_event(asset_type='table', asset_uuid=None, action='new')
    if is_user_administrator('list') or is_user_administrator('super'):
        pass
    else:
        flash("Sorry, you do not have the necessary permissions to complete this action.", "warning")
        return render_template('denied.html')
    table = Table()
    # Set current user as administrator by default
    table.config['staff']['administrators'] = [get_auth_user_oid()]
    table.config['uuid'] = '__new__'
    vars = {}
    vars['mode'] = 'new'
    vars['user_roles'] = USER_ROLES
    vars['AUTOLIST_MAPPINGS'] = AUTOLIST_MAPPINGS
    vars['DEFAULT_POPULATE_FROM_OPTION'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('LIST_EDIT', {}).get('DEFAULT_POPULATE_FROM_OPTION', 'autoList')
    return render_template('table-edit.html', table=table, vars=vars)

@bp.route('/<table_uuid>/students/add', methods=['POST'])
@login_required
def add_student(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='add_student')
    table = Table()
    ret = {
        'success': False,
        'already_exists': False,
        'identifier': '',
        'messages': []
    }
    if table.load(table_uuid) and table.is_user_authorised():
        student_details = {}
        if 'sid' not in request.form.keys():
            abort(400)
        sid = request.form.get('sid')
        # see if already exists
        student_data = StudentData(table)
        if student_data.find_student({'sid': sid}):
            ret['already_exists'] = True
            ret['identifier'] = sid
            return json.dumps(ret)
        # collect details
        for system_column in SYSTEM_COLUMNS:
            if system_column['name'] in request.form.keys():
                student_details[system_column['name']] = request.form.get(system_column['name'])
        # send for update
        result = student_data.add_single_student(student_details)
        # return
        ret['identifier'] = result['identifier']
        ret['messages'].extend(result['messages'])
        ret['success'] = result['success']
        return json.dumps(ret)
    else:
        abort(403)

@bp.route('/<table_uuid>/students/<oid_a>/merge/<oid_b>', methods=['POST'])
@login_required
def merge_student_records(table_uuid, oid_a, oid_b):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='merge_student_records')
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised(categories=['administrator']):
        student_data_a = StudentData(table)
        student_data_b = StudentData(table)
        if student_data_a.load_from_oid(oid_a) and student_data_b.load_from_oid(oid_b):
            mapping = {}
            for field in request.form.keys():
                if field.startswith('sres_merge_record_column_'):
                    mapping[field.replace('sres_merge_record_column_', '')] = request.form.get(field)
            res = student_data_a.merge_with(student_data_b, mapping)
            return json.dumps(res, default=str)
        else:
            abort(404)
    else:
        abort(403)
        
@bp.route('/<table_uuid>/students/<oid>', methods=['GET'])
@login_required
def get_student_data(table_uuid, oid):
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised(categories=['administrator', 'auditor']):
        student_data = StudentData(table)
        if student_data.load_from_oid(oid):
            return json.dumps({
                'config': student_data.config,
                'data': student_data.data
            }, default=str)
        else:
            abort(404)
    else:
        abort(403)
    
@bp.route('/<table_uuid>/students/search', methods=['GET'])
@login_required
def search_students_by_term(table_uuid, term=None, column_uuid=None):
    term = request.args.get('term')
    column_uuid = request.args.get('column_uuid')
    table = Table()
    column = Column()
    student_data_requestor = None
    # check authorisation
    authorised = False
    if table.load(table_uuid) and table.is_user_authorised(categories=['user', 'administrator', 'auditor']):
        authorised = True
        if column_uuid:
            column.load(column_uuid)
    else:
        if column.load(column_uuid) and column.is_peer_data_entry_enabled() and column.is_writeable_by_students():
            student_data_requestor = StudentData(table)
            if student_data_requestor.find_student(get_auth_user()):
                authorised = True
    # return results if authorised
    if authorised:
        results = search_students(term=term, table_uuid=table_uuid, column_uuid=column_uuid, preloaded_column=column, preloaded_student_data=student_data_requestor)
        return htmlsafe_dumps(results)
    else:
        abort(403)
        
@bp.route('/<table_uuid>/students/get', methods=['GET'])
@login_required
def get_students_by_term(table_uuid):
    """Similar to search_students_by_term but used only by administrators of the table.
    
    """
    search_term = request.args.get('search', '')
    results = find_students_from_tables_by_term(
        search_term=search_term,
        table_uuids=[table_uuid],
        anonymise_identities=is_identity_anonymiser_active()
    )
    return json.dumps({'students':results})

@bp.route('/<table_uuid>/students/get_all_identifiers', methods=['GET'])
@login_required
def get_all_identifiers(table_uuid):
    include_inactive = True if request.args.get('include_inactive', None) else False
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised(categories=['user', 'administrator', 'auditor']):
        sids = table.get_all_students_sids(only_active=not include_inactive)
        return json.dumps(sids)
    else:
        abort(403)

@bp.route('/<table_uuid>/import', methods=['GET','POST'])
@login_required
def import_data(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='import_data')
    vars = {}
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        if request.method == 'POST':
          vars['method'] = 'POST'
          if request.form.get('input_file_type') == 'scantron_auto_4_5_multi':
            ### scantron 4.5
            vars['scantron_version'] = '4.5'
            xls                      = pandas.ExcelFile(request.files.get('scantron_auto_4_5_multi_file'))
            sheetOne                 = xls.parse(0)
            question_header          = sheetOne.columns.values[4:]
            prefix                   = request.form.get('scantron_auto_4_5_multi_prefix')
            three_column_names       = ['Grade','Percent Score','Total Score']
            student_id_label         = 'Student ID'
          elif request.form.get('input_file_type') == 'scantron_auto_5_2_multi':
            ### scantron 5.2
            vars['scantron_version'] = '5.2'
            xls                      = pandas.ExcelFile(request.files.get('scantron_auto_5_2_multi_file'))
            sheetOne                 = xls.parse(sheet_name='Student Response Report',header=5)
            question_header          = sheetOne.columns.values[1:-3]
            prefix                   = request.form.get('scantron_auto_5_2_multi_prefix')
            three_column_names       = ['Total','Percent','Grade']
            student_id_label         = 'Students:'

          vars['just_check_students'] = request.form.get('just_check_students')

          ## 1 of 3. find students who did not take test/were not enrolled
          students_enrolled = table.get_all_students_sids()
       
          students_who_took_test_ids = [str(student_id).strip() for student_id in sheetOne[student_id_label][1:]]
       
          student_data = StudentData(table)
          students_not_enrolled = []
          i=1
          for student_id in students_who_took_test_ids:
            if not student_data.find_student(student_id):
              # Student not enrolled in this class! Did they mis-write their student ID?
              students_not_enrolled.append(student_id)
            i+=1
          students_who_took_test_list = [str(sid) for sid in students_who_took_test_ids]
          students_who_did_NOT_take_test = [student for student in students_enrolled if student not in students_who_took_test_list]

          vars['students_who_did_NOT_take_test'] = students_who_did_NOT_take_test
          vars['students_not_enrolled']          = students_not_enrolled
       
          if request.form.get('just_check_students') != 'on':

            ## 2 of 3. make five scantron columns
            three_column_uuid = {}

            for column_name in three_column_names:

              column = Column()
              if prefix == '' or prefix is None:
                column.config['name'] = column_name
              else:
                column.config['name'] = prefix + '_' + column_name
              column.config['active']['from'] = datetime.now()
              column.config['active']['to'] = datetime.now()
              column.config['type'] = 'mark'
              three_column_uuid[column_name] = column.create(table_uuid)

            two_types_of_student_data = ['Student Booleans','Student Responses']
            two_types_uuid = {}

            for column_name in two_types_of_student_data:
              column = Column()
              if prefix == '' or prefix is None:
                column.config['name'] = column_name
              else:
                column.config['name'] = prefix + '_' + column_name
              column.config['active']['from'] = datetime.now()
              column.config['active']['to'] = datetime.now()
              column.config['type'] = 'multiEntry'
              if column_name == 'Student Booleans':
                maximumValue = '1'
              else:
                maximumValue = ''
              multi_entry_list = []
              for question in question_header:
                multi_entry_list.append({
                  "label" : question,
                  "maximumValue" : maximumValue,
                  "type" : "regex",
                  "required" : "0",
                  "regex" : ".*",
                  "select" : [ ],
                  "selectmode" : "single",
                  "select_display_mode" : "btn-group",
                  "slider_mode" : "textual",
                  "slider_step" : 1,
                  "range_mode" : "rounddown",
                  "accordion_header" : "",
                  "extra_save_button" : ""})
              column.config['multi_entry'] = {'options': multi_entry_list}
              two_types_uuid[column_name] = column.create(table_uuid)

            ## 3 of 3. fill in student data
            students_who_took_test_ids = [str(student_id).strip() for student_id in sheetOne[student_id_label][1:]]
            answer_key = [sheetOne.iloc[0][question] for question in question_header]
       
            student_data = StudentData(table)
            i=1
            for student_id in students_who_took_test_ids:
              if student_data.find_student(student_id):
                for column_name in three_column_names:
                  student_data.data[three_column_uuid[column_name]] = str(sheetOne[column_name][i])
       
                responses = [sheetOne.iloc[i][question] for question in question_header]
                booleans  = [int(actual_answer == student_answer) for actual_answer,student_answer in zip(answer_key,responses)]
       
                student_data.data[two_types_uuid['Student Responses']] = responses
                student_data.data[two_types_uuid['Student Booleans']] = booleans
       
                student_data.save()
              i+=1


        vars['table_uuid'] = table.config['uuid']
        return render_template('import-data.html', vars=vars, table=table)

    else:
        flash("Sorry, we couldn't load the requested table or you are not authorised.", "danger")
        return render_template('denied.html')



@bp.route('/<table_uuid>/import/<source>/<stage>', methods=['POST'])
@login_required
def import_data_preprocess_file(table_uuid, source, stage):
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        if stage == 'preprocess':
            if 'file' not in request.files:
                abort(400)
            if source == 'generic_spreadsheet':
                # proceed with loading file
                logging.debug('loading file')
                file = request.files['file']
                new_filename = utils.create_uuid() + '.' + file.filename.split('.')[-1]
                clean_filename = re.sub('[^A-Z0-9a-z_]', '_', file.filename)
                gf = GridFile('temp')
                gf.save_file(file, new_filename)
                # process file
                logging.debug('calling preprocess_file_import')
                preprocess_result = table.preprocess_file_import(new_filename=new_filename)
                # get existing columns
                logging.debug('calling get_all_columns_info')
                all_columns_info = table.get_all_columns_info()
                system_columns_info = [ 
                    {
                        'name': v['name'],
                        'display': v['display']
                    } 
                    for v in SYSTEM_COLUMNS 
                    if v['name'] != 'sid' 
                ]
                # return
                logging.debug('returning')
                return json.dumps({
                    'system_filename': new_filename,
                    'clean_filename': clean_filename,
                    'data_head': preprocess_result['data_head'],
                    'headers': preprocess_result['headers'],
                    'destination_columns': [v for k, v in all_columns_info.items()],
                    'system_columns': system_columns_info,
                    'row_count': preprocess_result['row_count'],
                    'remembered_mappings': preprocess_result['remembered_mappings']
                }, default=str)
            else:
                abort(404)
        elif stage == 'create_new_columns':
            new_columns_request = request.form.get('new_columns_request', None)
            new_columns = []
            if new_columns_request:
                for i, c in json.loads(new_columns_request).items():
                    column = Column()
                    if column.create(table_uuid=table.config['uuid']):
                        column.config['name'] = c
                        column.config['active']['from'] = datetime.now()
                        column.config['active']['to'] = datetime.now()
                        column.config['type'] = 'mark'
                        column.update()
                        new_columns.append({
                            'index': int(i),
                            'uuid': column.config['uuid'],
                            'name': column.config['name']
                        })
                    else:
                        # uh oh
                        pass
            return json.dumps(new_columns)
        elif stage == 'import':
            identifier_header_index = int(float(request.form.get('identifier_header_index', None)))
            row_start = int(float(request.form.get('row_start', None)))
            rows_to_process = int(float(request.form.get('rows_to_process', None)))
            filename = request.form.get('filename', None)
            mapper = request.form.get('mapper')
            if identifier_header_index is not None and row_start is not None and filename is not None:
                import_results = table.process_file_import(
                    filename=filename,
                    identifier_header_index=identifier_header_index,
                    row_start=row_start,
                    rows_to_process=rows_to_process,
                    mapper=urllib.parse.parse_qs(mapper)
                )
                return json.dumps(import_results)
            else:
                print('400', identifier_header_index, row_start, filename)
                abort(400)
    else:
        abort(403)

@bp.route('/<table_uuid>/export', methods=['GET', 'POST'])
@login_required
def export_data(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='export_data')
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        if 'classlist' in request.args.keys() and request.method == 'POST':
            export_data = table.export_data_to_df(
                classlist=True,
                identifiers=request.form.get('identifier_list').split(',')
            )
        elif (request.form.get('identifiers') and request.method == 'POST') or request.method == 'GET':
            # see if identifiers is specified
            identifiers = json.loads(request.form.get('identifiers', '[]'))
            # see if column_uuid is specified
            if request.args.get('column_uuids'):
                column_uuids = request.args.get('column_uuids').split(',')
            elif request.form.get('column_uuids'):
                column_uuids = request.form.get('column_uuids').split(',')
            elif request.form.get('column_uuid'):
                column_uuids = [ request.form.get('column_uuid') ]
            else:
                column_uuids = []
            # see if view_uuid is specified
            if request.args.get('view_uuid'):
                view_uuid = request.args.get('view_uuid')
            elif request.form.get('view_uuid'):
                view_uuid = request.form.get('view_uuid')
            else:
                view_uuid = None
            # request export
            export_data = table.export_data_to_df(
                export_inactive_students=True if 'inactive' in request.args.keys() else False,
                deidentify=True if 'deidentify' in request.args.keys() else False,
                only_column_uuids=column_uuids,
                identifiers=identifiers,
                view_uuid=view_uuid
            )
        else:
            abort(400)
        resp = make_response(export_data['buffer'].getvalue())
        _fn = '{code}_{year}_{semester}_export'.format(
            code=utils.clean_uuid(table.config['code']),
            year=table.config['year'],
            semester=table.config['semester']
        )
        _fn = utils.clean_uuid(_fn)
        resp.headers["Content-Disposition"] = "attachment; filename={fn}.{ext}".format(
            fn=_fn,
            ext='csv'
        )
        resp.headers["Content-Encoding"] = "utf-8"
        resp.headers["Content-Type"] = "text/csv; charset=utf-8"
        return resp
    else:
        flash("You are not authorised to complete this action.", "danger")
        return render_template('denied.html')
    pass
    
@bp.route('/<table_uuid>/archive', methods=['PUT'])
@login_required
def archive_table(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='archive')
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        if table.archive():
            return '', 202
        else:
            abort(400)
    else:
        abort(403)
    
@bp.route('/<table_uuid>/unarchive', methods=['PUT'])
@login_required
def unarchive_table(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='unarchive')
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        if table.unarchive():
            return '', 202
        else:
            abort(400)
    else:
        abort(403)
    
@bp.route('/<table_uuid>/delete', methods=['DELETE'])
@login_required
def delete_table(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='delete')
    table = Table()
    if table.load(table_uuid) and is_user_administrator('super'):
        if table.delete():
            return '', 202
        else:
            abort(400)
    else:
        abort(403)

@bp.route('/<table_uuid>/connect/<lms>/refresh/<connection_type>', methods=['GET'])
@login_required
def connect_lms_refresh(table_uuid, lms, connection_type):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='connect_lms_refresh')
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        if lms == 'canvas':
            canvas_connector = CanvasConnector()
            if connection_type in CONNECTION_META.keys():
                canvas_connector.load_connected_course_ids()
                schedule_res = canvas_connector.schedule_task(
                    action="run", 
                    table_uuid=table.config['uuid'],
                    connection_type=connection_type,
                    canvas_course_ids=canvas_connector.connected_course_ids,
                    run_now=True
                )
                return 'OK'
            else:
                abort(400)
        elif lms == 'zoom':
            zoom_connector = ZoomConnector()
            connection_index = request.args.get('connection_index', None) # 1-based not 0-based
            existing_connections = zoom_connector.get_connection(table_uuid=table.config['uuid'], connection_type=connection_type)
            existing_connections = existing_connections.get('connections', [])
            logging.debug(f'existing_connections {existing_connections}')
            if utils.is_number(connection_index):
                connection_index = int(connection_index)
            if connection_index <= len(existing_connections):
                connection_config = existing_connections[connection_index - 1]
                schedule_res = zoom_connector.schedule_task(
                    action='run',
                    table_uuid=table.config['uuid'],
                    connection_type=connection_type,
                    connection_config=connection_config,
                    connection_index=connection_index - 1,
                    run_now=True
                )
                if schedule_res['success']:
                    return 'OK'
                else:
                    abort(400)
            else:
                abort(404)
        else:
            abort(404)
    else:
        abort(403)

@bp.route('/<table_uuid>/connect/<lms>', methods=['GET', 'POST'])
@login_required
def connect_lms(table_uuid, lms):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='connect_lms')
    table = Table()
    if not (table.load(table_uuid) and table.is_user_authorised()):
        flash("Could not load or not authorised.", "danger")
        return render_template('denied.html')
    vars = {}
    vars['CONNECTION_META'] = CONNECTION_META
    vars['table_uuid'] = table.config['uuid']
    vars['is_user_superadministrator'] = is_user_administrator('super')
    if lms == 'canvas':
        act_as = request.args.get('act_as')
        if act_as is not None and not is_user_administrator('super'):
            flash("You are not authorised to act as another user.", "danger")
        canvas_connector = CanvasConnector(act_as)
        # see if need to set auth token
        if request.method == 'POST' and request.form.get('auth_token', '') and 'token' in canvas_connector.config['methods']:
            if canvas_connector.set_auth_token(request.form.get('auth_token')):
                flash("Token updated", "info")
            else:
                flash("There may have been an issue updating the token.", "warning")
        # continue to process request
        if request.method == 'GET':
            # get connections
            canvas_connector.load_connections(table.config['uuid'])
            canvas_connector.load_connected_course_ids()
            connected_course_ids = canvas_connector.connected_course_ids
            connected_course_ids.extend(request.args.getlist('course_ids'))
            connected_course_ids = [int(i) for i in connected_course_ids if utils.is_number(i)]
            connected_course_ids = list(set(connected_course_ids))
            #connected_course_ids = [str(i) for i in connected_course_ids]
            # auth
            vars['auth_token'] = canvas_connector.get_auth_token()
            vars['token_error'] = not canvas_connector.check_token_validity()
            # get choosers
            _courses = canvas_connector.get_courses()
            course_chooser = _courses['courses']
            quiz_chooser = {}
            assignment_chooser = {}
            gradebook_chooser = {}
            if connected_course_ids:
                quiz_chooser = canvas_connector._get_quizzes_for_courses(connected_course_ids)['quizzes']
                assignment_chooser = canvas_connector._get_assignments_for_courses(connected_course_ids)['assignments']
            vars['course_chooser'] = course_chooser
            vars['connected_course_ids'] = connected_course_ids
            vars['quiz_chooser'] = quiz_chooser
            vars['assignment_chooser'] = assignment_chooser
            for con_id, con in vars['CONNECTION_META'].items():
                vars['CONNECTION_META'][con_id]['description'] = vars['CONNECTION_META'][con_id]['description'].format(frequency=vars['CONNECTION_META'][con_id]['frequency'])
            # figure out if any selected courses have concluded
            vars['concluded_courses_ids'] = list(set(connected_course_ids) & set(_courses['concluded_courses_ids']))
            # render
            return render_template('connect-canvas.html', table=table, vars=vars, canvas_connector=canvas_connector)
        elif request.method == 'POST':
            # get connected course ids
            canvas_course_ids = request.form.getlist('select_canvas_course_ids')
            canvas_course_ids = [int(float(i)) for i in canvas_course_ids if utils.is_number(i)]
            # loop through possible connections
            for con_id, con in CONNECTION_META.items():
                if request.form.get(con['form_element'], None):
                    # set the connection
                    additional_data = {}
                    if 'additional_form_elements' in con.keys():
                        for additional_form_element in con['additional_form_elements']:
                            el_name = '{}_{}'.format(con['form_element'], additional_form_element['name'])
                            if '_list' in additional_form_element.keys() and additional_form_element['_list']:
                                if request.form.getlist(el_name):
                                    setting = request.form.getlist(el_name)
                                    additional_data[additional_form_element['key']] = [int(float(s)) for s in setting if utils.is_number(s)]
                            else:
                                if request.form.get(el_name, None):
                                    additional_data[additional_form_element['key']] = request.form.get(el_name)
                    connection_res = canvas_connector.set_connection(
                        table_uuid=table.config['uuid'],
                        canvas_course_ids=canvas_course_ids,
                        connection_type=con_id,
                        additional_data=additional_data
                    )
                    for message in connection_res['messages']:
                        flash(message[0], message[1])
                    schedule_res = canvas_connector.schedule_task(
                        action="update", 
                        table_uuid=table.config['uuid'],
                        connection_type=con_id,
                        canvas_course_ids=canvas_course_ids,
                        run_now=(request.form.get('submit_action', '') == 'update_and_refresh')
                    )
                    for message in schedule_res['messages']:
                        flash(message[0], message[1])
                else:
                    # unset this connection
                    connection_res = canvas_connector.unset_connection(
                        table_uuid=table.config['uuid'],
                        connection_type=con_id
                    )
                    schedule_res = canvas_connector.schedule_task(
                        action="delete",
                        table_uuid=table.config['uuid'],
                        connection_type=con_id
                    )
            return redirect(url_for('table.connect_lms', table_uuid=table_uuid, lms=lms, course_ids=canvas_course_ids))
        else:
            abort(404)
    elif lms == 'zoom':
        zoom_connector = ZoomConnector()
        vars['CONNECTION_META'] = ZOOM_CONNECTION_META.values()
        # get columns from table
        _all_columns = table.get_all_columns_info().values()
        vars['all_columns'] = natsorted(_all_columns, key=lambda x: x.get('name', ''), alg=ns.IGNORECASE)
        # get user details
        vars['auth_token'] = zoom_connector.get_auth_token()
        zoom_user = zoom_connector._get_user_details()
        vars['token_error'] = not zoom_user['success']
        vars['zoom_user'] = zoom_user.get('data', {})
        vars['sres_user'] = { 'username': get_auth_user() }
        # get current user's meeting instances
        user_meetings = zoom_connector._get_user_meetings(get_instances=True, parse_instances=True).get('data', [])
        # get existing connections
        existing_connections = zoom_connector.get_connection(table_uuid=table.config['uuid'], connection_type='past_meeting_participants')
        existing_connections = existing_connections.get('connections', [])
        vars['existing_connections'] = existing_connections
        # make sure vars.user_meetings has additional meetings from saved connection config that may not be retrieved presently
        user_meetings = zoom_connector.extract_meeting_configs_from_connections(
            connections=existing_connections,
            existing_user_meetings=user_meetings,
            merge=True
        )
        vars['user_meetings'] = user_meetings
        # see if getting or posting
        if request.method == 'GET':
            return render_template('connect-zoom.html', table=table, vars=vars, zoom_connector=zoom_connector)
        elif request.method == 'POST':
            keyed_user_meetings = { m['uuid']:m for m in user_meetings }
            connections = []
            for i in range(1, 201):
                # grab data from request.form
                workflow_state = request.form.get(f'connection_workflow_state_{i}', 'deleted')
                if workflow_state == 'deleted':
                    continue
                # identify the zoom meeting configs
                source_zoom_meeting_identifiers = request.form.getlist(f'connection_source_zoom_meetings_{i}')
                meeting_configs = []
                for source_zoom_meeting_identifier in source_zoom_meeting_identifiers:
                    if source_zoom_meeting_identifier in keyed_user_meetings.keys():
                        meeting_configs.append(keyed_user_meetings[source_zoom_meeting_identifier])
                    else:
                        # uh oh
                        logging.error(f"Could not find meeting config for id {source_zoom_meeting_identifier}")
                # column actions
                column_action = request.form.get(f'connection_column_action_{i}', 'noimport')
                column_destination = request.form.get(f'connection_column_destination_{i}', '')
                column_new_name = request.form.get(f'connection_column_new_name_{i}', '')
                column_destination_uuid = ''
                if column_action == 'noimport':
                    pass
                elif column_action == 'new':
                    # make a name
                    if len(column_new_name) == 0:
                        column_new_name = 'Zoom meeting participants for ' + ', '.join(
                            [ m.get('topic', 'Zoom meeting') for m in meeting_configs ] # add meeting topic names
                        )
                    # create a column
                    new_column = Column(table)
                    if new_column.create(table_uuid=table.config['uuid']):
                        new_column.config['name'] = column_new_name
                        new_column.config['active']['from'] = datetime.now()
                        new_column.config['active']['to'] = datetime.now()
                        new_column.config['type'] = 'mark'
                        new_column.update()
                        column_destination_uuid = new_column.config['uuid']
                    else:
                        # uh oh
                        logging.error(f"Could not create new column!")
                elif column_action == 'existing':
                    column_destination_uuid = column_destination
                # save
                connection = {
                    'source_zoom_meetings': source_zoom_meeting_identifiers,
                    'column_destination_uuid': column_destination_uuid,
                    'meeting_configs': meeting_configs,
                    'sres_username': get_auth_user()
                }
                connections.append(connection)
            res = zoom_connector.set_connection(table.config['uuid'], 'past_meeting_participants', {'connections': connections})
            if res['success']:
                flash('Successfully saved connections', 'success')
            else:
                flash('Error saving connections', 'warning')
            return redirect(url_for('table.connect_lms', table_uuid=table.config['uuid'], lms='zoom'))
    else:
        abort(403)

@bp.route('/<table_uuid>/make_doc', methods=['POST'])
@login_required
def make_doc(table_uuid, encoded_identifiers=None, template_uuid=None, _first_step=False):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='make_doc')
    if template_uuid is None:
        template_uuid = request.form.get('template_uuid')
    vars = {
        'defaults': {}
    }
    vars['defaults']['content'] = """
        <p style="text-align:center;"><span style="font-size:16px;">$UOSNAME$<br>$UOSCODE$ | $YEAR$</p>
        <p style="text-align:center;"><span style="font-size:24px;"><strong>$PREFERREDNAME$ $SURNAME$</strong></span></p>
        <p style="text-align:center;"><span style="font-size:12px;">$EMAIL$<br>$QRCODE$<br>$CODE128$</span></p>
    """
    vars['defaults']['qrsize'] = 200
    vars['defaults']['barcode_width'] = ''
    vars['defaults']['rows_per_page'] = 2
    vars['defaults']['columns_per_page'] = 2
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        vars['table_uuid'] = table.config['uuid']
        vars['printout_templates'] = table.list_make_doc_templates()
        vars['template_uuid'] = template_uuid
        vars['template_config'] = vars['printout_templates'][template_uuid] if template_uuid in vars['printout_templates'].keys() else None
        vars['authorised_tables'] = list_authorised_tables()
        vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
        if not request.form.get('action') or _first_step:
            if encoded_identifiers:
                vars['encoded_identifiers'] = encoded_identifiers
            elif request.form.get('encoded_identifiers'):
                vars['encoded_identifiers'] = request.form.get('encoded_identifiers')
            else:
                vars['encoded_identifiers'] = base64.b64encode(bytes(request.form.get('identifiers', ''), 'utf-8')).decode()
            return render_template('table-make-doc.html', table=table, vars=vars)
        elif request.form.get('action') in ['save', 'saveas']:
            _template_uuid = table.save_make_doc_template(
                None if request.form.get('action') == 'saveas' else template_uuid,
                {
                    'name': request.form.get('template_name', 'Saved template'),
                    'rows_per_page': request.form.get('rows_per_page', 2),
                    'columns_per_page': request.form.get('columns_per_page', 2),
                    'html': request.form.get('editor1')
                }
            )
            return make_doc(
                table_uuid=table_uuid, 
                encoded_identifiers=request.form.get('encoded_identifiers'), 
                template_uuid=_template_uuid,
                _first_step=True
            )
        elif request.form.get('action') == 'delete' and template_uuid:
            table.delete_make_doc_template(template_uuid)
            return make_doc(
                table_uuid=table_uuid, 
                encoded_identifiers=request.form.get('encoded_identifiers'),
                template_uuid='',
                _first_step=True
            )
        elif request.form.get('action') == 'make':
            # make!
            encoded_identifiers = request.form.get('encoded_identifiers', '')
            if encoded_identifiers:
                identifiers = base64.b64decode(bytes(encoded_identifiers, 'utf-8')).decode().split(',')
                document = table.get_make_doc_document(
                    identifiers=identifiers, 
                    rows_per_page=int(request.form.get('rows_per_page', 2)), 
                    columns_per_page=int(request.form.get('columns_per_page', 2)), 
                    template=request.form.get('editor1'),
                    qr_width=request.form.get('qrsize', 200),
                    barcode_width=request.form.get('barcode_width', None)
                )
                resp = make_response(document)
                resp.headers["Content-Disposition"] = "attachment; filename={fn}.{ext}".format(
                    fn='{code}_{year}_{semester}_printout'.format(
                        code=utils.clean_uuid(table.config['code']),
                        year=table.config['year'],
                        semester=table.config['semester']
                    ),
                    ext='html'
                )
                resp.headers["Content-Type"] = "text/html"
                return resp
                #return send_from_directory(
                #    document['directory'],
                #    document['filename'],
                #    as_attachment=True,
                #    attachment_filename='{code}_{year}_{semester}_doc.docx'.format(
                #        code=utils.clean_uuid(table.config['code']),
                #        year=table.config['year'],
                #        semester=table.config['semester']
                #    )
                #)
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')
    
@bp.route('/<table_uuid>/my_id', methods=['GET'])
@login_required
def get_my_id(table_uuid):
    identifier = request.args.get('identifier')
    table = Table()
    vars = {}
    vars['identifier'] = None
    if table.load(table_uuid):
        student_data = StudentData(table)
        if table.is_user_authorised(categories=['user', 'administrator', 'auditor']):
            # admin or similar
            vars['mode'] = 'staff'
            if identifier and student_data.find_student(identifier):
                vars['identifier'] = student_data.config['sid']
        else:
            # student or similar - show their own
            vars['mode'] = 'student'
            identifier = get_auth_user()
            if student_data.find_student(identifier):
                vars['identifier'] = student_data.config['sid']
            else:
                flash("Sorry, we couldn't find {}".format(identifier, "warning"))
                return render_template('denied.html')
        if student_data._id:
            vars['preferred_name'] = student_data.config['preferred_name']
            vars['surname'] = student_data.config['surname']
            if is_identity_anonymiser_active():
                vars['preferred_name'] = name_to_pseudo(firstname=vars['preferred_name'])
                vars['surname'] = name_to_pseudo(surname=vars['surname'])
        return render_template('id-view.html', vars=vars, table=table)
    else:
        flash("There was a problem loading the list.", "warning")
        return render_template('denied.html')

@bp.route('/<table_uuid>/audit/history/download', methods=['GET'])
@login_required
def download_change_history(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='download_change_history')
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        column_uuid = request.args.get('column_uuid')
        by = request.args.get('by')
        expand_multientry = request.args.get('expme')
        if column_uuid:
            df = table.export_change_history(
                only_column_uuids=[column_uuid],
                by=by,
                try_expand_multientry=True if expand_multientry == '1' else False
            )
        else:
            df = table.export_change_history(
                only_column_uuids=[],
                by=by,
                try_expand_multientry=True if expand_multientry == '1' else False
            )
        if df is not None:
            resp = make_response(df['buffer'].getvalue())
            resp.headers["Content-Disposition"] = "attachment; filename={fn}.{ext}".format(
                fn='{code}_{year}_{semester}_change_history_report'.format(
                    code=utils.clean_uuid(table.config['code']),
                    year=table.config['year'],
                    semester=table.config['semester']
                ),
                ext='csv'
            )
            resp.headers["Content-Type"] = "text/csv"
            return resp
        else:
            flash("No change history found.", "warning")
            return render_template('denied.html')
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')
    
@bp.route('/<table_uuid>/audit/history/revert', methods=['POST'])
@login_required
def revert_change_history(table_uuid):
    table = Table()
    column_uuid = request.form.get('column_uuid')
    identifier = request.form.get('identifier')
    on_behalf_of = True if request.form.get('on_behalf_of') == 'true' else False
    id = request.form.get('id')
    if table.load(table_uuid) and table.is_user_authorised():
        column = Column(table)
        if identifier and id and column.load(column_uuid) and column.table.config['uuid'] == table.config['uuid']:
            student_data = StudentData(table)
            if student_data.find_student(identifier):
                result = student_data.revert_change_history(_id=id, column_uuid=column_uuid, on_behalf_of=on_behalf_of)
                return json.dumps(result)
            else:
                abort(404)
        else:
            abort(400)
    else:
        abort(403)

@bp.route('/<table_uuid>/audit/history/get', methods=['GET'])
@login_required
def get_change_history(table_uuid):
    table = Table()
    column_uuid = request.args.get('column_uuid')
    identifier = request.args.get('identifier')
    if table.load(table_uuid) and table.is_user_authorised():
        column = Column(table)
        if identifier and column.load(column_uuid) and column.table.config['uuid'] == table.config['uuid']:
            student_data = StudentData(table)
            if student_data.find_student(identifier):
                results = student_data.get_change_history(column_uuids=[column.config['uuid']])
                ret = []
                for result in results:
                    ret.append({
                        'timestamp': result['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                        'value': result['new_value'],
                        'auth_user': result['auth_user'],
                        'real_auth_user': result.get('real_auth_user', ''),
                        'id': str(result['_id']),
                        'report_number': result.get('report_number'),
                        'report_workflow_state': result.get('report_workflow_state')
                    })
                return json.dumps({
                    'history': ret,
                    'multiple_reports_mode_enabled': column.has_multiple_report_mode_enabled()
                })
            else:
                abort(404)
        else:
            abort(400)
    else:
        abort(403)
    
@bp.route('/<table_uuid>/audit/history/visualise', methods=['GET'])
@login_required
def visualise_change_history(table_uuid):
    # TODO
    pass
    

@bp.route('/<table_uuid>/visualise_tags', methods=['GET'])
@login_required
def visualise_tags(table_uuid):
  table = Table()
  if table.load(table_uuid) and table.is_user_authorised():
    vars = {}
    vars['table_uuid'] = table.config['uuid']
    possible_columns   = table.get_select_array()
    vars['tags']       = {}
    for tag in get_all_tags():
       columns = get_columns_with_tag_in_table(table.config['uuid'], tag['_id'])
       if columns:
         vars['tags'][tag['name']] = []
         for column in columns:
            vars['tags'][tag['name']].append(column)

    columns_which_have_tags = list(set(itertools.chain(*list(vars['tags'].values()))))

    vars['columns'] = []
    for possible_column in possible_columns:
      if possible_column['display_text'] in columns_which_have_tags:
        vars['columns'].append(possible_column)

    return render_template('visualise_tags.html', vars=vars, table=table)
  else:
    flash("Unauthorised.", "danger")
    return render_template('denied.html')
    
  

@bp.route('/<table_uuid>/related_assets', methods=['GET'])
@login_required
def view_related_assets(table_uuid):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='view_related_assets')
    vars = {}
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        vars['table_uuid'] = table.config['uuid']
        # filters
        from sres.filters import get_filters_for_table_uuid, Filter
        _filters = get_filters_for_table_uuid(table_uuid)
        filters = []
        for _filter in _filters:
            __filter = Filter()
            __filter._load(_filter)
            filter = {
                'uuid': _filter['uuid'],
                'name': _filter['name'],
                'description': _filter['description'],
                'date_run': '',
                'time_run': '',
                'recipient_count': ''
            }
            if len(_filter['run_history']):
                if isinstance(_filter['run_history'][0]['timestamp'], str):
                    timestamp = parser.parse(_filter['run_history'][0]['timestamp'])
                else:
                    timestamp = _filter['run_history'][0]['timestamp']
                filter['date_run'] = timestamp.strftime('%Y-%m-%d')
                filter['time_run'] = timestamp.strftime('%H:%M:%S')
                filter['recipient_count'] = __filter.get_recipient_sent_count()
            filters.append(filter)
        # portals
        from sres.portals import get_portals_for_table_uuid, Portal
        _portals = get_portals_for_table_uuid(table_uuid)
        portals = []
        for _portal in _portals:
            __portal = Portal()
            __portal._load(_portal)
            portal = {
                'uuid': _portal['uuid'],
                'name': _portal['name'],
                'description': _portal['description'],
                'active_now': __portal.is_portal_available()['available'],
                'active_now_messages': ' '.join([m[0] for m in __portal.is_portal_available()['messages']]),
                'active_from': _portal['active']['from'].strftime('%Y-%m-%d'),
                'active_to': _portal['active']['to'].strftime('%Y-%m-%d'),
                'active_duration': '{:.3g}'.format((_portal['active']['to'] - _portal['active']['from']).total_seconds() / 86400)
            }
            portals.append(portal)
        # render
        return render_template('table-related-assets.html', vars=vars, table=table, filters=filters, portals=portals)
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')

### NON-SPECIFIC ###

@bp.route('/available_data_entry', methods=['GET'])
@login_required
def get_available_data_entry():
    year = int(request.args.get('year', datetime.now().year))
    authorised_tables = list_authorised_tables(
        filter_years=[year],
        only_where_user_is=['user', 'administrator']
    )
    ret = []
    username = get_auth_user()
    user_oid = get_auth_user_oid()
    for authorised_table in authorised_tables:
        table = Table()
        if table.load(authorised_table['uuid']) and table.is_user_authorised(categories=['user', 'administrator']):
            ret_one = {
                'table': {
                    'uuid': table.config['uuid'],
                    'name': table.config['name'],
                    'display_name': table.get_full_name()
                },
                'columns': []
            }
            column_types_with_data_entry_enabled = [k for k, v in COLUMN_DATA_TYPES_META.items() if v['direct_data_entry']]
            all_columns = table.get_available_columns()
            # special way to load all columns at once
            all_columns_loaded = load_multiple_columns_by_uuids([ c['uuid'] for c in all_columns ], table)
            # parse loaded columns
            is_list_admin = table.is_user_authorised()
            for column_uuid, column in all_columns_loaded.items():
                if ((column.is_user_authorised(username=username, user_oid=user_oid, authorised_roles=['user'], skip_global_admin_check=True)) and column.is_active()) or is_list_admin:
                    if column.config['datasource']['mode'] != 'manual':
                        continue
                    ret_one['columns'].append({
                        'uuid': column.config['uuid'],
                        'type': column.config['type'],
                        'standard_entry_column': True if column.config['type'] in column_types_with_data_entry_enabled else False,
                        'table_uuid': table.config['uuid'],
                        'name': column.config['name'],
                        'description': column.config['description'],
                        'display_name': column.get_friendly_name(show_table_info=False),
                        'is_list_admin': is_list_admin,
                        'is_student_editable': column.is_student_editable(),
                        'is_active': column.is_active(),
                        'add_value_link': url_for('entry.add_value', table_uuid=table.config['uuid'], column_uuid=column.config['uuid']),
                        'add_value_bulk_link': url_for('entry.add_value_bulk', table_uuid=table.config['uuid'], column_uuid=column.config['uuid']),
                        'add_value_roll_link': url_for('entry.add_value_roll', table_uuid=table.config['uuid'], column_uuid=column.config['uuid'])
                    })
            ret_one['columns'] = list(natsorted(ret_one['columns'], key=lambda c: c['display_name'], alg=ns.IGNORECASE))
            ret_one['columns'] = list(natsorted(ret_one['columns'], key=lambda c: c['is_active'], reverse=True))
            ret.append(ret_one)
        ret = list(natsorted(ret, key=lambda t: t['table']['display_name'], alg=ns.IGNORECASE))
    return json.dumps(ret)
    
@bp.route('/rereference_columns', methods=['POST'])
@login_required
def rereference_columns():
    target_table_uuid = request.form.get('target_table_uuid')
    existing_columns = json.loads(request.form.get('existing_columns', '[]'))
    table = Table()
    if table.load(target_table_uuid) and table.is_user_authorised():
        res = rereference_columns_suggestions(target_table_uuid, existing_columns)
        return json.dumps(res)
    else:
        abort(403)

@bp.route('/find', methods=['GET'])
@login_required
def find_tables():
    # collect args
    year = request.args.get('year')
    semester = request.args.get('semester')
    code = request.args.get('code')
    requestor = request.args.get('requestor', 'index.index')
    show_archived = True if request.args.get('show_archived', 'hide') == 'show' else False
    user_is_superadmin = is_user_administrator('super')
    return_only_authorised = True
    return_views = False
    return_if_auditor = False
    if requestor == 'table.edit_table' or requestor == 'table.new_table':
        return_only_authorised = False
    elif requestor == 'index.index':
        return_views = True
        return_if_auditor = True
    # find tables
    tables = list_authorised_tables(
        show_archived=show_archived,
        code=code,
        filter_years=[year] if year else [],
        filter_semesters=[semester] if semester else [],
        only_where_user_is=[],
        ignore_authorisation_state=True # because this will be checked below
    )
    user_oid = get_auth_user_oid()
    ret = []
    for table in tables:
        t = {            
            'authorised': False
        }
        if user_oid in table['staff']['administrators'] or user_is_superadmin:
            t['authorised'] = True
        if return_if_auditor and user_oid in table['staff']['auditors']:
            t['authorised'] = True
        if return_only_authorised and t['authorised'] is False:
            continue
        t['uuid'] = table['uuid']
        t['code'] = table['code']
        t['name'] = table['name']
        t['year'] = table['year']
        t['semester'] = table['semester']
        t['workflow_state'] = table['workflow_state']
        t['contact'] = {
            'name': table['contact']['name'],
            'email': table['contact']['email']
        }
        t['views'] = []
        if return_views:
            views = table['views']
            for view in views:
                _view = {
                    'authorised': False
                }
                if user_oid in view['auth_users'] or user_is_superadmin:
                    _view['authorised'] = True
                if return_only_authorised and _view['authorised'] is False:
                    continue
                _view['view_uuid'] = view['uuid']
                _view['table_uuid'] = table['uuid']
                _view['name'] = view['name']
                _view['description'] = view['description']
                _view['role'] = view['role']
                t['views'].append(_view)
        ret.append(t)
    return json.dumps(ret)

### TABLEVIEW/DATA ###

@bp.route('/<table_uuid>/data', methods=['POST'])
@login_required
def load_data(table_uuid, view_uuid=None):
    dt_input = request.get_json(force=True)
    get_identifiers_only = request.args.get('identifiers_only', None)
    table = Table()
    if table.load(dt_input['tableuuid']):
        view = TableView(table)
        view.load(dt_input['viewuuid'])
        if not view.is_authorised_viewer():
            abort(403)
        data = view.load_data(dt_input, get_identifiers_only=get_identifiers_only)
        return jsonify(data)
    else:
        abort(404)

@bp.route('/<table_uuid>/views/list', methods=['GET'])
@login_required
def enumerate_views(table_uuid):
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised(categories=['user', 'administrator', 'auditor']):
        views = []
        for v in table.enumerate_views():
            views.append(
                {
                    'view_uuid': v['view_uuid'],
                    'table_uuid': v['table_uuid'],
                    'name': v['name'],
                    'description': v['description'],
                    'column_count': v['column_count'],
                    'role': v['role']
                }
            )
        return json.dumps({
            'views': views
        })
    else:
        abort(403)

@bp.route('/<table_uuid>/views/<view_uuid>/save', methods=['POST'])
@login_required
def save_view(table_uuid, view_uuid=None):
    # check authorisation
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        view = TableView(table)
        if view_uuid == '__view_uuid__' or request.form.get('view_uuid', None) == '':
            # need to create new view
            if not view.create():
                abort(400)
        else:
            if not view.load(view_uuid):
                abort(404)
        # save from request.form
        view.config['config'] = json.loads(request.form.get('state', '{}'))
        view.config['name'] = request.form.get('name', 'Custom view')
        view.config['description'] = request.form.get('description', '')
        view.config['role'] = request.form.get('role', '')
        view.config['auth_users'] = usernames_to_oids(json.loads(request.form.get('auth_users', '[]')))
        view.config['extra_data']['frozencolumns'] = int(request.form.get('frozencolumns')) if utils.is_number(request.form.get('frozencolumns')) else 2
        view.config['extra_data']['displayrestricted'] = request.form.get('displayrestricted', 'show_all')
        # save
        result = view.update()
        return json.dumps({
            'success': result,
            'view_uuid': view.config['uuid']
        })
    else:
        abort(403)

@bp.route('/<table_uuid>/views/<view_uuid>/delete', methods=['DELETE'])
@login_required
def delete_view(table_uuid, view_uuid=None):
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        view = TableView(table)
        if view.load(request.form.get('view_uuid')):
            return json.dumps({
                'success': view.delete(),
                'messages': []
            })
        else:
            abort(404)
    else:
        abort(403)

### COLUMN-SPECIFIC ###
    
@bp.route('/<table_uuid>/columns/<column_uuid>/view', methods=['GET'])
@login_required
def view_column(table_uuid, column_uuid):
    
    pass

@bp.route('/<table_uuid>/columns/list', methods=['GET'])
@login_required
def list_columns(table_uuid, column_uuids='', data_type=None):
    """
        Returns a list of dicts with metadata of columns in this table
        
        table_uuid
        column_uuids
        data_type
        sda_only
    """
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        column_uuids = request.args.get('column_uuids')
        base_references_only = True if str(request.args.get('base_only', 0)) == '1' else False
        if column_uuids:
            column_uuids = re.sub('[^A-Z0-9a-z_\.,]', '', column_uuids).split(',')
        else:
            column_uuids = []
        select_array = table.get_select_array(
            data_type=request.args.get('data_type'), 
            only_column_uuids=column_uuids,
            show_collapsed_multientry_option=True,
            hide_multientry_subfields=base_references_only,
            sda_only=True if str(request.args.get('sda_only', 0)) == '1' else False,
            get_text_only=True
        )
        return json.dumps({
            'columns': select_array,
            'magic_formatters': MAGIC_FORMATTERS_LIST
        })
    else:
        abort(400)

@bp.route('/<table_uuid>/columns/new/<column_type>', methods=['GET'])
@login_required
def new_column(table_uuid, column_type='standard'):
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='new_column')
    table = Table()
    column = Column()
    column.config['uuid'] = '__new__'
    aggregator_mode = request.args.get('mode', '')
    vars = {}
    vars['mode'] = 'new'
    vars['table_uuid'] = table_uuid
    vars['now'] = datetime.now()
    vars['COLUMN_DATA_TYPES_META'] = COLUMN_DATA_TYPES_META
    vars['TEXT_ENTRY_REGEX_OPTIONS'] = TEXT_ENTRY_REGEX_OPTIONS
    vars['MULTI_ENTRY_SUBFIELD_TYPES'] = MULTI_ENTRY_SUBFIELD_TYPES
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['MAGIC_FORMATTERS_LIST'] = MAGIC_FORMATTERS_LIST
    vars['TAG_AGGREGATION_ENABLED_BY_DEFAULT'] = current_app.config['SRES'].get('FEATURES', {}).get('TAG_AGGREGATION', {}).get('ENABLED_BY_DEFAULT', False)
    vars['FONT_FORMATS'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('FORMATTING', {}).get('FONT_FORMATS', '')
    vars['authorised_tables'] = list_authorised_tables()
    if table.load(table_uuid) and table.is_user_authorised():
        vars['other_available_columns'] = table.get_available_columns(exclude_uuids=[column.config['uuid']])
        if column_type == 'aggregator':
            aggregator_column = AggregatorColumn()
            vars['SIMPLE_AGGREGATORS'] = SIMPLE_AGGREGATORS
            vars['aggregator_mode'] = aggregator_mode
            vars['authorised_tables_instances'] = [table] # yes even for crosslist; user can load more lists via ajax later - this is much less expensive
            # base filters for queryBuilder
            vars['query_builder_filters'] = []
            select_array = table.get_select_array(show_collapsed_multientry_option=True)
            for c in select_array:
                vars['query_builder_filters'].append({
                    'id': c['value'],
                    'label': c['full_display_text'],
                    'type': 'string'
                })
            vars['select_array'] = select_array
            # render
            return render_template('column-aggregator-edit.html', table=table, column=column, aggregator_column=aggregator_column, vars=vars)
        elif column_type == 'tag_aggregator':
            vars['authorised_tables_instances'] = [table] # yes even for crosslist; user can load more lists via ajax later - this is much less expensive
            return render_template('column-tag-aggregator-edit.html', table=table, column=column, vars=vars)
        else:
            return render_template('column-edit.html', table=table, column=column, vars=vars)
    else:
        flash("Sorry, this list could not be loaded. You may not have the right permissions, or the list may not exist.", "warning")
        return render_template('denied.html')

@bp.route('/<table_uuid>/columns/<column_uuid>/delete', methods=['GET'])
@login_required
def delete_column(table_uuid, column_uuid):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='delete', related_asset_type='table', related_asset_uuid=table_uuid)
    # for standard and and aggregator columns
    table = Table()
    column = Column()
    if table.load(table_uuid) and table.is_user_authorised():
        if column.load(column_uuid):
            if column.delete():
                flash("Successfully deleted column.", "info")
            else:
                flash("There was a problem deleting the column.", "warning")
        else:
            flash("Could not load column.", "warning")
    return redirect(url_for('table.view_table', table_uuid=table_uuid))
    pass
    
@bp.route('/<table_uuid>/columns/<column_uuid>/clone', methods=['GET'])
@login_required
def clone_column(table_uuid, column_uuid, target_table_uuid=None):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='clone', related_asset_type='table', related_asset_uuid=table_uuid)
    # for standard and and aggregator columns
    table = Table()
    target_table = Table()
    column = Column()
    cloned_column = Column()
    target_table_uuid = request.args.get('target_table_uuid', None)
    result = None
    if table.load(table_uuid) and table.is_user_authorised() and column.load(column_uuid):
        # also check authorisation for target_table if needed
        if target_table_uuid:
            if target_table.load(target_table_uuid) and target_table.is_user_authorised():
                result = column.clone(target_table_uuid = target_table.config['uuid'])
                if result and cloned_column.load(result):
                    flash("Cloned successfully into other list.", "success")
                    references_to_other_columns = cloned_column.get_references_to_other_columns()
                    if len(references_to_other_columns['_all_uuids_from_other_tables']):
                        flash("This column appears to refer to columns outside of this list. If necessary, make sure these are updated.", "info")
                else:
                    flash("Error when cloning.", "warning")
            else:
                flash("Insufficient permissions for target list.", "danger")
        else:
            result = column.clone()
            if result:
                flash("Cloned successfully.", "success")
            else:
                flash("Error when cloning.", "warning")
    else:
        flash("Insufficient permissions or could not load list or column to be cloned.", "danger")
    if result:
        # go to edit screen of cloned column
        return redirect(url_for('table.edit_column', table_uuid=(target_table_uuid or table_uuid), column_uuid=result))
    else:
        # back to column edit for original column
        return redirect(url_for('table.edit_column', table_uuid=table_uuid, column_uuid=column_uuid))

@bp.route('/<table_uuid>/columns/<column_uuid>/tags', methods=['GET', 'POST'])
@login_required
def edit_column_tags(table_uuid, column_uuid):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='edit_column_tags', related_asset_type='table', related_asset_uuid=table_uuid)
    if request.method == 'POST':
        if request.form.get('action') == 'add_more_tags_to_column':
            tags_to_add = request.form.getlist('tags_to_add_to_dropdown')
            for tag_to_add in tags_to_add:
                add_tag_to_column(column_uuid, tag_to_add)
        elif request.form.get('action') == 'add_tags_to_multientry':
            tags_to_add = request.form.getlist('tags_to_add_to_dropdown')
            for tag_to_add in tags_to_add:
                add_tag_to_multientry(column_uuid, tag_to_add, request.form.get('multientry_label'))
        elif request.form.get('action') == 'remove_tag_from_column':
            remove_tag_from_column(column_uuid, request.form.get('tag_objectid'))
        elif request.form.get('action') == 'remove_tag_from_multientry':
            remove_tag_from_multientry(column_uuid, request.form.get('tag_objectid'), request.form.get('multientry_label'))

    table = Table()
    column = Column()
    if table.load(table_uuid) and table.is_user_authorised() and column.load(column_uuid):
        vars = {}
        vars['table_uuid'] = table_uuid
        vars['column_uuid'] = column_uuid
        vars['tags'] = []
        if column.get_datatype_friendly()['name'] == 'Multi-entry':
            vars['available_tags'] = {}
            for multientry in column.get_multientry_information():
                if 'tags' in multientry:
                    vars['available_tags'][multientry['label']] = get_all_tags_except_these(multientry['tags'])
                else:
                    vars['available_tags'][multientry['label']] = get_all_tags_except_these([])
        else: # normal column
            tags_for_available_query = []
            if column.has_tags:
                for tag in column.get_tags():
                    vars['tags'].append(tag)
                    tags_for_available_query.append(tag['_id'])
            vars['available_tags'] = get_all_tags_except_these(tags_for_available_query)

        multi_entry_labels_to_tags = get_all_tags_for_a_multi_entry_column(column_uuid)

        tag_groups = get_all_tag_groups()

        return render_template('edit-column-tags.html', table=table, column=column, vars=vars, tag_groups=tag_groups, multi_entry_labels_to_tags=multi_entry_labels_to_tags)
    else:
        return render_template('denied.html')
    
@bp.route('/<table_uuid>/columns/<column_uuid>/edit', methods=['GET', 'POST'])
@login_required
def edit_column(table_uuid, column_uuid, collective_mode=None, collective_asset_uuid=None, collective_vars=None):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='edit', related_asset_type='table', related_asset_uuid=table_uuid)
    column = Column()
    aggregator_mode = request.args.get('mode', '')
    if table_uuid == 'null':
        if column.load(column_uuid):
            return redirect(url_for('table.edit_column', table_uuid=column.table.config['uuid'], column_uuid=column.config['uuid'], mode=aggregator_mode))
    table = Table()
    vars = {}
    vars['mode'] = 'edit'
    vars['table_uuid'] = table_uuid
    vars['now'] = datetime.now()
    vars['COLUMN_DATA_TYPES_META'] = COLUMN_DATA_TYPES_META
    vars['TEXT_ENTRY_REGEX_OPTIONS'] = TEXT_ENTRY_REGEX_OPTIONS
    vars['MULTI_ENTRY_SUBFIELD_TYPES'] = MULTI_ENTRY_SUBFIELD_TYPES
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['MAGIC_FORMATTERS_LIST'] = MAGIC_FORMATTERS_LIST
    vars['TAG_AGGREGATION_ENABLED_BY_DEFAULT'] = current_app.config['SRES'].get('FEATURES', {}).get('TAG_AGGREGATION', {}).get('ENABLED_BY_DEFAULT', False)
    vars['FONT_FORMATS'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('FORMATTING', {}).get('FONT_FORMATS', '')
    vars['authorised_tables'] = list_authorised_tables()
    # set some things about the Collective
    if collective_mode:
        vars['collective_sharing'] = True
        vars['collective_sharing_mode'] = collective_mode
        vars['collective_sharing_asset'] = CollectiveAsset()
        vars['collective_vars'] = collective_vars
    else:
        vars['collective_sharing'] = False
    collective_asset_uuid = collective_asset_uuid or request.form.get('collective_asset_uuid', None)
    collective_asset = None
    if request.method == 'POST':
        _authorised_to_edit = False
        if table_uuid == 'collective' and collective_asset_uuid:
            collective_asset = CollectiveAsset()
            if collective_asset.load(collective_asset_uuid) and collective_asset.is_user_authorised_editor():
                _authorised_to_edit = True
        elif table.load(table_uuid) and table.is_user_authorised():
            _authorised_to_edit = True
        if _authorised_to_edit:
            column_uuids = []
            if 'new' in request.form['action']:
                add_access_event(asset_type='column', asset_uuid=column_uuid, action='create', related_asset_type='table', related_asset_uuid=table_uuid)
                # single or bulk mode?
                if 'toggle_bulk_add_mode' in request.form.keys():
                    # bulk mode
                    bulk_occurrences = max(int(request.form['bulk_activefrom_occurrences']), int(request.form['bulk_columnname_increment_occurrences']))
                    other_columnname_patterns = re.findall("\$N[\+\-]*[0-9]*\$", request.form['bulk_columnname_pattern'])
                    for occurrence_number in range(1, bulk_occurrences+1):
                        new_column = Column()
                        new_uuid = new_column.create(table_uuid=table_uuid)
                        if new_uuid:
                            column_uuids.append(new_uuid)
                        else:
                            flash("Sorry, columns could not be created.", "warning")
                            return redirect(url_for('table.new_column', table_uuid=table_uuid))
                        bulk_iterator = int(request.form['bulk_columnname_increment_start']) + ((occurrence_number - 1) * int(request.form['bulk_columnname_increment_step']))
                        new_column.config['name'] = request.form['bulk_columnname_pattern']
                        # find special patterns
                        for other_columnname_pattern in other_columnname_patterns:
                            pattern_operation = other_columnname_pattern[2:3]
                            pattern_operand = other_columnname_pattern[3:-1]
                            if pattern_operation == '-':
                                pattern_iterator = bulk_iterator - int(pattern_operand)
                            elif pattern_operation == '+':
                                pattern_iterator = bulk_iterator + int(pattern_operand)
                            else:
                                pattern_iterator = bulk_iterator
                            new_column.config['name'] = new_column.config['name'].replace(other_columnname_pattern, str(pattern_iterator))
                        # set other details
                        new_column.config['name'] = new_column.config['name'].replace('$N$', str(bulk_iterator))
                        bulk_activefrom_start = parser.parse('{} {}'.format(request.form['bulk_activefrom_start'], request.form.get('bulk_active_from_time', '00:00:00')))
                        bulk_activefrom_stepdays = int(request.form['bulk_activefrom_stepdays'])
                        bulk_activefrom_daysactive = int(request.form['bulk_activefrom_daysactive'])
                        new_column.config['active']['from'] = bulk_activefrom_start + timedelta(days=(occurrence_number - 1) * bulk_activefrom_stepdays)
                        new_column.config['active']['to'] = datetime.combine(
                            (new_column.config['active']['from'] + timedelta(days=(bulk_activefrom_daysactive - 1))).date(),
                            parser.parse(request.form['bulk_active_to_time']).time()
                        )
                        new_column.config['active']['from_time'] = request.form.get('bulk_active_from_time') or column.config['active']['from_time']
                        new_column.config['active']['to_time'] = request.form.get('bulk_active_to_time') or column.config['active']['to_time']
                        new_column.update()
                        column_uuid = new_uuid
                else:
                    # single mode
                    new_column = Column()
                    new_uuid = new_column.create(table_uuid=table_uuid)
                    if new_uuid:
                        new_column.config['name'] = request.form['columnname']
                        if request.form['action'] == 'new_aggregator':
                            column.config['type'] = 'aggregator'
                            new_column.config['active']['from'] = datetime.now()
                            new_column.config['active']['to'] = datetime.now()
                        elif request.form['action'] == 'new_tag_aggregator':
                            column.config['type'] = 'tag_aggregator'
                            new_column.config['active']['from'] = datetime.now()
                            new_column.config['active']['to'] = datetime.now()
                            tags = get_all_tag_names_for_a_set_of_columns(request.form.getlist('select_attributes'))
                            multientryoptions = []
                            for tag in tags:
                                multientryoptions.append(
                                    {
                                        "label" : '{} total'.format(tag),
                                        "type" : "regex",
                                        "required" : "0",
                                        "regex" : ".*",
                                        "select" : [ ],
                                        "selectmode" : "single",
                                        "select_display_mode" : "btn-group",
                                        "slider_mode" : "textual",
                                        "slider_step" : 1,
                                        "range_mode" : "rounddown",
                                        "accordion_header" : "",
                                        "extra_save_button" : ""
                                    }
                                )
                                multientryoptions.append(
                                    {
                                        "label" : '{} percentage'.format(tag),
                                        "type" : "regex",
                                        "required" : "0",
                                        "regex" : ".*",
                                        "select" : [ ],
                                        "selectmode" : "single",
                                        "select_display_mode" : "btn-group",
                                        "slider_mode" : "textual",
                                        "slider_step" : 1,
                                        "range_mode" : "rounddown",
                                        "accordion_header" : "",
                                        "extra_save_button" : ""
                                    }
                                )
                            new_column.config['multi_entry']['options'] = multientryoptions
                        else:
                            new_column.config['active']['from'] = parser.parse('{} {}'.format(request.form['activefrom'], request.form['active_from_time']))
                            new_column.config['active']['to'] = parser.parse('{} {}'.format(request.form['activeto'], request.form['active_to_time']))
                            column.config['active']['from_time'] = request.form.get('active_from_time') or column.config['active']['from_time']
                            column.config['active']['to_time'] = request.form.get('active_to_time') or column.config['active']['to_time']
                        new_column.update()
                        column_uuids.append(new_uuid)
                        column_uuid = new_uuid
                    else:
                        flash("Sorry, columns could not be created.", "warning")
                        return redirect(url_for('table.new_column', table_uuid=table_uuid))
            elif request.form['action'] == 'edit' or request.form['action'] == 'edit_aggregator':
                add_access_event(asset_type='column', asset_uuid=column_uuid, action='update', related_asset_type='table', related_asset_uuid=table_uuid)
                # edit mode, so load existing column
                column_uuids.append(column_uuid)
            # update
            for uuid in column_uuids:
                if 'new_tag_aggregator' == request.form['action']:
                  print('a TAG aggregator column')
                elif 'aggregator' in request.form['action']:
                    column = AggregatorColumn()
                    _current_column_type = 'aggregatorcolumn'
                else:
                    column = Column()
                    _current_column_type = 'column'
                if collective_asset_uuid is None or collective_asset_uuid == '':
                    if not column.load(uuid):
                        flash("There was a problem loading a column.", "warning")
                    if collective_asset_uuid == '':
                        # adding a new collective asset
                        collective_asset = CollectiveAsset(_current_column_type)
                        if collective_asset.create() and collective_asset.set_new_asset(column):
                            column = collective_asset.asset
                        else:
                            flash("Sorry, there was a problem making the Collective asset.", "danger")
                            return render_template('denied.html')
                else:
                    # collective asset exists
                    collective_asset = CollectiveAsset(_current_column_type)
                    if collective_asset.load(collective_asset_uuid):
                        column = collective_asset.asset
                        vars['collective_sharing_asset'] = collective_asset
                    else:
                        flash("Sorry, there was a problem loading the Collective asset.", "danger")
                        return render_template('denied.html')
                if column._id:
                    if 'edit' in request.form['action']:
                        # set system_name if not already, before modifying it
                        if column.config['system_name'] == '' and column.config['datasource'].get('mode') == 'sync':
                            column.config['system_name'] = column.config['name']
                        # only set these on edit, since on new these are saved previously
                        column.config['name'] = request.form['columnname']
                        if 'aggregator' not in request.form['action']:
                            column.config['active']['from'] = parser.parse('{} {}'.format(request.form['activefrom'], request.form['active_from_time']))
                            column.config['active']['to'] = parser.parse('{} {}'.format(request.form['activeto'], request.form['active_to_time']))
                            column.config['active']['from_time'] = request.form.get('active_from_time') or column.config['active']['from_time']
                            column.config['active']['to_time'] = request.form.get('active_to_time') or column.config['active']['to_time']
                    column.config['description'] = request.form['columndescription']
                    # update workflow_state if editing column
                    if collective_asset_uuid is None:
                        column.config['workflow_state'] = 'active'
                    # save base config
                    if 'new_tag_aggregator' == request.form['action']:
                        column.config['type'] = 'multiEntry'
                    elif 'aggregator' in request.form['action']:
                        # aggregator
                        column.config['type'] = 'aggregator'
                    else:
                        # not aggregator
                        column.config['active']['range_from_time'] = request.form.get('active_range_from_time') or column.config['active']['range_from_time']
                        column.config['active']['range_to_time'] = request.form.get('active_range_to_time') or column.config['active']['range_to_time']
                        column.config['type'] = request.form.get('dataType') or column.config['type']
                        column.config['simple_input']['allow_free'] = request.form.get('allowFreeInput') or column.config['simple_input']['allow_free']
                        column.config['simple_input']['allow_free_regex'] = request.form.get('allowFreeInputRegex') or column.config['simple_input']['allow_free_regex']
                        column.config['simple_input']['options'] = json.loads(request.form.get('selectFromListList')) if utils.is_json(request.form.get('selectFromListList')) else request.form.get('selectFromListList', [])
                        column.config['notify_email']['active'] = request.form.get('notifyEmail') or column.config['notify_email']['active']
                        column.config['notify_email']['subject'] = request.form.get('notifyEmailSubject') or column.config['notify_email']['subject']
                        column.config['notify_email']['body'] = request.form.get('notifyEmailBody') or column.config['notify_email']['body']
                        column.config['apply_to_others']['active'] = request.form.get('apply_to_others_active') or column.config['apply_to_others']['active']
                        column.config['apply_to_others']['other_columnuuid'] = request.form.get('apply_to_others_other_columnuuid') or column.config['apply_to_others']['other_columnuuid']
                        column.config['counter']['max'] = int(float(request.form.get('counterMax'))) or column.config['counter']['max']
                        column.config['counter']['increment'] = int(float(request.form.get('counterIncrement'))) or column.config['counter']['increment']
                        column.config['auto_proceed'] = request.form.get('autoProceed') or column.config['auto_proceed']
                        column.config['quick_info']['single'] = request.form.get('quickInfo') or column.config['quick_info']['single']
                        column.config['quick_info']['bulk'] = request.form.get('quickInfoBulk') or column.config['quick_info']['bulk']
                        column.config['file_link'] = request.form.get('file_link_active') or column.config['file_link']
                        column.config['sign_in_out']['week_start'] = request.form.get('signinoutmemory_firstweek') or column.config['sign_in_out']['week_start']
                        column.config['sign_in_out']['on_out'] = request.form.get('signinout_onout') or column.config['sign_in_out']['on_out']
                        column.config['sign_in_out']['message_welcome'] = request.form.get('signinout_message_welcome') or column.config['sign_in_out']['message_welcome']
                        column.config['sign_in_out']['message_goodbye'] = request.form.get('signinout_message_goodbye') or column.config['sign_in_out']['message_goodbye']
                        column.config['auto_reset']['active'] = request.form.get('auto_reset_active') or column.config['auto_reset']['active']
                        column.config['auto_reset']['time'] = parser.parse(request.form.get('auto_reset_hour')) or column.config['auto_reset']['time']
                        column.config['auto_reset']['value'] = request.form.get('auto_reset_value', '')
                        column.config['auto_backup_email']['active'] = request.form.get('auto_backup_email_active') or column.config['auto_backup_email']['active']
                        column.config['auto_backup_email']['interval_minutes'] = int(request.form.get('auto_backup_email_interval')) or column.config['auto_backup_email']['interval_minutes']
                        column.config['auto_backup_email']['start_time'] = parser.parse(request.form.get('auto_backup_email_start')) or column.config['auto_backup_email']['start_time']
                        column.config['auto_backup_email']['end_time'] = parser.parse(request.form.get('auto_backup_email_end')) or column.config['auto_backup_email']['end_time']
                        column.config['auto_backup_email']['email_target'] = request.form.get('auto_backup_email_address') or column.config['auto_backup_email']['email_target']
                    # custom options
                    for key, value in column.config['custom_options'].items():
                        if 'custom_options_{}'.format(key) in request.form.keys():
                            if key in ['quickinfo_rollview', 'grouping_column', 'peer_data_entry_condition_column', 'peer_data_entry_match_column', 'restrict_by_username_column', 'only_show_condition_column', 'maximumValue', 'datatype_file_allowed_extensions']:
                                # respect the exact setting
                                column.config['custom_options'][key] = request.form.get('custom_options_{}'.format(key), '')
                            else:
                                column.config['custom_options'][key] = request.form.get('custom_options_{}'.format(key)) or column.config['custom_options'][key]
                    if 'new_tag_aggregator' == request.form.get('action'):
                        print('new tag aggregator bit not sure what this does')
                    elif 'aggregator' not in request.form.get('action'):
                        # multientry
                        if request.form.get('multi_entry_sort_order'):
                            multi_entry_options = []
                            multi_entry_sort_order = request.form.get('multi_entry_sort_order').replace('id=', '').split('&')
                            for i in multi_entry_sort_order:
                                temp = {}
                                temp['label'] = request.form.get('multi_entry_label_{}'.format(i), '')
                                temp['maximumValue'] = request.form.get('multi_entry_maximumValue_{}'.format(i), '')
                                temp['type'] = request.form.get('multi_entry_type_{}'.format(i), 'regex')
                                temp['required'] = request.form.get('multi_entry_required_{}'.format(i), '0')
                                temp['render_calculated_value'] = request.form.get('multi_entry_render_calculated_value_{}'.format(i), 'no')
                                temp['render_calculated_value_config'] = json.loads(request.form.get('multi_entry_render_calculated_value_config_{}'.format(i), '{}'))
                                temp['editing_allowed_by'] = request.form.get('multi_entry_editing_allowed_by_{}'.format(i), 'anyone')
                                temp['regex'] = request.form.get('multi_entry_regex_{}'.format(i), '.*')
                                temp['select'] = json.loads(request.form.get('multi_entry_select_{}'.format(i), '[]')) if utils.is_json(request.form.get('multi_entry_select_{}'.format(i))) else request.form.get('multi_entry_select_{}'.format(i), '[]')
                                temp['selectmode'] = request.form.get('multi_entry_select_mode_{}'.format(i), 'single')
                                temp['select_display_mode'] = request.form.get(f'multi_entry_select_display_mode_{i}', 'btn-group')
                                temp['slider_mode'] = request.form.get('multi_entry_slider_mode_{}'.format(i), 'textual')
                                temp['slider_step'] = request.form.get('multi_entry_slider_step_{}'.format(i), 1.0)
                                temp['range_mode'] = request.form.get('multi_entry_range_mode_{}'.format(i), 'rounddown')
                                temp['accordion_header'] = request.form.get('multi_entry_accordion_header_{}'.format(i), 'no')
                                temp['extra_save_button'] = request.form.get('multi_entry_extra_save_button_{}'.format(i), 'no')
                                if not isinstance(temp['slider_step'], float):
                                    try:
                                        temp['slider_step'] = float(temp['slider_step'])
                                    except:
                                        temp['slider_step'] = 1.0
                                multi_entry_options.append(temp)
                            column.config['multi_entry']['options'] = multi_entry_options
                    # aggregator options
                    if 'new_tag_aggregator' == request.form['action']:
                        columns = request.form.getlist('select_attributes')
                        sum_and_save_tags_for_a_set_of_columns(table_uuid,columns,new_uuid)
                    elif 'aggregator' in request.form['action']:
                        aggregation_options = {}
                        for simple_aggregator in SIMPLE_AGGREGATORS:
                            if simple_aggregator['parameters']:
                                for parameter in simple_aggregator['parameters']:
                                    param_name = 'aggregator_type_simple_{}_parameter_{}'.format(simple_aggregator['name'], parameter['name'])
                                    aggregation_options[param_name] = request.form.get(param_name, '')
                        aggregation_options['aggregator_type_mathematical_operations_formula'] = request.form.get('aggregator_type_mathematical_operations_formula', '')
                        aggregation_options['aggregator_type_mapper_inputs'] = request.form.get('aggregator_type_mapper_inputs', '').replace('\r', '').split('\n')
                        aggregation_options['aggregator_type_mapper_outputs'] = request.form.get('aggregator_type_mapper_outputs', '').replace('\r', '').split('\n')
                        aggregation_options['method'] = request.form.get('aggregator_type', '')
                        aggregation_options['recalculate_trigger'] = request.form.get('recalculate_trigger', '')
                        aggregation_options['rounding'] = request.form.get('post_calculation_rounding', '')
                        aggregation_options['rounding_direction'] = request.form.get('post_calculation_rounding_direction', 'nearest')
                        aggregation_options['post_aggregation_arithmetic_operator'] = request.form.get('post_aggregation_arithmetic_operator', '')
                        aggregation_options['post_aggregation_arithmetic_value'] = request.form.get('post_aggregation_arithmetic_value', '')
                        aggregation_options['regex_replace_pattern'] = request.form.get('regex_replace_pattern', '')
                        aggregation_options['regex_replace_mode'] = request.form.get('regex_replace_mode', '')
                        aggregation_options['regex_replace_replacement'] = request.form.get('regex_replace_replacement', '')
                        aggregation_options['attributes'] = request.form.getlist('select_attributes')
                        aggregation_options['blank_handling'] = request.form.get('aggregation_options_blank_handling', 'leave')
                        # axis
                        aggregation_options['axes'] = []
                        for axis in ['t', 'r']:
                            if request.form.get('aggregation_axis_{}'.format(axis)):
                                aggregation_options['axes'].append(axis)
                        # axis settings
                        aggregation_options['t_axis_source'] = request.form.get('aggregation_axis_t_source', 'all')
                        aggregation_options['t_axis_source_limit'] = request.form.get('aggregation_axis_t_source_limit', 'no')
                        aggregation_options['t_axis_source_limit_from'] = parser.parse('{} {}'.format(
                            request.form.get('aggregation_axis_t_source_limit_date_from'),
                            request.form.get('aggregation_axis_t_source_limit_time_from')
                        ))
                        aggregation_options['t_axis_source_limit_to'] = parser.parse('{} {}'.format(
                            request.form.get('aggregation_axis_t_source_limit_date_to'),
                            request.form.get('aggregation_axis_t_source_limit_time_to')
                        ))
                        # collect case builder cases
                        cases = request.form.get('aggregator_type_case_builder_cases', '')
                        case_builder_cases = []
                        case_builder_contents_contain_html_tag = False
                        if cases and utils.is_json(cases):
                            cases = json.loads(cases)
                            for case_n in cases:
                                case = {
                                    'content': request.form.get('case_content-case-x{}'.format(case_n), ''),
                                    'default_case': '1' if request.form.get('case_conditions_default-case-x{}'.format(case_n)) else '0',
                                    'rules': {}
                                }
                                if not case_builder_contents_contain_html_tag and re.match('<.+?>', case['content']):
                                    case_builder_contents_contain_html_tag = True
                                rules = request.form.get('rules-for-builder-basic-case-x{}'.format(case_n), '')
                                if utils.is_json(rules):
                                    rules = json.loads(rules)
                                    case['rules'] = rules
                                case_builder_cases.append(case)
                        aggregation_options['aggregator_type_case_builder_cases'] = case_builder_cases
                        if aggregation_options['method'] == 'case_builder' and case_builder_contents_contain_html_tag and column.config['custom_options']['allow_html'] != 'true':
                            flash("Rich text (HTML) seems to be enabled as part of case builder outputs, but HTML output from this column is not currently enabled. To allow the rich text to present properly, you may need to enable the 'Allow raw HTML' advanced setting.", "warning")
                        # self and peer review settings
                        aggregation_options['aggregator_type_self_peer_review_grouping_column'] = request.form.get('aggregator_type_self_peer_review_grouping_column', '')
                        aggregation_options['aggregator_type_self_peer_review_score_column'] = request.form.get('aggregator_type_self_peer_review_score_column', '')
                        # combine all together
                        column.config['aggregation_options'] = {**column.config['aggregation_options'], **aggregation_options}
                    # permissions
                    column.config['permissions']['edit']['user']['mode'] = request.form.get('permissions_edit_mode_user', 'allow')
                    column.config['permissions']['edit']['user']['except'] = request.form.getlist('permissions_edit_except_user')
                    column.config['permissions']['edit']['student']['mode'] = request.form.get('permissions_edit_mode_student', 'deny')
                    # scheduled jobs
                    if collective_asset_uuid is None:
                        column.update_scheduled_job(
                            type='reset', 
                            data=column.config['auto_reset']['value'], 
                            hour=column.config['auto_reset']['time'].hour,
                            minute=column.config['auto_reset']['time'].minute
                        )
                        column.update_scheduled_job(
                            type='backup_data_email', 
                            email_target=column.config['auto_backup_email']['email_target'],
                            interval_minutes=column.config['auto_backup_email']['interval_minutes'], 
                            start_time=column.config['auto_backup_email']['start_time'],
                            end_time=column.config['auto_backup_email']['end_time']
                        )
                    # update db
                    if column.update():
                        flash("Column configuration successfully updated.", "success")
                        if 'new_tag_aggregator' == request.form.get('action'):
                            print('tag aggregator bit')
                        elif 'aggregator' in request.form.get('action') and collective_asset_uuid is None:
                            # offer to recalculate
                            flash(Markup("""
                                You can <a href="#" class="sres-aggregator-recalculate-now"><span class="fa fa-calculator"></span> recalculate now</a>.
                            """), "info")
                            # if saving an aggregator, now need to update other columns' aggregated_by
                            update_others_result = column.update_other_columns_aggregated_by()
                            flash("Also updated {} source columns. {}".format(
                                len(update_others_result['successful']), 
                                "Failed {}.".format(len(update_others_result['failed'])) if len(update_others_result['failed']) else ''
                            ), "info")
                            if update_others_result['failed']:
                                for message in update_others_result['messages']:
                                    flash(message[0], message[1])
                    else:
                        flash("There was a problem updating the column configuration.", "warning")
                # save collective, if applicable
                if collective_asset is not None:
                    if collective_asset.set_metadata_from_form(request.form, process_share_referenced_columns=False if collective_asset_uuid else True) and collective_asset.update_referenced_columns_config():
                        flash("Collective asset successfully updated.", "success")
                    else:
                        flash("Unexpected error updating Collective asset.", "warning")
            # render
            if collective_asset is not None:
                return redirect(url_for('collective.show_asset', asset_uuid=collective_asset.config['uuid']))
            else:
                return redirect(url_for('table.edit_column', table_uuid=table_uuid, column_uuid=column_uuids[0], mode=aggregator_mode))
        else:
            flash("Sorry, this asset could not be loaded or you may not have the right permissions.", "warning")
            return render_template('denied.html')
    elif request.method == 'GET':
        add_access_event(asset_type='column', asset_uuid=column_uuid, action='view', related_asset_type='table', related_asset_uuid=table_uuid)
        authorised_to_load = False
        vars['available_columns_from_collective_asset'] = []
        if collective_asset_uuid:
            # collective asset exists
            collective_asset = CollectiveAsset()
            if collective_asset.load(collective_asset_uuid) and (collective_asset.is_user_authorised_editor() or collective_asset.is_user_authorised_viewer()):
                column = collective_asset.asset
                vars['collective_sharing_asset'] = collective_asset
                vars['other_available_columns'] = []
                vars['available_columns_from_collective_asset'] = collective_asset.get_select_array_for_referenced_columns()
                authorised_to_load = True
        else:
            authorised_to_load = table.load(table_uuid) and column.load(column_uuid) and table.is_user_authorised() and column.config['table_uuid'] == table.config['uuid']
            vars['other_available_columns'] = table.get_available_columns(exclude_uuids=[column.config['uuid']])
        vars['source_collective_asset'] = {
            'original_referenced_columns': []
        }
        if request.args.get('from_collective_asset_uuid'):
            collective_asset = CollectiveAsset()
            if collective_asset.load(request.args.get('from_collective_asset_uuid')):
                vars['source_collective_asset']['original_referenced_columns'] = collective_asset.get_select_array_for_referenced_columns()
        _existing_distinct_data = enumerate_distinct_data_by_column(column, '.*', search_any=True, sort_results=False)
        vars['data_exists'] = True if (len(_existing_distinct_data) > 1 or (len(_existing_distinct_data) == 1 and len(_existing_distinct_data[0]) > 0)) else False
        if authorised_to_load:
            if column.config['type'] == 'aggregator':
                aggregator_column = AggregatorColumn()
                if aggregator_column.load(column.config['uuid']):
                    vars['SIMPLE_AGGREGATORS'] = SIMPLE_AGGREGATORS
                    vars['aggregator_mode'] = aggregator_mode
                    vars['simple_aggregator_used'] = True if column.config['aggregation_options']['method'] in [v['name'] for v in SIMPLE_AGGREGATORS] else False
                    vars['simple_aggregator_used'] = True if column.config['aggregation_options']['method'] == 'mapper' else vars['simple_aggregator_used']
                    authorised_and_referenced_table_uuids = aggregator_column.get_referenced_table_uuids(user_must_be_admin=True)
                    vars['referenced_table_uuids'] = aggregator_column.get_referenced_table_uuids(get_from_all_methods=False)
                    vars['referenced_table_uuids_all_methods'] = aggregator_column.get_referenced_table_uuids(get_from_all_methods=True)
                    vars['authorised_and_referenced_table_uuids'] = authorised_and_referenced_table_uuids
                    if aggregator_mode == 'crosslist':
                        # for crosslist, load all referenced and authorised tables
                        vars['authorised_tables_instances'] = []
                        for authorised_and_referenced_table_uuid in authorised_and_referenced_table_uuids:
                            vars['authorised_tables_instances'].append(Table())
                            vars['authorised_tables_instances'][len(vars['authorised_tables_instances']) - 1].load(authorised_and_referenced_table_uuid)
                    else:
                        vars['authorised_tables_instances'] = [table]
                    # base filters for queryBuilder and select_array
                    vars['query_builder_filters'] = []
                    if collective_asset_uuid:
                        vars['select_array'] = collective_asset.get_select_array_for_referenced_columns()
                        for c in vars['select_array']:
                            vars['query_builder_filters'].append({
                                'id': c['value'],
                                'label': c['full_display_text'],
                                'type': 'string'
                            })
                    else:
                        select_array = table.get_select_array(show_collapsed_multientry_option=True)
                        for c in select_array:
                            vars['query_builder_filters'].append({
                                'id': c['value'],
                                'label': c['full_display_text'],
                                'type': 'string'
                            })
                        vars['select_array'] = select_array
                    # render
                    return render_template('column-aggregator-edit.html', table=table, column=column, aggregator_column=aggregator_column, vars=vars)
                else:
                    flash('Sorry, could not load aggregator', 'warning')
                    return render_template('denied.html')
            else:
                return render_template('column-edit.html', table=table, column=column, vars=vars)
        else:
            flash("Sorry, this column could not be loaded. You may not have the right permissions, or the column may not exist.", "warning")
            return render_template('denied.html')

@bp.route('/<table_uuid>/columns/<column_uuid>/quick_access', methods=['GET'])
@login_required
def show_quick_access_links(table_uuid, column_uuid):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='show_quick_access_links', related_asset_type='table', related_asset_uuid=table_uuid)
    table = Table()
    column = Column()
    vars = {}
    if table.load(table_uuid) and table.is_user_authorised(categories=['user', 'administrator', 'auditor']) and column.load(column_uuid):
        vars['table_uuid'] = table.config['uuid']
        vars['user'] = get_auth_user()
        vars['now'] = datetime.now()
        return render_template('column-quick-access-links.html', table=table, column=column, vars=vars)
    else:
        flash("Sorry, there was a problem loading the list or column, or you may not be authorised.", "danger")
        return render_template('denied.html')
    
@bp.route('/<table_uuid>/columns/<column_uuid>/details', methods=['GET'])
@login_required
def get_details(table_uuid, column_uuid):
    
    pass
    
@bp.route('/<table_uuid>/columns/<column_uuid>/multi_edits', methods=['GET'])
@login_required
def show_multi_edits(table_uuid, column_uuid):
    
    pass
    
@bp.route('/<table_uuid>/columns/<column_uuid>/null_counts', methods=['GET'])
@login_required
def show_null_counts(table_uuid, column_uuid):
    
    pass
    
@bp.route('/<table_uuid>/columns/<column_uuid>/sda', methods=['GET'])
@login_required
def show_student_direct_access_links(table_uuid, column_uuid):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='show_student_direct_access_links', related_asset_type='table', related_asset_uuid=table_uuid)
    vars = {}
    mode = request.args.get('mode', 'single')
    vars['mode'] = mode
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised(categories=['user', 'administrator', 'auditor']):
        vars['table_uuid'] = table.config['uuid']
        column = Column(table)
        if column.load(column_uuid):
            if (column.is_student_direct_access_allowed(mode=mode) and (column.is_self_data_entry_enabled() or column.is_peer_data_entry_enabled())):
                if vars['mode'] == 'single':
                    vars['url'] = url_for('entry.add_value', table_uuid=table.config['uuid'], column_uuid=column.config['uuid'], sdak=column.get_student_direct_access_key(), _external=True)
                elif vars['mode'] == 'roll':
                    vars['url'] = url_for('entry.add_value_roll', table_uuid=table.config['uuid'], column_uuid=column.config['uuid'], sdak=column.get_student_direct_access_key(), _external=True)
                vars['go_url'] = make_go_url(vars['url'])
                return render_template('column-student-direct-access-links.html', table=table, column=column, vars=vars)
            else:
                flash("This column is not configured for student direct access for {} mode.".format(mode), "warning")
                return render_template('denied.html')
        else:
            flash("Error loading column information.", "danger")
            return render_template('denied.html')
    else:
        flash("Insufficient permissions to perform this action.", "danger")
        return render_template('denied.html')
    
@bp.route('/<table_uuid>/columns/<column_uuid>/coversheet/edit', methods=['GET', 'POST'])
@login_required
def edit_coversheet(table_uuid, column_uuid):
    
    pass

@bp.route('/<table_uuid>/columns/<column_uuid>/data/get', methods=['GET'])
@login_required
def get_data(table_uuid, column_uuid):
    logging.debug('request received')
    # grab params
    sdak = request.args.get('sdak')
    identifier = request.args.get('identifier')
    report_index = request.args.get('report_index', -1) # for the multiple_reports mode
    return_all_reports = request.args.get('return_all_reports', '') # for the multiple_reports mode
    sda_mode = request.args.get('sda_mode') # not currently used
    # set up
    column = Column()
    # check auth and get data
    logging.debug('loadingcolumn')
    if column.load(column_uuid):
        # permissions checks
        logging.debug('checkingperms')
        if column.is_user_authorised(authorised_roles=['user', 'administrator'], sdak=sdak, sda_mode=sda_mode):
            # set data
            logging.debug('instantiating studentdata')
            student_data = StudentData(column.table)
            logging.debug('findingstudent')
            if student_data.find_student(identifier):
                logging.debug('getting data')
                _data = student_data.get_data_for_entry(column, report_index=report_index)
                logging.debug('processing for return')
                ret = {}
                ret[identifier] = {
                    'data': json.dumps(_data['data']),
                    'multiple_reports_meta': {
                        'index': _data['report_index'],
                        'count': _data['report_available_number_count']
                    }
                }
                # include all reports if requested
                if return_all_reports == 'yes':
                    ret[identifier]['all_reports_data'] = _data['all_reports_data']
                # return
                logging.debug('returning')
                return json.dumps(ret, default=str)
            else:
                abort(404)
        else:
            abort(403)
    else:
        abort(400)
    
@bp.route('/<table_uuid>/columns/<column_uuid>/data/set', methods=['POST'])
@login_required
def set_data(table_uuid, column_uuid, identifier=None, identifiers=[], ignore_notify=False, ignore_apply_to_others=False, data=''):
    # grab form data
    identifier = request.form.get('identifier')
    report_index = request.form.get('report_index', -1) # for multiple reports mode
    quick_info = request.form.get('quick_info')
    echo = json.loads(request.form.get('echo', '{}'))
    identifiers = request.form.getlist('identifiers[]')
    ignore_notify = True if request.form.get('ignore_notify', 'false') == 'true' else False
    ignore_apply_to_others = True if request.form.get('ignore_apply_to_others', 'false') == 'true' else False
    data = request.form.get('data')
    sdak = request.args.get('sdak')
    sda_mode = request.args.get('sda_mode')
    table = Table()
    if not table.load(table_uuid):
        abort(400)
    column = Column(table)
    ret = {
        #identifier: {
        #    'success': False,
        #    'data': {},
        #    'messages': [],
        #    'notify_email': {},
        #    'apply_to_others': {}
        #    'load_existing_data_mode': 'latest' # default, per custom_options_load_existing_data
        #}
    }
    if identifiers:
        original_identifier = None
    else:
        identifiers = [identifier]
        original_identifier = identifier
    if column.load(column_uuid, default_table_uuid=table.config['uuid']):
        # permissions checks
        if column.is_user_authorised(authorised_roles=['user', 'administrator'], sdak=sdak, sda_mode=sda_mode):
            # set data
            student_data = StudentData(column.table)
            #logging.debug('{}-{}-{}'.format(column.table.config['uuid'], column.config['uuid'], column_uuid))
            for identifier in identifiers:
                student_data._reset()
                ret[identifier] = {
                    'success': False,
                    'data': {},
                    'messages': [],
                    'notify_email': {},
                    'apply_to_others': {},
                    'echo': echo,
                    'multiple_reports_meta': {
                        'index': None,
                        'count': None
                    },
                    'load_existing_data_mode': column.config['custom_options'].get('load_existing_data', 'latest')
                }
                if student_data.find_student(identifier):
                    # check time delay lockout if necessary
                    if student_data.in_time_delay_lockout(
                        column_uuid=column_uuid, 
                        seconds_differential=column.config['custom_options']['time_delay_lockout_duration'] 
                    ):
                        abort(429)
                    # clean input as necessary
                    if column.config['type'] == 'multiEntry':
                        data = utils.bleach_multientry_data(
                            data=data,
                            column=column
                        )
                    # set data for this student
                    primary_result = student_data.set_data(
                        column_uuid=column.column_reference if column.is_system_column else column.config['uuid'],
                        data=data,
                        commit_immediately=True,
                        student_direct_access_key=request.args.get('sdak'),
                        preloaded_column=column,
                        threaded_aggregation=True,
                        report_index=report_index,
                        only_save_history_if_delta=False
                    )
                    ret[identifier]['success'] = primary_result['success']
                    ret[identifier]['status_code'] = primary_result['status_code']
                    ret[identifier]['is_aggregated_by_others'] = primary_result['is_aggregated_by_others']
                    if primary_result['success']:
                        # echo the data
                        ret[identifier]['data']['saved'] = data
                        # if image column, need to also provide access key/url
                        if column.config['type'] == 'image':
                            ret[identifier]['data']['key'] = get_file_access_key(data)
                            ret[identifier]['data']['url'] = get_file_access_url(data, full_path=True)
                        # return some info about the student
                        ret[identifier]['person'] = {
                            'preferred_name': student_data.config['preferred_name'],
                            'surname': student_data.config['surname'],
                            'sid': student_data.config['sid']
                        }
                        # return quick_info if necessary
                        if quick_info and quick_info in ['bulk', 'roll', 'single']:
                            ret[identifier]['quick_info'] = student_data.get_quick_info(None, type=quick_info, preloaded_column=column, preloaded_student_data=student_data)['quick_info_html']
                        # return multiple_reports_index if necessary
                        ret[identifier]['multiple_reports_meta']['index'] = primary_result['multiple_reports_meta'].get('index', '')
                        ret[identifier]['multiple_reports_meta']['count'] = primary_result['multiple_reports_meta'].get('count', '')
                    ret[identifier]['messages'].extend(primary_result['messages'])
                    # process notifyemail if needed
                    if not ignore_notify and column.config['notify_email']['active'] == 'true':
                        notify_email_result = student_data.send_notify_email(column.config['uuid'], bypass_auth_check=True)
                        ret[identifier]['notify_email'] = notify_email_result
                        ret[identifier]['messages'].extend(notify_email_result['messages'])
                    # process apply to others if needed
                    if not ignore_apply_to_others and column.config['apply_to_others']['active'] == 'true' and column.config['apply_to_others']['other_columnuuid']:
                        apply_to_others_result = student_data.apply_to_others(
                            original_column=column, 
                            other_column_uuid=column.config['apply_to_others']['other_columnuuid'], 
                            notify_by_email=True if column.config['notify_email']['active'] == 'true' else False,
                            threaded_aggregation=True,
                            sdak=sdak,
                            sda_mode=sda_mode
                        )
                        ret[identifier]['apply_to_others'] = apply_to_others_result
                        ret[identifier]['messages'].extend(apply_to_others_result['messages'])
                else:
                    print('404', identifier)
                    #abort(404)
                    return 'could not find {}'.format(identifier), 404
            if original_identifier:
                # single identifier mode
                if ret[original_identifier]['success']:
                    return json.dumps(ret[original_identifier])
                elif ret[original_identifier]['status_code']:
                    abort(ret[original_identifier]['status_code'])
            else:
                return json.dumps(ret)
        else:
            abort(403)
    else:
        abort(400)

@bp.route('/<table_uuid>/columns/<column_uuid>/data/set_rich', methods=['POST'])
@login_required
def set_rich_data(table_uuid, column_uuid):
    
    column = Column()
    ret = {
        'identifier': None,
        'success': False,
        'data': None,
        'messages': []
    }
    
    type = bleach.clean(request.form.get('t', ''))
    identifier = bleach.clean(request.form.get('i', ''))
    sdak = bleach.clean(request.args.get('sdak', ''))
    if sdak == '':
        sdak = None
    sda_mode = bleach.clean(request.args.get('sda_mode', ''))
    if sda_mode == '':
        sda_mode = None
    
    if column.load(column_uuid):
        # permissions checks
        if column.is_user_authorised(authorised_roles=['user', 'administrator'], sdak=sdak, sda_mode=sda_mode):
            # set data
            student_data = StudentData(column.table)
            if student_data.find_student(identifier):
                if type == 'image':
                    data = bleach.clean(request.form.get('d', ''))
                    result = student_data.set_data_image(
                        column_uuid=column_uuid,
                        image_data=data,
                        save_image_only=True,
                        student_direct_access_key=sdak
                    )
                    ret['success'] = result['success']
                    ret['data'] = result['new_image_filename']
                    ret['identifier'] = identifier
                    ret['messages'].extend(result['messages'])
                elif type == 'audio':
                    f = request.files['d']
                    result = student_data.set_data_rich(
                        table_uuid=column.table.config['uuid'],
                        column_uuid=column_uuid,
                        identifier=identifier,
                        rich_data=f,
                        student_direct_access_key=sdak
                    )
                    ret['success'] = result['success']
                    ret['data'] = result['saved_as_filename']
                    ret['identifier'] = identifier
                    ret['messages'].extend(result['messages'])
                elif type == 'file':
                    #logging.debug(request.files.getlist('files'))
                    ret['data'] = []
                    ret['identifier'] = identifier
                    for i, file in enumerate(request.files.getlist('files')):
                        if i >= int(column.config['custom_options']['datatype_file_multiple_number']):
                            # reached the file upload limit
                            ret['messages'].append(("Too many files selected", "warning"))
                            break
                        _result = student_data.set_data_rich(
                            table_uuid=column.table.config['uuid'],
                            column_uuid=column_uuid,
                            identifier=identifier,
                            rich_data=file,
                            student_direct_access_key=sdak
                        )
                        if i == 0:
                            ret['success'] = _result['success']
                        else:
                            ret['success'] = ret['success'] and _result['success']
                        if _result['success']:
                            ret['data'].append({
                                'original_filename': file.filename,
                                'saved_filename': _result['saved_as_filename'],
                                'url': get_file_access_url(_result['saved_as_filename'], full_path=True)
                            })
                        ret['messages'].extend(_result['messages'])
                else:
                    ret['message'] = "Bad type request"
            else:
                ret['message'] = "Student not found"
    else:
        ret['message'] = "Unauthorised"
    return json.dumps(ret)  

@bp.route('/<table_uuid>/columns/<column_uuid>/aggregator/recalculate', methods=['POST'])
@login_required
def recalculate_aggregator(table_uuid, column_uuid):
    identifiers = request.form.getlist('identifiers[]')
    aggregator_column = AggregatorColumn()
    columns_already_traversed = []
    if aggregator_column.load(column_uuid):
        result = aggregator_column.calculate_aggregation(identifiers=identifiers, forced=True, columns_already_traversed=columns_already_traversed)
    return json.dumps(result)
    
@bp.route('/<table_uuid>/columns/<column_uuid>/aggregator/check_formula', methods=['POST'])
@login_required
def check_formula_expression(table_uuid, column_uuid):
    body = request.form.get('body', '')
    expr = BeautifulSoup(body, 'html.parser').get_text()
    expr = utils.clean_exprtk_expression(expr)
    result = utils.check_exprtk_expression(expr)
    return json.dumps(result)

@bp.route('/<table_uuid>/columns/<column_uuid>/change_history/auth_users', methods=['GET'])
@login_required
def get_unique_auth_users(table_uuid, column_uuid):
    column = Column()
    if column.load(column_uuid):
        if column.is_user_authorised():
            auth_users = change_history.get_distinct_auth_users(column_uuids=[column_uuid])
            return json.dumps(auth_users)
        else:
            abort(403)
    else:
        abort(404)

@bp.route('/<table_uuid>/columns/<column_uuid>/data/apply_to_others_retrospective', methods=['GET'])
@login_required
def apply_to_others_retrospective(table_uuid, column_uuid):
    column = Column()
    if column.load(column_uuid):
        if column.is_user_authorised():
            
            source_username = request.args.get('source_username')
            
            ret = {
                'success': False,
                'other_column_uuid': None,
                'processed_user_target_pairs': {}
            }
            
            # get apply_to_others config
            other_column_uuid = column.config['apply_to_others']['other_columnuuid']
            ret['other_column_uuid'] = other_column_uuid
            if other_column_uuid == '' or other_column_uuid is None:
                return json.dumps(ret)
            
            # get current change history
            ch = change_history.get_change_history(
                column_uuids=[column_uuid],
                return_cursor=True,
                auth_users=[source_username]
            )
            
            # set up objects
            cached_student_data_by_identifier = {}
            
            # iterate through change_history
            processed_user_target_pairs = {}
            for record in ch:
                if not record.get('auth_user') or not record.get('identifier'):
                    continue
                user_target_pair = (record['auth_user'], record['identifier'])
                if user_target_pair in processed_user_target_pairs.keys():
                    continue
                # load student_data
                if record['identifier'] not in cached_student_data_by_identifier.keys():
                    cached_student_data_by_identifier[record['identifier']] = StudentData(column.table)
                    if not cached_student_data_by_identifier[record['identifier']].find_student(record['identifier']):
                        continue
                _student_data = cached_student_data_by_identifier[record['identifier']]
                # restore
                apply_to_others_result = _student_data.apply_to_others(
                    original_column=column, 
                    other_column_uuid=other_column_uuid, 
                    notify_by_email=False,
                    #notify_by_email=True if column.config['notify_email']['active'] == 'true' else False,
                    threaded_aggregation=True,
                    data_override=record.get('new_value', ''),
                    auth_user_override=record['auth_user'],
                    assume_authorised_for_column=True
                )
                # add the user target pair to the list of processed pairs
                processed_user_target_pairs[user_target_pair] = apply_to_others_result
            ret['success'] = True
            ret['processed_user_target_pairs'] = { f"{k[0]} -> {k[1]}":v  for k, v in processed_user_target_pairs.items() }
            return json.dumps(ret, default=str)
        else:
            abort(403)
    else:
        abort(404)
    

