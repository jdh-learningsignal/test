from flask import request, abort, Blueprint, jsonify
import re
import json
import logging
from datetime import datetime
from dateutil import parser

from sres.blueprints.api.auth import check_authentication
from sres.auth import is_logged_in, get_auth_user, is_user_administrator
from sres.tables import Table, list_authorised_tables
from sres.columns import Column
from sres.users import oids_to_usernames, usernames_to_oids
from sres.studentdata import StudentData, STUDENT_DATA_DEFAULT_CONFIG, REQUIRED_BASE_FIELDS

bp = Blueprint('api_tables', __name__, url_prefix='/api/v1')

@bp.route('/tables/<table_uuid>', methods=['DELETE', 'GET', 'PUT'])
def rud_table(table_uuid, override_method=None):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    table = Table()
    if not table.load(table_uuid):
        abort(400)
    if not table.is_user_authorised(user_oid=auth['auth_user_oid']):
        abort(403)
    
    if override_method:
        request_method = override_method
    else:
        request_method = request.method
    
    if request_method == 'DELETE':
        if table.delete(override_username=auth['auth_username']):
            resp = jsonify({
                'success': True,
                'name': table.get_full_name()
            })
            resp.status_code = 200
            return resp
        else:
            abort(400)
    elif request_method == 'GET':
        resp = jsonify({
            'uuid': table.config['uuid'],
            'code': table.config['code'],
            'name': table.config['name'],
            'year': table.config['year'],
            'semester': table.config['semester'],
            'workflow_state': table.config['workflow_state'],
            'contact.name': table.config['contact']['name'],
            'contact.email': table.config['contact']['email'],
            'staff.administrators': list(oids_to_usernames(table.config['staff']['administrators']).values()),
            'staff.users': list(oids_to_usernames(table.config['staff']['users']).values()),
            'staff.auditors': list(oids_to_usernames(table.config['staff']['auditors']).values())
        })
        resp.status_code = 200
        return resp
    elif request_method == 'PUT':
        # update
        config = {}
        for key in [ 'code', 'name', 'year', 'semester', 'workflow_state', 'contact.name', 'contact.email', 'staff.administrators', 'staff.users', 'staff.auditors' ]:
            config[key] = request.form.get(key, None)
        if _update_table(config, table, auth['auth_username']):
            return rud_table(table_uuid, override_method='GET')
        else:
            abort(400)
    abort(400)
    
@bp.route('/tables', methods=['POST', 'GET'])
def create_table():
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # check permissions
    if is_user_administrator('list', username=auth['auth_username']) or is_user_administrator('super', username=auth['auth_username']):
        if request.method == 'GET':
            authorised_tables = list_authorised_tables(
                show_archived=True,
                filter_years=request.args.getlist('years'),
                filter_semesters=request.args.getlist('semesters'),
                override_user_oid=auth['auth_user_oid']
            )
            tables = []
            for authorised_table in authorised_tables:
                _table = {}
                for k in [ 'uuid', 'code', 'name', 'year', 'semester', 'workflow_state' ]:
                    _table[k] = authorised_table.get(k, '')
                _table['contact.name'] = authorised_table.get('contact', {}).get('name', '')
                _table['contact.email'] = authorised_table.get('contact', {}).get('email', '')
                _table['staff.administrators'] = list( oids_to_usernames( authorised_table.get('staff', {}).get('administrators', '') ).values() )
                _table['staff.users'] = list( oids_to_usernames( authorised_table.get('staff', {}).get('users', '') ).values() )
                _table['staff.auditors'] = list( oids_to_usernames( authorised_table.get('staff', {}).get('auditors', '') ).values() )
                tables.append(_table)
            resp = jsonify(tables)
            resp.status_code = 200
            return resp
        elif request.method == 'POST':
            new_table = Table()
            new_uuid = new_table.create(override_username=auth['auth_username'])
            if new_uuid:
                try:
                    config = {
                        'code': request.form['code'],
                        'name': request.form['name'],
                        'year': request.form['year'],
                        'semester': request.form['semester'],
                        'contact.name': request.form['contact.name'],
                        'contact.email': request.form['contact.email'],
                        'staff.administrators': request.form.get('staff.administrators', '[]'),
                        'staff.users': request.form.get('staff.users', '[]'),
                        'staff.auditors': request.form.get('staff.auditors', '[]'),
                        'workflow_state': 'active'
                    }
                except:
                    abort(400)
                if _update_table(config, new_table, auth['auth_username'], mode='new'):
                    return rud_table(new_uuid, override_method='GET')
                else:
                    abort(400)
            else:
                abort(400)
    else:
        abort(403)
    abort(400)

def _parse_unknown_list(unknown_list):
    if type(unknown_list) is list:
        return unknown_list
    elif type(unknown_list) is str:
        try:
            return json.loads(unknown_list)
        except:
            logging.error(f'Could not parse unknown_list {unknown_list}')
            return []

def _update_table(config, table, auth_username, mode='update'):
    
    for key in [ 'code', 'name', 'year', 'semester', 'workflow_state' ]:
        if config[key] is not None:
            table.config[key] = config[key]
    
    if config['contact.name'] is not None:
        table.config['contact']['name'] = config['contact.name']
    if config['contact.email'] is not None:
        table.config['contact']['email'] = config['contact.email']
    if config['staff.administrators'] is not None:
        table.config['staff']['administrators'] = usernames_to_oids(_parse_unknown_list(config['staff.administrators']))
    if config['staff.users'] is not None:
        table.config['staff']['users'] = usernames_to_oids(_parse_unknown_list(config['staff.users']))
    if config['staff.auditors'] is not None:
        table.config['staff']['auditors'] = usernames_to_oids(_parse_unknown_list(config['staff.auditors']))
    
    if table.update(override_username=auth_username):
        return table
    else:
        return None

@bp.route('/tables/<table_uuid>/students', methods=['DELETE', 'GET', 'PUT', 'POST'])
def crud_students(table_uuid, override_method=None):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    table = Table()
    if not table.load(table_uuid):
        abort(400)
    if not table.is_user_authorised(user_oid=auth['auth_user_oid']):
        abort(403)
    
    if override_method:
        request_method = override_method
    else:
        request_method = request.method
    
    if request_method == 'DELETE':
        
        identifiers = request.args.getlist('identifiers')
        student_data = StudentData(table)
        
        ret = []
        
        for identifier in identifiers:
            student_data._reset()
            if student_data.find_student(identifier):
                student_data.config['status'] = 'inactive'
                student_data.save()
        
        return crud_students(table_uuid, override_method='GET')
        
    elif request_method == 'GET':
        
        identifiers = request.args.getlist('identifiers')
        student_data = StudentData(table)
        
        ret = []
        
        if len(identifiers) == 0:
            # if no identifiers specified, get all students
            only_active = True if request.args.get('only_active') == 'true' else False
            students = table.load_all_students(only_active=only_active, get_email=True)
            for student in students:
                _student = {}
                for key in STUDENT_DATA_DEFAULT_CONFIG.keys():
                    if student.get(key) is not None:
                        _student[key] = student[key]
                ret.append(_student)
        else:
            # get specified students
            for identifier in identifiers:
                student_data._reset()
                if student_data.find_student(identifier):
                    _student = {}
                    for key in STUDENT_DATA_DEFAULT_CONFIG.keys():
                        if student_data.config.get(key) is not None:
                            _student[key] = student_data.config[key]
                    ret.append(_student)
                else:
                    # hmmmm not found
                    pass # ...
        
        resp = jsonify(ret)
        resp.status_code = 200
        return resp
        
    elif request_method == 'POST' or request_method == 'PUT':
        
        students = request.get_json(force=True)
        
        _students = []
        for student in students:
            if request_method == 'POST':
                # check if required data is present if adding students
                required_base_field_missing = False
                for field in REQUIRED_BASE_FIELDS:
                    if field not in student.keys():
                        required_base_field_missing = True
                        break
                if required_base_field_missing:
                    # skip this student
                    continue
            # repackage data
            _student = {}
            for key in STUDENT_DATA_DEFAULT_CONFIG.keys():
                _student[key] = student.get(key)
            _students.append(_student)
        
        remove_if_not_present = True if request.args.get('remove_if_not_present') == 'true' else False
        res = table._update_enrollments(
            _students,
            { k: { 'field': k } for k in STUDENT_DATA_DEFAULT_CONFIG.keys() },
            remove_not_present=remove_if_not_present
        )
        
        resp = jsonify(res)
        resp.status_code = 200
        return resp

    abort(400)

@bp.route('/tables/<table_uuid>/data', methods=['GET', 'POST'])
def cru_data(table_uuid, override_method=None):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    table = Table()
    if not table.load(table_uuid):
        abort(400)
    
    if override_method:
        request_method = override_method
    else:
        request_method = request.method
    
    if request_method == 'GET':
        
        if not table.is_user_authorised(categories=['administrator', 'user', 'auditor'], user_oid=auth['auth_user_oid']):
            abort(403)
        
        column_uuids = request.args.getlist('column_uuids')
        identifiers = request.args.getlist('identifiers')
        student_data = StudentData(table)
        
        # preload columns
        preloaded_columns = {}
        for column_uuid in column_uuids:
            column = Column()
            if column.load(column_uuid):
                if column.is_user_authorised(username=auth['auth_username'], authorised_roles=['administrator', 'user', 'auditor']):
                    preloaded_columns[ column.config['uuid'] ] = column
        
        ret = []
        
        if len(identifiers) == 0:
            identifiers = table.get_all_students_sids()
        
        for identifier in identifiers:
            
            student_data._reset()
            _student = {
                'identifier': identifier
            }
            if student_data.find_student(identifier):
                _student['sid'] = student_data.config['sid']
                _student['email'] = student_data.config['email']
                _data = {}
                for column_uuid, column in preloaded_columns.items():
                    res = student_data.get_data(
                        column_uuid=column_uuid,
                        preloaded_column=preloaded_columns[column_uuid],
                        do_not_deserialise=True
                    )
                    _data[column_uuid] = res['data']
                _student['data'] = _data
            else:
                logging.warning(f'Could not find student {identifier}')
            ret.append(_student)
        
        resp = jsonify(ret)
        resp.status_code = 200
        return resp
        
    elif request_method == 'POST':
        
        if not table.is_user_authorised(categories=['administrator', 'user'], user_oid=auth['auth_user_oid']):
            abort(403)
        
        # expecting list of dicts. Each dict has key 'identifier' and 'data'.
        # Key 'data' is a dict itself, keyed by column_uuid and values being the data.
        
        #records = _parse_unknown_list(request.form.get('data'))
        records = request.get_json(force=True)
        
        student_data = StudentData(table)
        
        # preload the columns
        column_uuids = []
        for record in records:
            for column_uuid, data in record.get('data', {}).items():
                if column_uuid not in column_uuids:
                    column_uuids.append(column_uuid)
        preloaded_columns = {}
        for column_uuid in column_uuids:
            column = Column()
            if column.load(column_uuid):
                if column.is_user_authorised(username=auth['auth_username'], authorised_roles=['administrator', 'user']):
                    preloaded_columns[ column.config['uuid'] ] = column
        
        ret = []
        
        allowed_column_uuids = list(preloaded_columns.keys())
        for record in records:
            student_data._reset()
            identifier = record.get('identifier')
            data = record.get('data')
            if identifier and data:
                _ret = {
                    'identifier': identifier,
                    'results': {}
                }
                if student_data.find_student(identifier):
                    for column_uuid, _data in data.items():
                        _ret['results'][column_uuid] = {}
                        if column_uuid in allowed_column_uuids:
                            res = student_data.set_data(
                                column_uuid=column_uuid,
                                data=_data,
                                auth_user_override=auth['auth_username'],
                                commit_immediately=True,
                                preloaded_column=preloaded_columns[column_uuid],
                                preloaded_columns=preloaded_columns
                            )
                            if res['success']:
                                _ret['results'][column_uuid]['status_code'] = 200
                            else:
                                _ret['results'][column_uuid]['status_code'] = 400
                        else:
                            _ret['results'][column_uuid]['status_code'] = 403
                else:
                    _ret['status'] = 404
                ret.append(_ret)
                
        resp = jsonify(ret)
        resp.status_code = 200
        return resp
