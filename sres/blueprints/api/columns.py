from flask import request, abort, Blueprint, jsonify, url_for
import re
import json
import logging
from datetime import datetime
from dateutil import parser

from sres.blueprints.api.auth import check_authentication
from sres.auth import is_logged_in, get_auth_user
from sres.columns import Column
from sres.tables import Table
from sres.go import make_go_url

bp = Blueprint('api_columns', __name__, url_prefix='/api/v1')

@bp.route('/tables/<table_uuid>/columns/<column_uuid>', methods=['DELETE', 'GET', 'PUT'])
def rud_column(table_uuid, column_uuid, override_method=None):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    column = Column()
    if not column.load(column_uuid):
        abort(400)
    if not column.table.is_user_authorised(user_oid=auth['auth_user_oid']):
        abort(403)
    
    if override_method:
        request_method = override_method
    else:
        request_method = request.method
    
    if request_method == 'DELETE':
        if column.delete(override_username=auth['auth_username']):
            resp = jsonify({
                'success': True,
                'name': column.get_friendly_name(show_table_info=False, get_text_only=True)
            })
            resp.status_code = 200
            return resp
        else:
            abort(400)
    elif request_method == 'GET':
        resp = jsonify({
            'uuid': column.config['uuid'],
            'table_uuid': column.config['table_uuid'],
            'type': column.config['type'],
            'name': column.config['name'],
            'description': column.config['description'],
            'workflow_state': column.config['workflow_state'],
            'active_from': _active_config_to_timestamp( column.config['active']['from'], column.config['active']['from_time'] ),
            'active_to': _active_config_to_timestamp( column.config['active']['to'], column.config['active']['to_time'] )
        })
        resp.status_code = 200
        return resp
    elif request_method == 'PUT':
        # update
        config = {}
        for key in [ 'name', 'type', 'description', 'workflow_state', 'active_from', 'active_to' ]:
            config[key] = request.form.get(key, None)
        if _update_column(config, column, auth['auth_username']):
            return rud_column(table_uuid, column.config['uuid'], override_method='GET')
        else:
            abort(400)
    abort(400)

@bp.route('/tables/<table_uuid>/columns/<column_uuid>/clone', methods=['POST'])
def clone_column(table_uuid, column_uuid):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    column = Column()
    if not column.load(column_uuid):
        abort(400)
    if not column.table.is_user_authorised(user_oid=auth['auth_user_oid']):
        abort(403)
    
    target_table_uuid = request.form.get('target_table_uuid')
    
    if target_table_uuid:
        target_table = Table()
        if not target_table.load(target_table_uuid):
            abort(400)
        if not target_table.is_user_authorised(user_oid=auth['auth_user_oid']):
            abort(403)
    else:
        target_table = column.table
    
    cloned_column_uuid = column.clone(target_table_uuid=target_table.config['uuid'], override_username=auth['auth_username'])
    cloned_column = Column()
    if cloned_column_uuid and cloned_column.load(cloned_column_uuid):
        return rud_column(target_table.config['uuid'], cloned_column.config['uuid'], override_method='GET')
    else:
        abort(500)

@bp.route('/tables/<table_uuid>/columns/<column_uuid>/urls', methods=['GET'])
def get_column_urls(table_uuid, column_uuid):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    column = Column()
    if not column.load(column_uuid):
        abort(400)
    if not column.table.is_user_authorised(user_oid=auth['auth_user_oid']):
        abort(403)
    
    urls = []
    
    urls.append({
        'type': 'staff_data_entry_single',
        'url': make_go_url( url_for('entry.add_value', table_uuid=column.table.config['uuid'], column_uuid=column.config['uuid'], _external=True) )
    })
    if not column.has_multiple_report_mode_enabled():
        urls.append({
            'type': 'staff_data_entry_roll',
            'url': make_go_url( url_for('entry.add_value_bulk', table_uuid=column.table.config['uuid'], column_uuid=column.config['uuid'], _external=True) )
        })
        urls.append({
            'type': 'staff_data_entry_bulk',
            'url': make_go_url( url_for('entry.add_value_roll', table_uuid=column.table.config['uuid'], column_uuid=column.config['uuid'], _external=True) )
        })
    
    if column.is_student_direct_access_allowed() and (column.is_self_data_entry_enabled() or column.is_peer_data_entry_enabled()):
        urls.append({
            'type': 'student_data_entry_single',
            'url': make_go_url( url_for('entry.add_value', table_uuid=column.table.config['uuid'], column_uuid=column.config['uuid'], sdak=column.get_student_direct_access_key(), _external=True) )
        })
    
    if column.is_student_direct_access_allowed(mode='roll') and (column.is_self_data_entry_enabled() or column.is_peer_data_entry_enabled()):
        urls.append({
            'type': 'student_data_entry_roll',
            'url': make_go_url( url_for('entry.add_value_roll', table_uuid=column.table.config['uuid'], column_uuid=column.config['uuid'], sdak=column.get_student_direct_access_key(), _external=True) )
        })
    
    resp = jsonify(urls)
    resp.status_code = 200
    return resp
   
@bp.route('/tables/<table_uuid>/columns', methods=['POST', 'GET'])
def create_column(table_uuid):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # check permissions
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised(user_oid=auth['auth_user_oid']):
        if request.method == 'GET':
            all_columns_info = table.get_all_columns_info()
            columns = []
            for column_uuid, column_info in all_columns_info.items():
                _column = {}
                for key in [ 'uuid', 'name', 'type', 'description', 'workflow_state' ]:
                    _column[key] = column_info[key]
                _column['active_from'] = _active_config_to_timestamp( column_info['active']['from'], column_info['active']['from_time'] )
                _column['active_to'] = _active_config_to_timestamp( column_info['active']['to'], column_info['active']['to_time'] )
                columns.append(_column)
            resp = jsonify(columns)
            resp.status_code = 200
            return resp
        elif request.method == 'POST':
            new_column = Column()
            new_uuid = new_column.create(table_uuid=table_uuid, override_username=auth['auth_username'])
            if new_uuid:
                config = {
                    'name': request.form['name'],
                    'type': request.form.get('type', 'mark'),
                    'description': request.form.get('description', None),
                    'active_from': request.form.get('active_from', None),
                    'active_to': request.form.get('active_to', None),
                    'workflow_state': 'active'
                }
                if _update_column(config, new_column, auth['auth_username'], mode='new'):
                    return rud_column(table_uuid, new_uuid, override_method='GET')
                else:
                    abort(400)
            else:
                abort(400)
    else:
        abort(403)
    abort(400)

def _update_column(config, column, auth_username, mode='update'):
    
    for key in [ 'name', 'type', 'description', 'workflow_state' ]:
        if config[key] is not None:
            column.config[key] = config[key]
    
    if config.get('active_from') is not None:
        column.config['active']['from'], column.config['active']['from_time'] = _timestamp_to_active_config(config['active_from'])
    elif config.get('active_from') is None and mode == 'new':
        column.config['active']['from'] = datetime.now()
    
    if config.get('active_to') is not None:
        column.config['active']['to'], column.config['active']['to_time'] = _timestamp_to_active_config(config['active_to'])
    elif config.get('active_to') is None and mode == 'new':
        column.config['active']['to'] = datetime.now()
    
    if column.update(override_username=auth_username):
        return column
    else:
        return None

def _active_config_to_timestamp(active_config_date, active_config_time):
    try:
        return datetime.combine(
            active_config_date.date(),
            datetime.strptime(active_config_time, '%H:%M:%S').time()
        ).strftime('%Y-%m-%d %H:%M:%S')
    except:
        return ''
    
def _timestamp_to_active_config(timestamp):
    try:
        active_config = parser.parse(timestamp)
        return active_config, active_config.strftime('%H:%M:%S')
    except:
        return datetime.now(), datetime.now().strftime('%H:%M:%S')

    
    