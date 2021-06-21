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
from sres.filters import Filter, HIDEABLE_UI_ELEMENTS
from sres.users import oids_to_usernames, usernames_to_oids
from sres.blueprints.api.shared import parse_unknown_list, api_abort

bp = Blueprint('api_filters', __name__, url_prefix='/api/v1')

@bp.route('/filters/<filter_uuid>/edit/url', methods=['GET'])
def get_edit_filter_url(filter_uuid):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    ui_hide = request.args.get('h', '')
    ui_hide_long = request.args.getlist('hide')
    
    ui_hide_keys = []
    for el in HIDEABLE_UI_ELEMENTS:
        if el[0] in ui_hide or el[1] in ui_hide_long:
            ui_hide_keys.append(el[0])
    
    resp = jsonify({
        'url': url_for('filter.edit_filter', filter_uuid=filter_uuid, uih=''.join(ui_hide_keys), _external=True)
    })
    resp.status_code = 200
    return resp
    
@bp.route('/filters/<filter_uuid>', methods=['DELETE', 'GET', 'PUT', 'POST'])
def crud_filter(filter_uuid, override_method=None):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    if override_method:
        request_method = override_method
    else:
        request_method = request.method
    
    filter = Filter()
    
    if request_method == 'POST':
        req = request.get_json(force=True)
        new_filter_uuid = filter.create(override_user_oid=auth['auth_user_oid'])
        if new_filter_uuid:
            req['uuid'] = new_filter_uuid
            if _update_filter(req, filter, auth['auth_username'], mode='update'):
                return crud_filter(new_filter_uuid, override_method='GET')
            else:
                abort(500)
        else:
            abort(500)
    
    # load and check permissions for existing filter
    if not filter.load(filter_uuid):
        abort(400)
    if not filter.is_user_authorised(override_user_oid=auth['auth_user_oid']):
        abort(403)
    
    if request_method == 'DELETE':
        if filter.delete(override_username=auth['auth_username']):
            resp = jsonify({
                'success': True,
                'name': filter.config['name']
            })
            resp.status_code = 200
            return resp
        else:
            abort(500)
    elif request_method == 'GET':
        ret = {
            'uuid': filter.config['uuid'],
            'name': filter.config['name'],
            'description': filter.config['description'],
            'workflow_state': filter.config['workflow_state']
        }
        ret['email'] = filter.config['email']
        ret['conditions'] = filter.config['conditions']
        ret['administrators'] = list(oids_to_usernames(filter.config['administrators']).values())
        resp = jsonify(ret)
        resp.status_code = 200
        return resp
    elif request_method == 'PUT':
        req = request.get_json(force=True)
        if _update_filter(req, filter, auth['auth_username'], mode='update'):
            return crud_filter(filter_uuid, override_method='GET')
        else:
            abort(500)
    abort(400)
    
def _update_filter(req, filter, auth_username, mode='update'):
    
    for key in [ 'name', 'description', 'workflow_state' ]:
        if key in req:
            filter.config[key] = req[key]
    
    if 'administrators' in req:
        filter.config['administrators'] = usernames_to_oids(req['administrators'])
    
    if 'conditions' in req and type(req['conditions']) is dict:
        filter.config['conditions'] = req['conditions']
    
    if 'email' in req and type(req['email']) is dict:
        
        for key in [ ('addresses', dict), ('feedback', dict), ('sender', dict), ('body_first', str), ('body_last', str), ('subject', str), ('sections', list) ]:
            k = key[0]
            t = key[1]
            if k in req['email'] and type(req['email'][k]) is t:
                filter.config['email'][k] = req['email'][k]
    
    if filter.update(override_username=auth_username):
        return filter
    else:
        return None

@bp.route('/filters/<filter_uuid>/targets', methods=['GET'])
def filter_targets(filter_uuid):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    filter = Filter()
    if not filter.load(filter_uuid):
        abort(400)
    if not filter.is_user_authorised(override_user_oid=auth['auth_user_oid']):
        abort(403)
    
    filter_results = filter.run_conditions()
    identifiers = [ v['sid'] for k, v in filter_results['data'].items() ]
    
    resp = jsonify({
        'identifiers': identifiers
    })
    resp.status_code = 200
    return resp
    
@bp.route('/filters/<filter_uuid>/messages', methods=['GET', 'POST'])
def filter_messages(filter_uuid):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # load and check permissions
    filter = Filter()
    if not filter.load(filter_uuid):
        abort(400)
    if not filter.is_user_authorised(override_user_oid=auth['auth_user_oid']):
        abort(403)
    
    if request.method == 'GET':
        identifiers = request.args.getlist('identifiers')
        if len(identifiers) == 0:
            return api_abort(400, 'Identifiers must be specified.')
        results = filter.get_personalised_message(identifiers=identifiers, mode='preview')
        resp = jsonify(results)
        resp.status_code = 200
        return resp
    elif request.method == 'POST':
        # check if already sent
        if len(filter.config['run_history']) > 0:
            return api_abort(409, 'Filter has already been run.')
        # get target identifiers
        identifiers = request.form.getlist('identifiers')
        if len(identifiers) == 0:
            return api_abort(400, 'Identifiers must be specified.')
        # request queue
        results = filter.queue_send(identifiers=identifiers, auth_username=auth['auth_username'])
        # return
        resp = jsonify(results)
        resp.status_code = 200
        return resp
        


