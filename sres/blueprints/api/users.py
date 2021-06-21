from flask import request, abort, Blueprint, jsonify, url_for
import re
import json
import logging
from datetime import datetime
from dateutil import parser

from sres.blueprints.api.shared import api_abort
from sres.blueprints.api.auth import check_authentication
from sres.auth import is_user_administrator
from sres.users import oids_to_usernames, usernames_to_oids, User, search_users

bp = Blueprint('api_users', __name__, url_prefix='/api/v1')
    
@bp.route('/users', methods=['GET', 'PUT'])
def cru_user(override_method=None, get_username=None):
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # must be superadministrator
    if not is_user_administrator('super', username=auth['auth_username']):
        abort(403)
    
    if override_method:
        request_method = override_method
    else:
        request_method = request.method
    
    if request_method == 'GET':
        username = request.args.get('username') or get_username
        email = request.args.get('email')
        # must have some sort of request parameter
        if username is None and email is None:
            abort(400)
        user = User()
        # try locate user
        if user.find_user(username=username, email=email, strict_find=True, add_if_not_exists=False):
            ret = {
                'username': user.config.get('username', ''),
                'email': user.config.get('email', ''),
                'given_names': user.config.get('given_names', ''),
                'surname': user.config.get('surname', ''),
            }
            # return
            resp = jsonify(ret)
            resp.status_code = 200
            return resp
        else:
            return api_abort(404, 'Sorry, could not find user.')
    elif request_method == 'PUT':
        req = request.get_json(force=True)
        # request must be list/array
        if type(req) is not list:
            return api_abort(400, 'Request must be in the form of an array.')
        # iterate through request array
        ret = []
        for user_info in req:
            user = User()
            if 'username' not in user_info.keys():
                continue
            if user.find_user(username=user_info.get('username'), email=user_info.get('email'), add_if_not_exists=True, strict_find=True):
                for k in [ 'email', 'given_names', 'surname' ]:
                    if k in user_info.keys():
                        user.config[k] = user_info.get(k, '')
                user.update()
                ret.append({
                    'username': user.config.get('username', ''),
                    'email': user.config.get('email', ''),
                    'given_names': user.config.get('given_names', ''),
                    'surname': user.config.get('surname', ''),
                })
        # return
        resp = jsonify(ret)
        resp.status_code = 200
        return resp
    abort(400)
    
@bp.route('/users/find', methods=['GET'])
def find_user():
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    # must be superadministrator
    if not is_user_administrator('super', username=auth['auth_username']):
        abort(403)
    
    if request.method == 'GET':
        
        search_term = request.args.get('search_term')
        
        # must have some sort of request parameter
        if search_term is None:
            return api_abort(400, 'Search term is missing.')
        
        # search by search_term
        found_users = search_users(search_term)
        ret = []
        for found_user in found_users:
            ret.append({
                'username': found_user.get('username', ''),
                'email': found_user.get('email', ''),
                'given_names': found_user.get('given_names', ''),
                'surname': found_user.get('surname', '')
            })
        # return
        resp = jsonify(ret)
        resp.status_code = 200
        return resp

