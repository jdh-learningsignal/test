from flask import Blueprint, current_app, session, request, abort, render_template, flash, redirect, url_for, Markup
import json
from datetime import datetime
import logging

from sres.auth import login_required, is_user_administrator, get_auth_user
from sres import utils
from sres.access_logs import add_access_event
from sres.users import User

bp = Blueprint('me', __name__, url_prefix='/me')

@bp.route('/', methods=['GET'])
@login_required
def view_my_account():
    add_access_event(asset_type='me', action='view')
    vars = {}
    user = User()
    if user.find_user(username=get_auth_user(), add_if_not_exists=False):
        vars['user_config'] = user.config
        vars['api_keys'] = [ 
            { 
                'uuid': uuid,
                'key': k['key'][0:5] + '*****',
                'created': k['created'],
                'description': k.get('description', '')
            } 
            for uuid, k in user.get_api_keys().items() 
        ]
        return render_template('me.html', vars=vars)
    else:
        flash(f"Error, could not load user {get_auth_user()}.", "danger")
        return render_template('denied.html')
       
@bp.route('/api/keys', methods=['POST', 'DELETE'])
@login_required
def my_api_keys():
    add_access_event(asset_type='me_api_keys', action='view')
    user = User()
    if user.find_user(username=get_auth_user(), add_if_not_exists=False):
        if request.method == 'DELETE':
            # delete according to provided key uuid
            uuid = request.form.get('uuid')
            if user.delete_api_key(uuid):
                return json.dumps({
                    'success': True,
                    'uuid': uuid
                })
            else:
                abort(400)
        elif request.method == 'POST':
            # make a new one and return
            new_k = user.make_api_key(description=request.form.get('description', ''))
            if new_k:
                return json.dumps({
                    'uuid': new_k['uuid'],
                    'key': new_k['key'],
                    'description': new_k['description']
                })
            else:
                abort(400)
    else:
        abort(404)