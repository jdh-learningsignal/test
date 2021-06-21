from flask import Blueprint, current_app, session, request, render_template, flash, redirect, url_for, Markup, abort
import json
from datetime import datetime

from sres.auth import login_required, is_user_administrator
from sres.ldap import find_user as ldap_find_user
from sres.users import search_users as db_search_users, get_admins, ADMIN_CATEGORIES, change_admins, get_admins_aggregate
from sres import utils
from sres.studentdata import StudentData
from sres.tables import Table
from sres.columns import Column

bp = Blueprint('user', __name__, url_prefix='/users')

@bp.route('/search', methods=['GET'])
@login_required
def search_users():
    if is_user_administrator('super') or is_user_administrator('list') or is_user_administrator('filter'):
        term = request.args.get('term', None)
        ret = []
        if term:
            # search db.users
            db_users = db_search_users(term)
            #db_users = []
            # search ldap
            ldap_users = ldap_find_user(term)
            #print(term, ldap_users, db_users)
            # combine
            users = {}
            for user in db_users + ldap_users:
                if user['username'] not in users.keys():
                    users[user['username']] = user
            for username, user in users.items():
                ret.append({
                    'value': user[current_app.config['SRES']['AUTHENTICATION']['SEARCH_RESULTS']['SEARCH_RESULT_USE_FIELD']],
                    'display': "{} ({})".format(
                        user[current_app.config['SRES']['AUTHENTICATION']['SEARCH_RESULTS']['SEARCH_RESULT_DISPLAY_FIELD']],
                        user[current_app.config['SRES']['AUTHENTICATION']['SEARCH_RESULTS']['SEARCH_RESULT_USE_FIELD']]
                    )
                })
        return json.dumps({
            'results': ret
        })
    else:
        abort(403)

