import functools
from flask import g, session, redirect, url_for, request, current_app
from hashlib import sha512
import os
import logging

from sres.users import User, _users_length
from sres.ldap import authenticate as ldap_authenticate, find_user_by_username
from sres.db import _get_db

def log_in_user(username, password, additional_text_to_log='', user_details={}, saml2_authenticated=False):
    """
        Attempts to authenticate user to log in.
        
        username (string)
        password (string)
    """
    username = username.lower()
    logging.info("User attempting login [{}] [{}]".format(username, additional_text_to_log))
    if password is None and username:
        pass
    elif len(password) == 0:
        return False
    for auth_method in current_app.config['SRES'].get('AUTHENTICATION', {}).get('ENABLED_METHODS', []):
        if auth_method == 'SAML2' and saml2_authenticated == True:
            authenticated = True
            break
        elif auth_method == 'LDAP':
            result = ldap_authenticate(username, password)
            authenticated = result['authenticated']
            user_details = result['user_details']
            if authenticated:
                break
        elif auth_method == 'FALLBACK':
            result = authenticate_using_fallback(username, password)
            authenticated = result['authenticated']
            user_details = result['user_details']
            if authenticated:
                break
    if authenticated:
        logging.info("User successfully logged in [{}]".format(username))
        return log_in_session(username, password, user_details)
    elif current_app.config['SRES'].get('ALLOW_FIRST_USER_AUTO_REGO', False) and _users_length() == 0:
        # special case first ever user, add as superadmin if config allows
        logging.critical("Adding first user as superadministrator [{}]".format(username))
        return log_in_session(
            username, 
            password, 
            user_details={
                'username': username,
                'permissions': ['_super']
            }
        )
    logging.warning("User failed to log in [{}]".format(username))
    return False

def log_in_session(username, password=None, user_details={}):
    session['username'] = username
    user = User()
    if user.find_user(username=username, strict_find=True, add_if_not_exists=True):
        session['user_oid'] = user._id
        session['global_admin_categories'] = user.config['permissions']
        # save user details to db if exists
        if user_details:
            for k, v in user_details.items():
                user.config[k] = v
            session['user_config'] = {
                'email': user.config.get('email'),
                'username': user.config.get('username'),
                'given_names': user.config.get('given_names'),
                'surname': user.config.get('surname'),
                'sid': user.config.get('sid')
            }
            # save fallback if appropriate
            if password is not None and 'FALLBACK' in current_app.config['SRES'].get('AUTHENTICATION', {}).get('ENABLED_METHODS', []):
                remember_fallback_login(username, password, user)
            # update db
            user.update()
        # make display name
        session['display_name'] = '{} {}'.format(user.config['given_names'], user.config['surname'])
        return True
    else:
        # something has gone wrong!
        return False

def log_out():
    logging.info("Logging out [{}]".format(session.get('username', '')))
    #session.clear()
    for k in ['username', 'user_oid', 'global_admin_categories', 'display_name', '_nonces', '_csrf_token', 'user_config']:
        try:
            session.pop(k)
        except:
            pass
    return True

def authenticate_using_fallback(username, password):
    user = User()
    ret = {
        'authenticated': False,
        'user_details': {}
    }
    if user.find_user(username=username, strict_find=True, add_if_not_exists=False):
        salted_password = '{}{}'.format(password, user.config['salt'])
        m = sha512(bytes(salted_password, 'utf-8'))
        if m.hexdigest().lower() == user.config['hashed_password'].lower():
            user_details = {
                'username': user.config['username'],
                'given_names': user.config['given_names'],
                'surname': user.config['surname'],
                'email': user.config['email'],
                'display_name': '{} {}'.format(user.config['given_names'], user.config['surname'])
            }
            ret['authenticated'] = True
            ret['user_details'] = user_details
    return ret

def remember_fallback_login(username, password, user):
    """
        Saves hashed password to supplied user (User) instance for fallback login 
        e.g. if authentication server is down. Does *not* commit to db.users.
        
        username (string)
        password (string)
        user (User)
    """
    salt = sha512(os.urandom(16)).hexdigest().upper()
    salted_password = '{}{}'.format(password, salt)
    hashed_password = sha512(bytes(salted_password, 'utf-8')).hexdigest().upper()
    user.config['salt'] = salt
    user.config['hashed_password'] = hashed_password
    return True

def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if not is_logged_in():
            #next = url_for(request.endpoint, **request.view_args)
            next = request.url
            return redirect(url_for('login.login', next=next, _external=True))
        return view(**kwargs)
    return wrapped_view
        
def is_logged_in():
    if session.get('username', '') != '':
        return True
    else:
        return False

def get_auth_user():
    """Returns the username string of the currently logged in user"""
    u = session.get('username', '')
    if u != '':
        return u
    else:
        return None

def get_auth_user_oid(username=None):
    """Returns the _id (ObjectId) of the currently logged in user"""
    if session.get('user_oid', None):
        return session.get('user_oid')
    user = User()
    if username is None:
        username = get_auth_user()
    if user.find_user(username=username):
        return user._id
    else:
        return None    

def is_user_administrator(category, username=None, user_oid=None):
    """
        Returns (boolean) whether currently logged in user is a global admin of specified category
        category (string) list|filter|super
    """
    # try get from session
    try:
        if session.get('global_admin_categories') and (username is None or (username and username == get_auth_user())):
            return '_{}'.format(category) in session.get('global_admin_categories')
    except:
        pass
    # otherwise long-winded approach
    user = User()
    try:
        if username is None and user_oid is None:
            username = get_auth_user()
        if username:
            if user.find_user(username=username):
                if '_{}'.format(category) in user.config['permissions']:
                    return True
        if user_oid is not None:
            if user.find_user(add_if_not_exists=False, oid=user_oid):
                if '_{}'.format(category) in user.config['permissions']:
                    return True
    except:
        pass
    return False

def is_user_asset_administrator_anywhere(asset_type=None, username=None):
    """
        Looks in each asset type (e.g. filter, portal, insight, list) and determines if
        the user is an administrator. Returns boolean.
        
        asset_type (str singular) list|filter|portal|insight
    """
    db = _get_db()
    if not username:
        user_oid = get_auth_user_oid()
    admin_roles = {}
    if asset_type == 'list' or asset_type is None:
        admin_roles['list'] = len(list(db.tables.find({'staff.administrators': user_oid}, ['_id'])))
    if asset_type == 'filter' or asset_type is None:
        admin_roles['filter'] = len(list(db.filters.find({'administrators': user_oid}, ['_id'])))
    if asset_type == 'portal' or asset_type is None:
        admin_roles['portal'] = len(list(db.portals.find({'administrators': user_oid}, ['_id'])))
    if asset_type == 'insight' or asset_type is None:
        admin_roles['insight'] = len(list(db.insights.find({'administrators': user_oid}, ['_id'])))
    if asset_type is not None and asset_type in admin_roles.keys():
        return admin_roles[asset_type] > 0
    else:
        total_admin_count = 0
        for role, count in admin_roles.items():
            total_admin_count += count
        return total_admin_count > 0

def is_user_list_auditor_anywhere():
    """Checks to see if user is an auditor in any list. Returns boolean."""
    user_oid = get_auth_user_oid()
    db = _get_db()
    if len(list(db.tables.find({'staff.auditors': user_oid}))):
        return True
    return False

