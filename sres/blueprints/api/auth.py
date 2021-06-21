import logging
import re
from flask_wtf import csrf

from sres.users import get_user_by_api_key
from sres.auth import is_logged_in, get_auth_user, get_auth_user_oid
from sres import utils

def check_authentication(request):
    
    ret = {
        'authenticated': False,
        'auth_username': None,
        'auth_user_oid': None
    }
    
    token = request.headers.get('sres-api-token', '')
    if token:
        # check API key
        token = re.sub("[^A-Z0-9a-z ]", "", token)
        if token:
            user = get_user_by_api_key(token)
            if user is not None and user.config['username']:
                ret['authenticated'] = True
                ret['auth_username'] = user.config['username']
                ret['auth_user_oid'] = user._id
            else:
                # bad
                logging.warning(f"Could not identify user with token {token}")
        else:
            # bad
            logging.warning("Cleaned sres-api-token is blank.")
    else:
        # e.g. logged in through standard browser session
        # check csrf token
        try:
            csrf.validate_csrf(request.headers.get('x-csrftoken'))
            # check session login
            ret['authenticated'] = is_logged_in()
            if ret['authenticated']:
                ret['auth_username'] = get_auth_user()
                ret['auth_user_oid'] = get_auth_user_oid()
            else:
                # bad
                logging.warning("Could not log in user at api.auth")
        except Exception as e:
            # bad
            logging.warning("Unexpected error logging in user at api.auth")
            logging.error(e)
    
    return ret

