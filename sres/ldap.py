import ldap
import logging
from flask import current_app

def _load_config():
    try:
        return current_app.config['SRES'].get('AUTHENTICATION', {}).get('CONFIG', {}).get('LDAP', {})
    except:
        from sres.config import _get_config
        try:
            config = _get_config()
            return config.SRES.get('AUTHENTICATION', {}).get('CONFIG', {}).get('LDAP', {})
        except:
            return {}

def authenticate(username, password):
    """
        Authenticates username to LDAP directory using supplied credential.
        
        username (string)
        password (string)
    """
    ret = {
        'authenticated': False,
        'user_details': {}
    }
    LDAP_CONFIG = _load_config()
    try:
        username = _escape(username)
        #password = _escape(password)
        l = ldap.initialize(LDAP_CONFIG.get('SERVER', None))
        l.simple_bind_s(
            who=LDAP_CONFIG.get('USERNAME_PREFIX', '') + username + LDAP_CONFIG.get('USERNAME_SUFFIX', ''),
            cred=password
        )
        ldap_user_details = find_user_by_username(username)
        if ldap_user_details:
            ret['user_details'] = ldap_user_details[0]
        ret['authenticated'] = True
        return ret
    except Exception as e:
        logging.error('ldap.authenticate failed for [{}]'.format(username))
        logging.exception(e)
        return ret

def find_user_by_username(username):
    """
        Searches the LDAP directory by specified username (string) and 
        returns user_details (list of dicts).
    """
    LDAP_CONFIG = _load_config()
    username = _escape(username)
    filter = '({username_attribute}={username})'.format(
        username_attribute=LDAP_CONFIG.get('USERNAME_ATTRIBUTE'),
        username=username
    )
    return _find_ldap_user(filter=filter)

def find_user(term):
    """
        Searches the LDAP directory using term (string).
        Returns list of dicts.
    """
    LDAP_CONFIG = _load_config()
    term = _escape(term)
    l = ldap.initialize(LDAP_CONFIG.get('SERVER', None))
    l.simple_bind_s(
        who=LDAP_CONFIG.get('BIND_USERNAME'),
        cred=LDAP_CONFIG.get('BIND_PASSWORD')
    )
    if ' ' in term:
        fn = term.split(' ')[0]
        sn = term.split(' ')[-1]
        if 'FILTER_PATTERN_SIMPLE_NAME' in LDAP_CONFIG.keys():
            filter = LDAP_CONFIG['FILTER_PATTERN_SIMPLE_NAME']
        else:
            filter = "(|(&({displayName}=$fn$*)({sn}=$sn$*))({cn}=$term$*))".format(
                displayName=LDAP_CONFIG['DISPLAYNAME_ATTRIBUTE'],
                sn=LDAP_CONFIG['SURNAME_ATTRIBUTE'],
                cn=LDAP_CONFIG['USERNAME_ATTRIBUTE']
            )
        filter = filter.replace('$fn$', fn).replace('$sn$', sn).replace('$term$', term)
    else:
        if 'LDAP_FILTER_PATTERN_DEFAULT' in LDAP_CONFIG.keys():
            filter = LDAP_CONFIG['LDAP_FILTER_PATTERN_DEFAULT']
        else:
            filter = "(|({displayName}=$term$*)({cn}=$term$*))".format(
                displayName=LDAP_CONFIG['DISPLAYNAME_ATTRIBUTE'],
                cn=LDAP_CONFIG['USERNAME_ATTRIBUTE']
            )
        filter = filter.replace('$term$', term)
    results = _find_ldap_user(filter)
    return results

def _find_ldap_user(filter, attributes=None, scope='ONELEVEL'):
    """
        Searches the LDAP directory for user(s).
        
        filter (string) LDAP filter string
        attributes (list of strings) Defaults to that set by config.
        scope (string) ONELEVEL|BASE|SUBTREE

        Returns list of dicts with keys username, given_names, surname, email, identifier, display_name
    """
    LDAP_CONFIG = _load_config()
    if not attributes:
        attributes = LDAP_CONFIG.get('SEARCH_REQUEST_ATTRIBUTES').split(',')
    l = ldap.initialize(LDAP_CONFIG.get('SERVER', None))
    l.simple_bind_s(
        who=LDAP_CONFIG.get('BIND_USERNAME'),
        cred=LDAP_CONFIG.get('BIND_PASSWORD')
    )
    results = l.search_s(
        LDAP_CONFIG.get('DN_BASE'),
        getattr(ldap, 'SCOPE_' + scope.upper()),
        filterstr=filter,
        attrlist=attributes
    )
    ret = []
    if results:
        for result in results:
            ret.append({
                'username': result[1][LDAP_CONFIG.get('USERNAME_ATTRIBUTE')][0].decode(),
                'given_names': result[1][LDAP_CONFIG.get('GIVENNAME_ATTRIBUTE')][0].decode(),
                'surname': result[1][LDAP_CONFIG.get('SURNAME_ATTRIBUTE')][0].decode(),
                'email': result[1][LDAP_CONFIG.get('EMAIL_ATTRIBUTE')][0].decode(),
                'identifier': result[1][LDAP_CONFIG.get('IDENTIFIER_ATTRIBUTE')][0].decode() if LDAP_CONFIG.get('IDENTIFIER_ATTRIBUTE') in result[1].keys() else '',
                'display_name': result[1][LDAP_CONFIG.get('DISPLAYNAME_ATTRIBUTE')][0].decode()
            })
    return ret

def _escape(s):
    from ldap import filter
    return filter.escape_filter_chars(s)
