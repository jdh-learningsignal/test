import os
import logging

from flask import Blueprint, url_for, current_app, request, render_template, redirect, session, make_response

from urllib.parse import urlparse

try:
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    from onelogin.saml2.utils import OneLogin_Saml2_Utils
except ImportError:
    logging.error('Could not import python3-saml package. This package must be installed to use SRES as a SAML SP.')

from sres.auth import log_in_user, log_out, get_auth_user

def init_saml_auth(req):
    auth = OneLogin_Saml2_Auth(req, custom_base_path=current_app.config['SRES']['AUTHENTICATION']['CONFIG']['SAML2']['SAML_PATH'])
    return auth

def prepare_flask_request(request):
    # If server is behind proxys or balancers use the HTTP_X_FORWARDED fields
    url_data = urlparse(request.url)
    return {
        'https': 'on' if request.scheme == 'https' else 'off',
        'http_host': request.host,
        'server_port': url_data.port,
        'script_name': request.path,
        'get_data': request.args.copy(),
        'lowercase_urlencoding': True, # Uncomment if using ADFS as IdP, https://github.com/onelogin/python-saml/pull/144
        'post_data': request.form.copy()
    }

bp = Blueprint("saml", __name__, url_prefix="/saml")

@bp.route('/', methods=['GET', 'POST'])
def index():
    req = prepare_flask_request(request)
    auth = init_saml_auth(req)
    errors = []
    next_url = request.args.get('next')
    not_auth_warn = False
    success_slo = False
    attributes = False
    paint_logout = False
    
    if 'sso' in request.args:
        sso_url = auth.login(return_to=next_url if next_url else None)
        vars = {
            'next_url': next_url if next_url else url_for('index.index', _external=True),
            'sso_url': sso_url
        }
        return redirect(auth.login(
            return_to=next_url if next_url else None#,
            #force_authn=True if session.get('force_reauthentication', False) is True else False
            #, is_passive=True
            #, force_authn=False
        ))
    #elif 'sso2' in request.args:
    #    return_to = '%sattrs/' % request.host_url
    #    return redirect(auth.login(return_to))
    elif 'slo' in request.args:
        name_id = None
        session_index = None
        if 'samlNameId' in session:
            name_id = session['samlNameId']
        if 'samlSessionIndex' in session:
            session_index = session['samlSessionIndex']
        log_out()
        #session['force_reauthentication'] = True
        return redirect(auth.logout(name_id=name_id, session_index=session_index))
    elif 'acs' in request.args:
        auth.process_response()
        errors = auth.get_errors()
        not_auth_warn = not auth.is_authenticated()
        if len(errors) == 0:
            session['samlUserdata'] = auth.get_attributes()
            session['samlNameId'] = auth.get_nameid()
            session['samlSessionIndex'] = auth.get_session_index()
            self_url = OneLogin_Saml2_Utils.get_self_url(req)
            _user_details = {}
            _user_data = auth.get_attributes()
            #logging.debug('_user_data')
            #logging.debug(str(_user_data))
            #logging.debug(auth.get_nameid())
            for _key, _attribute_name in current_app.config['SRES']['AUTHENTICATION']['CONFIG']['SAML2']['ATTRIBUTE_NAMES'].items():
                if _attribute_name in _user_data.keys():
                    try:
                        _user_details[_key] = _user_data[_attribute_name][0]
                    except:
                        logging.exception(f'Error in getting attribute {_attribute_name} key {_key} for nameid {auth.get_nameid()}')
                        _user_details[_key] = ''
            _user_details['username'] = auth.get_nameid()
            if log_in_user(
                username=_user_details['username'],
                password=None,
                user_details=_user_details,
                saml2_authenticated=True
            ):
                # login success!
                # clear force_reauthentication session flag
                if 'force_reauthentication' in session.keys():
                    session.pop('force_reauthentication')
                # send user places
                if 'RelayState' in request.form and self_url != request.form['RelayState']:
                    try:
                        return redirect(auth.redirect_to(request.form['RelayState']))
                    except:
                        return redirect(url_for('index.index', _external=True))
                else:
                    return redirect(url_for('index.index', _external=True))
            else:
                return redirect(url_for('login.login'))
    elif 'sls' in request.args:
        dscb = lambda: session.clear()
        url = auth.process_slo(delete_session_cb=dscb)
        errors = auth.get_errors()
        if len(errors) == 0:
            if url is not None:
                return redirect(url)
            else:
                success_slo = True
    
    if 'samlUserdata' in session:
        paint_logout = True
        if len(session['samlUserdata']) > 0:
            attributes = session['samlUserdata'].items()
    
    return redirect(url_for('index.index', _external=True))

#@bp.route('/attrs/')
#def attrs():
#    paint_logout = False
#    attributes = False
#
#    if 'samlUserdata' in session:
#        paint_logout = True
#        if len(session['samlUserdata']) > 0:
#            attributes = session['samlUserdata'].items()
#
#    return render_template('attrs.html', paint_logout=paint_logout,
#                           attributes=attributes)

@bp.route('/metadata.xml')
def metadata():
    req = prepare_flask_request(request)
    auth = init_saml_auth(req)
    settings = auth.get_settings()
    metadata = settings.get_sp_metadata()
    errors = settings.validate_metadata(metadata)

    if len(errors) == 0:
        resp = make_response(metadata, 200)
        resp.headers['Content-Type'] = 'text/xml'
    else:
        resp = make_response(', '.join(errors), 500)
    return resp
