from flask import Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app, abort
from flask_wtf.csrf import CSRFProtect

from sres.auth import log_in_user, log_out, is_logged_in, login_required, get_auth_user

bp = Blueprint("login", __name__, url_prefix="/login")

@bp.route("/", methods=["GET", "POST"])
def login():
    _enabled_auth_methods = current_app.config['SRES']['AUTHENTICATION']['ENABLED_METHODS']
    vars = {
        'saml2_enabled': 'SAML2' in _enabled_auth_methods,
        'saml2_url': url_for('saml.index', sso='', next=request.args.get('next',''), _external=True)
    }
    if request.method == "GET":
        # if user is already logged in then redirect to index
        if is_logged_in() == True:
            if request.args.get('headless') == 'login':
                # headless login, just display yay
                flash('Logged in successfully. You can close this tab now and resume your work.', 'success')
                vars['title'] = 'Logged in'
                vars['propagate_csrf_token_to_opener'] = True
                vars['show_close_button'] = True
                return render_template("login-headless.html", vars=vars)
            return redirect(url_for('index.index'))
        # if the only auth method is saml2 then go directly to saml2 authentication
        if len(_enabled_auth_methods) == 1 and 'SAML2' in _enabled_auth_methods:
            return redirect(url_for('saml.index', sso='', next=request.args.get('next',''), _external=True))
        # otherwise go with standard approaches
        if 'LDAP' in _enabled_auth_methods or 'FALLBACK' in _enabled_auth_methods:
            return render_template("login.html", vars=vars)
    elif request.method == "POST":
        next_url = request.args.get("next")
        result = log_in_user(request.form["loginUsername"], request.form["loginPassword"], str(next_url))
        if result:
            if next_url is None:
                if request.args.get('headless') == 'login':
                    return redirect(url_for('login.login', headless='login'))
                return redirect(url_for('index.index'))
            else:
                return redirect(next_url)   
        else:
            flash("Please check your username and password and try again.", "danger")
            return render_template("login.html", vars=vars)

@bp.route("/logout", methods=["GET"])
def logout():
    for auth_method in current_app.config['SRES']['AUTHENTICATION']['ENABLED_METHODS']:
        if auth_method == 'SAML2':
            return redirect(url_for('saml.index', slo='', _external=True))
        elif auth_method == 'LDAP' or auth_method == 'FALLBACK':
            result = log_out()
            return redirect(url_for('login.login', next=request.args.get('next')))

@bp.route("/ping", methods=['GET'])
def ping():
    auth_user = get_auth_user()
    if auth_user is None:
        abort(401)
    else:
        if auth_user:
            return auth_user
        else:
            abort(401)
    
