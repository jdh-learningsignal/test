from flask import Flask, current_app, redirect, url_for, request, session, flash, render_template, Markup, escape, Response
from flask_babel import Babel, gettext
from flask_session import Session
from flask_mail import Mail
from flask_talisman import Talisman
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
import json
from flask_wtf.csrf import CSRFProtect
import logging
import logging.handlers as handlers
import random
import string
import re
from bs4 import BeautifulSoup
from werkzeug.http import dump_cookie

from sres.tables import list_authorised_tables
from sres.db import _get_db
from sres.files import get_file_access_url
from sres import utils
from sres.auth import get_auth_user, is_user_administrator
from sres.anonymiser import anonymise_identifier, anonymise, is_identity_anonymiser_active
from sres.go import make_go_url

from sres.blueprints import table
from sres.blueprints import index
from sres.blueprints import login
#from sres.blueprints import vis
from sres.blueprints import filter
from sres.blueprints import file
from sres.blueprints import column
from sres.blueprints import entry
from sres.blueprints import tracking
from sres.blueprints import portal
from sres.blueprints import user
from sres.blueprints import admin
from sres.blueprints import lti
from sres.blueprints import insight
from sres.blueprints import saml
from sres.blueprints import collective
from sres.blueprints import tags
from sres.blueprints import tag_groups
from sres.blueprints import connect
from sres.blueprints import summary
from sres.blueprints import me
from sres.blueprints import go
from sres.blueprints.api import columns as api_columns
from sres.blueprints.api import tables as api_tables
from sres.blueprints.api import collective as api_collective
from sres.blueprints.api import filters as api_filters
from sres.blueprints.api import users as api_users

class ContextFilter(logging.Filter):
    def filter(self, record):
        record.ip = ''
        try:
            if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
                record.ip = request.environ['REMOTE_ADDR'] if request.environ['REMOTE_ADDR'] is not None else ''
            else:
                record.ip = request.environ['HTTP_X_FORWARDED_FOR']
        except:
            record.ip = ''
        return True

class LoggerFilter(logging.Filter):
    def __init__(self, logger_name, name='', exclude=False):
        super(LoggerFilter, self).__init__(name)
        self.logger_name = logger_name
        self.exclude = exclude
    def filter(self, record):
        if record.name:
            if self.logger_name in record.name:
                return not self.exclude
        return self.exclude

def create_app(test_config=None):
    
    app = Flask(__name__, instance_relative_config=True)
    
    if test_config is None:
        app.config.from_pyfile('config.py', silent=False)
    else:
        app.config.from_mapping(test_config)
    
    log_formatter = logging.Formatter('%(asctime)s - %(ip)s - %(name)s - %(levelname)s - %(message)s')
    # general logger
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_log_handler = handlers.RotatingFileHandler(
            app.config['SRES'].get('LOGS', {}).get('ROOT', 'sres.log'),
            maxBytes=1024*1024*20,
            backupCount=500
        )
        root_log_handler.addFilter(ContextFilter())
        root_log_handler.addFilter(LoggerFilter('sres.db.studentdata', exclude=True))
        root_log_handler.setFormatter(log_formatter)
        root_log_handler.setLevel(logging.DEBUG)
        root_logger.addHandler(root_log_handler)
    root_logger.setLevel(logging.DEBUG)
    # data logger
    data_logger = logging.getLogger('sres.db.studentdata')
    if not data_logger.handlers:
        data_log_handler = handlers.RotatingFileHandler(
            app.config['SRES'].get('LOGS', {}).get('DATA', 'sres_data.log'),
            maxBytes=1024*1024*20,
            backupCount=500
        )
        data_log_handler.addFilter(ContextFilter())
        data_log_handler.setFormatter(log_formatter)
        data_log_handler.setLevel(logging.INFO)
        data_logger.addHandler(data_log_handler)
    data_logger.setLevel(logging.INFO)
    # set other logger config
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    logging.getLogger('urllib3.connectionpool').setLevel(logging.INFO)
    logging.info("Starting up...")
    
    # reverse proxy config
    if app.config.get('REVERSE_PROXY_ACTIVE', None):
        app.wsgi_app = ReverseProxied(
            app.wsgi_app, 
            script_name=app.config.get('REVERSE_PROXY_APPLICATION_ROOT', None), 
            scheme=app.config.get('REVERSE_PROXY_SCHEME', None), 
            server=app.config.get('REVERSE_PROXY_SERVER', None)
        )
    
    # session
    app.config['SESSION_TYPE'] = 'mongodb'
    app.config['SESSION_MONGODB'] = _get_db() #PyMongo(app).db #'127.0.0.1'
    app.config['SESSION_MONGODB_DB'] = 'sres'
    app.config['SESSION_MONGODB_COLLECT'] = 'sessions'
    Session(app)
    
    # csrf
    #app.jinja_env.globals['csrf_token'] = utils.generate_csrf_token
    csrf = CSRFProtect(app)
    csrf.exempt(lti.bp)

    app.config['WTF_CSRF_TIME_LIMIT'] = None # tokens valid for life of session
    
    # Talisman
    talisman = Talisman(
        app,
        content_security_policy={
            'default-src': [
                "'self'",
                'blob:'
            ],
            'frame-src': [
                '*'
            ],
            'img-src': [
                '*',
                "'self'",
                'data:'
            ],
            'script-src': [
                "'self'",
                '*.google-analytics.com',
                '*.gstatic.com',
                "'unsafe-inline'",
                "'unsafe-eval'",
                'blob:'
            ],
            'style-src': [
                "'self'",
                "'unsafe-inline'",
                '*.gstatic.com',
                '*.googleapis.com',
            ],
            'font-src': [
                "'self'",
                'data:',
                '*.gstatic.com'
            ],
            'media-src': [
                "'self'",
                'blob:'
            ],
			'connect-src': [
				"'self'",
				'blob:',
                '*.google-analytics.com'
			]
        },
        frame_options=None
    )
    
    # setting cookie samesite=None
    app.config['SESSION_COOKIE_SAMESITE'] = "None"
    # hack because flask-session doesn't respect the SESSION_COOKIE_SAMESITE setting
    # per https://github.com/fengsp/flask-session/issues/111#issuecomment-595779676
    class MyResponse(Response):
        def set_cookie(self, *args, **kwargs):
            cookie = dump_cookie(*args, **kwargs)
            if 'samesite' in kwargs and kwargs['samesite'] is None or 'samesite' not in kwargs:
                cookie = f"{cookie}; SameSite=None;"
            # force Secure if Talisman is not working for some reason
            if 'secure;' not in cookie.lower():
                cookie = f"{cookie}; Secure;"
            self.headers.add(
                'Set-Cookie',
                cookie
            )
    app.response_class = MyResponse
    
    # mail
    app.mail = Mail(app)
    
    # scheduler
    app.scheduler = BackgroundScheduler()
    app.scheduler.configure(jobstores={
        'default': MongoDBJobStore(
            database='sres',
            collection='apscheduler',
            client=_get_db()
        )
    })
    app.scheduler.start()
    
    # Babel
    app.config['BABEL_DEFAULT_LOCALE'] = app.config['SRES'].get('BABEL_DEFAULT_LOCALE', 'en')
    app.babel = Babel(app)
    
    # healthcheck route
    @talisman(force_https=False)
    @app.route('/healthcheck', methods=['GET'])
    def healthcheck():
        return 'OK'
    
    # selected legacy routes
    @app.route('/viewList.cfm', methods=['GET'])
    def legacy_view_table():
        return redirect(url_for('table.view_table', table_uuid=request.args['tableuuid']), 301)
    # sv.cfm
    @app.route('/sv.cfm', methods=['GET'])
    def legacy_view_sv():
        portal_uuid = request.args.get('svuuid')
        return redirect(url_for('portal.view_portal', portal_uuid=portal_uuid), 301)
    # addValue.cfm
    @app.route('/addValue.cfm', methods=['GET'])
    def legacy_add_value():
        table_uuid = request.args.get('tableuuid')
        column_uuid = request.args.get('columnuuid')
        identifier = request.args.get('scancode')
        return redirect(url_for('entry.add_value', table_uuid=table_uuid, column_uuid=column_uuid, identifier=identifier), 301)
    # addValueRoll.cfm
    @app.route('/offline/addValueRoll.cfm', methods=['GET'])
    def legacy_add_value_roll():
        table_uuid = request.args.get('tableuuid')
        column_uuid = request.args.get('columnuuid')
        return redirect(url_for('entry.add_value_roll', table_uuid=table_uuid, column_uuid=column_uuid), 301)
    # t.cfm
    @app.route('/t.cfm', methods=['GET'])
    def legacy_url_tracker():
        l = request.args.get('l')
        u = request.args.get('u')
        return redirect(url_for('tracking.url', l=l, u=u), 301)
    # beacon.png.cfm
    @app.route('/beacon.png.cfm', methods=['GET'])
    def legacy_beacon_png():
        u = request.args.get('u')
        return redirect(url_for('tracking.get_beacon', u=u), 301)
        
    #@app.route('/error', methods=['GET'])
    #def error_route():
    #    u = None
    #    u + 12
    
    app.register_blueprint(table.bp)
    app.register_blueprint(login.bp)
    app.register_blueprint(index.bp)
    #app.register_blueprint(vis.bp)
    app.register_blueprint(filter.bp)
    app.register_blueprint(file.bp)
    app.register_blueprint(column.bp)
    app.register_blueprint(entry.bp)
    app.register_blueprint(tracking.bp)
    app.register_blueprint(portal.bp)
    app.register_blueprint(user.bp)
    app.register_blueprint(admin.bp)
    app.register_blueprint(insight.bp)
    app.register_blueprint(collective.bp)
    app.register_blueprint(tags.bp)
    app.register_blueprint(tag_groups.bp)
    app.register_blueprint(connect.bp)
    app.register_blueprint(summary.bp)
    app.register_blueprint(me.bp)
    app.register_blueprint(go.bp)
    if app.config['SRES'].get('LTI', {}).get('ENABLED', False):
        app.register_blueprint(lti.bp)
    
    app.register_blueprint(api_columns.bp)
    csrf.exempt(api_columns.bp)
    app.register_blueprint(api_tables.bp)
    csrf.exempt(api_tables.bp)
    app.register_blueprint(api_filters.bp)
    csrf.exempt(api_filters.bp)
    app.register_blueprint(api_collective.bp)
    csrf.exempt(api_collective.bp)
    app.register_blueprint(api_users.bp)
    csrf.exempt(api_users.bp)
    
    # Disable Content-Security-Policy for the service worker script
    with app.app_context():
        setattr(
            current_app.view_functions.get('entry.add_value_service_worker'), 
            'talisman_view_options', 
            {'content_security_policy':None}
        )
    
    app.register_blueprint(saml.bp)
    csrf.exempt(saml.bp)
    
    app.jinja_env.filters['datetime'] = filter_datetime
    app.jinja_env.filters['jsondump'] = filter_jsondump
    app.jinja_env.filters['sresfileurl'] = filter_sres_file_access_url
    app.jinja_env.filters['islist'] = filter_is_list
    app.jinja_env.filters['rand'] = filter_random
    app.jinja_env.filters['nltobr'] = filter_nl_to_br
    app.jinja_env.filters['safe_no_script'] = filter_safe_no_script
    app.jinja_env.filters['anonymise_id'] = filter_anonymise_id
    app.jinja_env.filters['anonymise_id_ifneedbe'] = filter_anonymise_id_ifneedbe
    app.jinja_env.filters['anonymise_forename'] = filter_anonymise_forename
    app.jinja_env.filters['anonymise_surname'] = filter_anonymise_surname
    app.jinja_env.filters['anonymise_field'] = filter_anonymise_field
    
    app.jinja_env.globals['get_go_url'] = make_go_url
    app.jinja_env.globals['create_uuid'] = utils.create_uuid
    app.jinja_env.globals['get_sres_user_details'] = _get_sres_user_details
    
    @app.errorhandler(404)
    def error_404(e):
        flash(Markup("<span class=\"fa fa-question-circle\"></span> Sorry, the requested resource could not be found."), "danger")
        return render_template('denied.html')
    
    @app.errorhandler(500)
    def error_500(e):
        flash(Markup("<span class=\"fa fa-exclamation-circle\"></span> Sorry, an unexpected error occured."), "danger")
        uuid = utils.create_uuid()
        logging.error('Exception {}'.format(uuid))
        logging.exception(e)
        flash(Markup("The following information might help a system administrator troubleshoot the issue: error id {} s{}".format(uuid, current_app.config['SRES'].get('SERVER_NUMBER', 0))), "warning")
        return render_template('denied.html')
    
    return app

def _get_sres_user_details():
    _u = {}
    _username = get_auth_user()
    _u['username'] = _username
    for _a in ['list', 'filter', 'super']:
        _u[f'admin-{_a}'] = is_user_administrator(_a, _username)
    return _u

class ReverseProxied(object):
    def __init__(self, app, script_name=None, scheme=None, server=None):
        self.app = app
        self.script_name = script_name
        self.scheme = scheme
        self.server = server
    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_SCRIPT_NAME', '') or self.script_name
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]
        scheme = environ.get('HTTP_X_SCHEME', '') or self.scheme
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        #server = environ.get('HTTP_X_FORWARDED_SERVER', '') or self.server
        server = environ.get('HTTP_X_FORWARDED_HOST', '') or self.server
        if server:
            environ['HTTP_HOST'] = server
        return self.app(environ, start_response)

def filter_datetime(datetime, format='date'):
    from dateutil import parser
    if isinstance(datetime, str):
        try:
            datetime = parser.parse(datetime)
        except:
            return ''
    elif datetime is None:
        return ''
    if format == 'date':
        return datetime.strftime('%Y-%m-%d')
    elif format == 'datetime':
        return datetime.strftime('%Y-%m-%d %H:%M:%S')
    elif format == 'datetime_with_day':
        return datetime.strftime('%a %Y-%m-%d %H:%M:%S')
    elif format == 'hm':
        return datetime.strftime('%H:%M')
    elif format == 'hms':
        return datetime.strftime('%H:%M:%S')

def filter_jsondump(input):
    return json.dumps(input)

def filter_sres_file_access_url(filename):
    return get_file_access_url(filename)

def filter_is_list(input):
    return isinstance(input, list)

def filter_random(sep):
    return sep.join(random.choice(string.ascii_lowercase) for i in range(10))

def filter_nl_to_br(input):
    input = re.sub('\r\n|\r|\n', '\n', input)
    input = '<br>'.join( [ escape(s) for s in input.split('\n') ] )
    return Markup(input)

def filter_safe_no_script(input):
    soup = BeautifulSoup(input, 'html.parser')
    e = soup.find_all('script')
    for i in e:
        i.extract()
    return Markup(str(soup))

def filter_anonymise_id(input):
    return anonymise_identifier(input)
    
def filter_anonymise_id_ifneedbe(input):
    if is_identity_anonymiser_active():
        return anonymise_identifier(input)
    else:
        return input
    
def filter_anonymise_forename(input):
    return anonymise('preferred_name', input)
    
def filter_anonymise_surname(input):
    return anonymise('surname', input)
    
def filter_anonymise_field(input, field_type):
    return anonymise(field_type, input)
    

