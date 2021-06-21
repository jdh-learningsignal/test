from flask import Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app, abort, Response, make_response
from flask_wtf.csrf import CSRFProtect
from datetime import datetime
from urllib import parse
from copy import deepcopy
from pylti.flask import lti
import logging
import json
import collections

from sres.auth import log_in_user, log_out, is_logged_in, login_required
from sres.portals import list_authorised_portals, Portal
from sres.columns import Column
from sres.auth import login_required, is_user_asset_administrator_anywhere, is_user_administrator, get_auth_user, get_auth_user_oid, log_out, log_in_session, remember_fallback_login
from sres.db import _get_db
from sres.tables import list_authorised_tables
from sres import utils

bp = Blueprint('lti', __name__, url_prefix='/lti')

PORTAL_EMBED_SIZES = {
    "full_width": {
        "label": "Full width and full height",
        "width": "100vw",
        "height": "95vh"
    },
    "full_width_half_height": {
        "label": "Full width and half height",
        "width": "100vw",
        "height": "50vh"
    },
    "large": {
        "label": "Large (1280 x 720 pixels)",
        "width": "1280",
        "height": "720"
    },
    "medium": {
        "label": "Medium (640 x 320 pixels)",
        "width": "640",
        "height": "320"
    },
    "small" : {
        "label": "Small (320 x 320 pixels)",
        "width": "320",
        "height": "320"
    }
}

def launch_learner(lti_uuid=None, consumer_resource_link_id=None, _lti=None):
    """Gets the current oauth identifier and renders the corresponding portal if user is a learner.
    If a portal does not exist, displays error.
    
    lti_uuid (str)
    consumer_resource_link_id (str)
    _lti (pylti.flask.LTI instance)
    """
    lti = LTI()
    lti_loaded = False
    # load up the lti link
    if lti_uuid is not None:
        lti_loaded = lti.load(lti_uuid=lti_uuid)
    if consumer_resource_link_id is not None:
        lti_loaded = lti.load(consumer_resource_link_id=consumer_resource_link_id)
    # how did we go>
    if not lti_loaded:
        logging.info("Asset is non-existent")
        flash("Sorry, an asset has not been linked with this course yet.", "danger")
        return render_template('lti/flash.html', title='Error')
    # determine asset type and uuid
    portal_uuid = lti.getConfig('portal_uuid')
    column_uuid = lti.getConfig('column_uuid')
    asset_type = lti.getConfig('asset_type')
    # return
    if (asset_type == 'column' or asset_type == 'column_sda') and column_uuid:
        column = Column()
        if column.load(column_uuid):
            column_entry_mode = lti.getConfig('column_entry_mode')
            column_show_header = lti.getConfig('column_show_header')
            column_show_quickinfo = lti.getConfig('column_show_quickinfo')
            sdak = None
            if asset_type == 'column_sda':
                sdak = column.get_student_direct_access_key()
            if column_entry_mode == 'single':
                return redirect(url_for(
                    'entry.add_value',
                    table_uuid=column.table.config['uuid'],
                    column_uuid=column.config['uuid'],
                    sdak=sdak,
                    show_nav=0 if column_show_header == 'hide' else 1,
                    show_qi=0 if column_show_quickinfo == 'hide' else 1,
                    _scheme='https',
                    _external=True
                ))
            elif column_entry_mode == 'roll':
                return redirect(url_for(
                    'entry.add_value_roll',
                    table_uuid=column.table.config['uuid'],
                    column_uuid=column.config['uuid'],
                    sdak=sdak,
                    show_nav=0 if column_show_header == 'hide' else 1,
                    show_qi=0 if column_show_quickinfo == 'hide' else 1,
                    _scheme='https',
                    _external=True
                ))
            else:
                return return_error("Sorry, we couldn't load this column, it appears to be misconfigured.")
        else:
            return return_error("Sorry, we couldn't load this column.")
    elif asset_type == 'portal' or portal_uuid:
        portal = Portal()
        if portal.load(portal_uuid):
            # determine view mode if student or instructor
            if _lti is not None and _lti.is_role(_lti, 'instructor') and portal.is_user_authorised():
                return redirect(url_for('portal.view_portal', preview=1, portal_uuid=portal_uuid, _scheme='https', _external=True))
            else:
                return redirect(url_for('portal.view_portal', portal_uuid=portal_uuid, _scheme='https', _external=True))
        else:
            return return_error("Sorry, we couldn't load this portal.")
    return return_error("Sorry, we couldn't load this resource. It may be misconfigured.")

def launch_instructor():
    return render_template(
        'lti/launch.html',
        display_name=session.get('display_name', 'Unknown user'),
        username=session.get('username', '')
    )

def return_error(msg):
    flash(msg, "danger")
    return render_template('lti/flash.html', title='Error')

def check_auth(username):
    """This might be deleted later, I'm not sure if it's worth checking user permissions on the SRES end for an LTI use case.
    """
    db = _get_db()
    user = db.users.find_one({'username': username}, ['permissions'])
    if user is None:
        return False 
    try:
        perms = user['permissions']
        if len(perms) == 0:
            return False
        for perm in perms:
            if perm == "_super":
                return True 
    except:
        return False
    return False

def error(exception=None):
    error_uuid = utils.create_uuid()
    logging.error(f"PyLTI error {error_uuid}: {exception}")
    return return_error(f"There was an unexpected error accessing this page. Please check that you are authorised for access (you need to be an enrolled student to access a resource, and an enrolled instructor to set up a resource). Error ID {error_uuid}.")

def auth_user(username, user_details=None):
    """Authorises a user based on their LMS credentials through oAuth. 
    If a user is currently logged in, it will first log out that user and then try authenticate the LMS user.
    """
    logging.info("Logging in " + username)
    result = log_in_session(username, user_details=user_details)
    logging.info("Successfully logged in " + username)
    return result

@bp.route("/launch", methods=["GET", "POST"])
@lti(error=error, request='initial', role='any', app=current_app)
def launch(lti):
    """Launch endpoint from an LMS. 
    Logs in user. If a launch uuid has been provided, renders the corresponding view.
    Otherwise, it will store necessary session variables and render a view dependent on user roles.
    """
    
    tool_consumer_info_product_family_code = request.form.get('tool_consumer_info_product_family_code')
    launch_presentation_return_url = request.form.get('launch_presentation_return_url')
    consumer_resource_link_id = request.form.get('resource_link_id', None)
    
    # check if referring domain is whitelisted
    referring_hostname = utils.get_referrer_hostname(request)
    if referring_hostname not in current_app.config['SRES']['LTI']['WHITELISTED_DOMAINS']:
        logging.error("LTI launch request not from whitelisted domain: {}".format(referring_hostname))
        return return_error("Security and configuration error. Please access this from an allowed domain.")
    
    logging.info("LTI launched successfully")
    
    # authenticate user
    username = request.form.get(current_app.config['SRES']['LTI']['LAUNCH_PARAMS_MAPPING']['USERNAME'])
    if username:
        user_details = {}
        for k in ['username', 'given_names', 'surname', 'email', 'display_name']:
            if request.form.get(current_app.config['SRES']['LTI']['LAUNCH_PARAMS_MAPPING'][k.upper()]):
                user_details[k] = request.form.get(current_app.config['SRES']['LTI']['LAUNCH_PARAMS_MAPPING'][k.upper()])
        auth_res = auth_user(username, user_details=user_details)
    else:
        logging.error('Username not found')
        abort(400)
    if auth_res is False:
        logging.info("User was not able to be authenticated: " +  username)
        abort(401)
    
    # determine whether there is enough configuration to launch as a student
    if tool_consumer_info_product_family_code == 'canvas':
        lti_uuid = request.form.get('custom_launch_uuid', None)
        if lti_uuid is not None:
            return launch_learner(lti_uuid=lti_uuid, _lti=lti)
    if tool_consumer_info_product_family_code == 'moodle':
        _lti = LTI()
        _lti.load(consumer_resource_link_id=consumer_resource_link_id)
        if _lti.getConfig('portal_uuid') or _lti.getConfig('column_uuid'):
            return launch_learner(lti_uuid=_lti.getConfig('uuid'), _lti=lti)
    
    # otherwise, launch the target selector as staff
    # Store all request variables in a session for subsequent use
    session['lti_authoriser'] = request.form.get(current_app.config['SRES']['LTI']['LAUNCH_PARAMS_MAPPING']['USERNAME'])
    session['lti_course_id'] = request.form.get(current_app.config['SRES']['LTI']['CONSUMER_PARAMS']['COURSE_ID'])
    session['lti_domain'] = referring_hostname
    session['lti_tool_consumer_info_product_family_code'] = tool_consumer_info_product_family_code
    session['lti_launch_presentation_return_url'] = launch_presentation_return_url
    session['lti_consumer_resource_link_id'] = consumer_resource_link_id
    return launch_instructor()

@bp.route("/find_asset/<asset_type>", methods=["GET"])
@lti(error=error, request='session', role='instructor', app=current_app)
def find_asset(asset_type, lti):
    """Endpoint which renders a page for user to choose an asset for deployment
    :param lti: Contains LTI variables passed from LMS

    """
    if asset_type not in ['portal', 'column', 'column_sda']:
        return return_error("Asset not allowed")
    vars = {
        'asset_type': asset_type,
        'PORTAL_EMBED_SIZES': PORTAL_EMBED_SIZES
    }
    if asset_type == 'column' or asset_type == 'column_sda':
        vars['authorised_tables'] = list_authorised_tables()
    return render_template('lti/components/portals_and_columns.html', vars=vars)

@bp.route("/deploy_lti/<asset_type>/<asset_uuid>/size/<embed_size>", methods=['GET'])
@lti(error=error, request='session', role='instructor', app=current_app)
def deploy_lti(asset_type, asset_uuid, embed_size='full_width', lti=None):
    """Creates an association between a unique oauth_timestamp and a specified SRES asset.
    :param asset_type: What kind of asset to associate - portal, column.
    :param asset_uuid: UUID of the SRES asset to associate.
    :param embed_size: A str key of PORTAL_EMBED_SIZES.
    :param lti: Contains LTI variables passed from LMS
    """
    
    lti_instance = LTI()
    lti_uuid = utils.create_uuid()
    
    course_id = session.get('lti_course_id')
    domain = session.get('lti_domain')
    
    column_entry_mode = request.args.get('column_entry_mode', None)
    if column_entry_mode not in ['single', 'roll']:
        column_entry_mode = None
    
    config = {
        'uuid': lti_uuid,
        'asset_type': asset_type,
        'authoriser': session['lti_authoriser'],
        'consumer_resource_link_id': session.get('resource_link_id'),
        'tool_consumer_info_product_family_code': session.get('lti_tool_consumer_info_product_family_code'),
        'domain': domain,
        'course_id': course_id
    }
    if asset_type == 'portal':
        config['portal_uuid'] = asset_uuid
    elif asset_type in ['column', 'column_sda']:
        config['column_uuid'] = asset_uuid
        config['column_entry_mode'] = column_entry_mode
        config['column_show_header'] = request.args.get('column_show_header', None)
        config['column_show_quickinfo'] = request.args.get('column_show_quickinfo', None)
    
    lti_instance.setConfig(config)
    lti_instance.create()
    
    if session.get('lti_tool_consumer_info_product_family_code') == 'canvas':
        redirect_url = f"%2Fcourses%2F{course_id}%2Fexternal_tools%2Fretrieve%3Fdisplay%3Dborderless%26url%3Dhttps%3A%2F%2F{current_app.config['SERVER_NAME']}%2Flti%2Flaunch%3Fcustom_launch_uuid%3D{lti_uuid}"
        #_redirect_url = f"/courses/{course_id}/external_tools/retrieve?display=borderless&url&https://{current_app.config['SERVER_NAME']}/lti/launch?custom_launch_uuid={lti_uuid}"
        #redirect_url = parse.quote(_redirect_url)
        lms_url = (f"https://{domain}/courses/{course_id}/external_content/success/external_tool_dialog?return_type=iframe"
            f"&width={PORTAL_EMBED_SIZES[embed_size]['width']}"
            f"&height={PORTAL_EMBED_SIZES[embed_size]['height']}"
            f"&url={redirect_url}"
        )
        return redirect(lms_url)
    elif session.get('lti_tool_consumer_info_product_family_code') == 'moodle':
        flash("Successfully configured. Please refresh this page to load this activity.", "success")
        return render_template('lti/flash.html', title='Success')

# LTI XML Configuration
@bp.route("/xml/", methods=['GET'])
def xml():
    """
    Returns the lti.xml file for the app.
    XML can be built at https://www.eduappcenter.com/
    """
    try:
        return Response(render_template(
                'lti/launch.xml',
                server_name=current_app.config['SERVER_NAME'],
                title=current_app.config['SRES']['LTI']['TITLE'],
                title_insert=current_app.config['SRES']['LTI']['TITLE_INSERT'],
                launch_url=url_for('lti.launch', _scheme='https', _external=True)
            ), mimetype='application/xml'
        )
    except:
        logging.error("Error with XML.")
        return return_error('''Error with XML. Please refresh and try again. If this error persists,
            please contact support.''')

class LTI:
    default_config = {
        'uuid': None,
        'asset_type': None,
        'portal_uuid': None,
        'column_uuid': None,
        'column_entry_mode': None,
        'column_show_header': None,
        'column_show_quickinfo': None,
        'date_created': datetime.now(),
        'authoriser': '',
        'is_active': True,
        'consumer_resource_link_id': None,
        'tool_consumer_info_product_family_code': None,
        'domain': None,
        'course_id': None
    }

    def __init__(self):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
        self.is_collective_asset = False
    
    def setConfig(self, ltiDict):
        for key, val in ltiDict.items():
            self.config[key] = val
    
    def getConfig(self, key):
        return self.config[key]
    
    def load(self, lti_uuid=None, consumer_resource_link_id=None):
        db_filter = {}
        if lti_uuid is not None:
            db_filter['uuid'] = utils.clean_uuid(lti_uuid)
        if consumer_resource_link_id is not None:
            db_filter['consumer_resource_link_id'] = consumer_resource_link_id
        results = list(self.db.lti.find(db_filter))
        if len(results) == 1:
            return self._load(results[0])
        else:
            return False
    
    def _load(self, db_result):
        self._id = db_result['_id']
        for key, value in self.default_config.items():
            try:
                if isinstance(self.config[key], collections.Mapping):
                    # is dict-type so try and merge
                    self.config[key] = {**value, **db_result[key]}
                else:
                    self.config[key] = db_result[key]
            except:
                self.config[key] = value
        return True

    def create(self):
        result = self.db.lti.insert_one(self.config)
        logging.info("Inserted into db")
        if result.acknowledged and self.load(self.config['uuid']):
            return self.config['uuid']
        else:
            logging.error("It didn't work", result)
            return None
    