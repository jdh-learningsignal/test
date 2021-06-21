from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app, abort, Markup, make_response)
import json
from bs4 import BeautifulSoup
from dateutil import parser
from datetime import datetime
import re
import bleach
import logging
from natsort import natsorted, ns
import random

from sres.users import usernames_to_oids
from sres.auth import login_required, is_user_administrator, get_auth_user, is_logged_in, get_auth_user_oid
from sres.columns import SYSTEM_COLUMNS, Column, MAGIC_FORMATTERS_LIST, get_db_results_for_columns_and_tables
from sres.tables import list_authorised_tables, Table
from sres.portals import Portal, list_authorised_portals, FEEDBACK_STYLES, interpret_portal_availability, AUTHORISED_ROLES
from sres import utils
from sres.studentdata import get_random_identifier_for_table, StudentData, substitute_text_variables, GENERAL_FIELDS, IDENTIFIER_FIELDS
from sres.conditions import Conditions
from sres.logs import add_interaction_event, add_feedback_event
from sres.collective_assets import CollectiveAsset
from sres.access_logs import add_access_event
from sres.anonymiser import anonymise, is_identity_anonymiser_active
from sres.files import get_file_access_url, get_file_access_key
from sres.search import find_students_from_tables_by_term

bp = Blueprint('portal', __name__, url_prefix='/portals')

@bp.route('/new', methods=['GET'])
@login_required
def new_portal():
    add_access_event(asset_type='portal', asset_uuid=None, action='new')
    vars = {}
    vars['mode'] = 'new'
    vars['now'] = datetime.now()
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['GENERAL_FIELDS'] = GENERAL_FIELDS
    vars['MAGIC_FORMATTERS_LIST'] = MAGIC_FORMATTERS_LIST
    vars['FONT_FORMATS'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('FORMATTING', {}).get('FONT_FORMATS', '')
    vars['teachers_limit_by_column'] = None
    vars['viewers_limit_by_column'] = None
    vars['authorised_tables'] = list_authorised_tables()
    vars['query_builder_filters'] = []
    vars['query_builder_rules'] = {}
    if request.args.get('source_table_uuid'):
        vars['referenced_table_uuids'] = [request.args.get('source_table_uuid')]
    else:
        vars['referenced_table_uuids'] = []
    if is_user_administrator('filter') or is_user_administrator('super'):
        portal = Portal()
        portal.config['administrators'] = [get_auth_user_oid()]
        portal.config['uuid'] = '__new__'
        return render_template('portal-edit.html', portal=portal, vars=vars)
    else:
        flash("Sorry, only administrators are authorised to access this feature.", "warning")
        return render_template('denied.html')

@bp.route('/<portal_uuid>/edit', methods=['GET', 'POST'])
@login_required
def edit_portal(portal_uuid, collective_mode=None, collective_asset_uuid=None, collective_vars=None):
    add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='edit')
    vars = {}
    portal = Portal()
    vars['mode'] = 'edit'
    vars['now'] = datetime.now()
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['GENERAL_FIELDS'] = GENERAL_FIELDS
    vars['MAGIC_FORMATTERS_LIST'] = MAGIC_FORMATTERS_LIST
    vars['FONT_FORMATS'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('FORMATTING', {}).get('FONT_FORMATS', '')
    vars['authorised_tables'] = list_authorised_tables()
    vars['lti_enabled'] = current_app.config['SRES'].get('LTI', {}).get('ENABLED', False)
    vars['teachers_limit_by_column'] = None
    vars['viewers_limit_by_column'] = None
    # set some things about the Collective
    if collective_mode:
        vars['collective_sharing'] = True
        vars['collective_sharing_mode'] = collective_mode
        vars['collective_sharing_asset'] = CollectiveAsset()
        vars['collective_vars'] = collective_vars
    else:
        vars['collective_sharing'] = False
    collective_asset_uuid = collective_asset_uuid or request.form.get('collective_asset_uuid', None)
    collective_asset = None
    try:
        if request.method == 'POST' and request.form['action'] == 'new':
            # adding a new portal
            if is_user_administrator('filter') or is_user_administrator('super'):
                pass
            else:
                flash("Sorry, only administrators are authorised to access this feature.", "warning")
                return render_template('denied.html')
        else:
            if collective_asset_uuid is None or collective_asset_uuid == '':
                if not portal.load(portal_uuid):
                    flash("Sorry, this portal could not be loaded.", "danger")
                    return render_template('denied.html')
                if not portal.is_user_authorised():
                    flash("Sorry, you do not appear to have the right permissions to proceed.", "danger")
                    return render_template('denied.html')
                if collective_asset_uuid == '':
                    # adding a new collective asset
                    collective_asset = CollectiveAsset('portal')
                    if collective_asset.create() and collective_asset.set_new_asset(portal):
                        portal = collective_asset.asset
                    else:
                        flash("Sorry, there was a problem making the Collective asset.", "danger")
                        return render_template('denied.html')
            else:
                # collective asset exists
                collective_asset = CollectiveAsset('portal')
                if collective_asset.load(collective_asset_uuid):
                    portal = collective_asset.asset
                    vars['collective_sharing_asset'] = collective_asset
                else:
                    flash("Sorry, there was a problem loading the Collective asset.", "danger")
                    return render_template('denied.html')
    except:
        flash("Sorry, this portal could not be loaded. You may not have the right permissions, or the portal may not exist.", "warning")
        return render_template('denied.html')
    # continue
    if request.method == 'GET':
        add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='view')
        # starting filters for queryBuilder
        vars['query_builder_filters'] = []
        select_array = []
        if portal.is_collective_asset or request.args.get('from_collective_asset_uuid'):
            if request.args.get('from_collective_asset_uuid'):
                collective_asset = CollectiveAsset()
                collective_asset.load(request.args.get('from_collective_asset_uuid'))
            select_array = collective_asset.get_select_array_for_referenced_columns()
            referenced_table_uuids = []
        else:
            referenced_table_uuids = portal.get_referenced_table_uuids()
            for referenced_table_uuid in referenced_table_uuids:
                table_temp = Table()
                if table_temp.load(referenced_table_uuid) and table_temp.is_user_authorised():
                    select_array.extend(table_temp.get_select_array(show_collapsed_multientry_option=True))
        if select_array:
            for c in select_array:
                vars['query_builder_filters'].append({
                    'id': c['value'],
                    'label': c['full_display_text'],
                    'type': 'string'
                })
        vars['referenced_table_uuids'] = referenced_table_uuids
        # limit-by columns
        for r in [ 'teacher', 'viewer' ]:
            if portal.config[f'{r}s_limit_by_columnuuid']:
                vars[f'{r}s_limit_by_column'] = Column()
                vars[f'{r}s_limit_by_column'].load( portal.config[f'{r}s_limit_by_columnuuid'] )
        # render
        return render_template('portal-edit.html', portal=portal, vars=vars)
    elif request.method == 'POST':
        if request.form['action'] == 'new':
            add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='create')
            if not portal.create():
                flash("Problem creating new portal.", "danger")
        elif request.form['action'] == 'edit' and collective_asset_uuid is None and portal._id is None:
            add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='update')
            if portal.load(portal_uuid) and portal.is_user_authorised():
                pass
            else:
                flash("Problem loading portal.", "danger")
        if portal._id:
            # general config
            portal.config['name'] = request.form.get('portal_name')
            portal.config['description'] = request.form.get('portal_description')
            # staff-level access config
            for r in AUTHORISED_ROLES.keys():
                portal.config[f'{r}s'] = usernames_to_oids(request.form.getlist(f'authorised_{r}s'))
            for r in [ 'teacher', 'viewer' ]:
                portal.config['active'][f'available_to_{r}s'] = True if f'portal_active_for_{r}s' in request.form.keys() else False
                portal.config[f'{r}s_limit_by_columnuuid'] = request.form.get(f'authorised_{r}s_limit_by_columnuuid', '')
            # other config
            portal.config['active']['available'] = True if 'view_active' in request.form.keys() else False
            portal.config['active']['from'] = parser.parse(request.form.get('available_from', str(datetime.now())))
            portal.config['active']['to'] = parser.parse(request.form.get('available_to', str(datetime.now())))
            portal.config['require_auth'] = True if 'require_authentication' in request.form.keys() else False
            portal.config['if_student_unknown'] = request.form.get('extra_config_if_student_unknown', 'disallow')
            portal.config['feedback'] = FEEDBACK_STYLES[request.form.get('feedback_request', 'helpfulyesno')]
            portal.config['max_width_px'] = request.form.get('max_width_px', '')
            portal.config['reload_portal_interval'] = request.form.get('reload_portal_interval', '')
            # panels
            if utils.is_json(request.form.get('panels')):
                panels = []
                for panel_number in json.loads(request.form.get('panels')):
                    panel_uuid = request.form.get('panel_uuid-panel-x{}'.format(panel_number), '')
                    if not panel_uuid:
                        panel_uuid = utils.create_uuid()
                    panel = {
                        'uuid': panel_uuid,
                        'show_when': request.form.get('panel_showwhen-panel-x{}'.format(panel_number), 'always'),
                        'mode': request.form.get('panel_mode-panel-x{}'.format(panel_number), 'read'),
                        'content': request.form.get('panel_content-panel-x{}'.format(panel_number), ''),
                        'conditions': {"condition": "AND", "rules": []},
                        'display_order': panel_number,
                        'availability': request.form.get('panel_availability-panel-x{}'.format(panel_number), 'available')
                    }
                    # availability range
                    try:
                        panel['availability_from'] = parser.parse(
                            '{} {}'.format(
                                request.form.get('panel_availability_from_date-panel-x{}'.format(panel_number), ''), 
                                request.form.get('panel_availability_from_time-panel-x{}'.format(panel_number), '')
                            )
                        )
                        panel['availability_to'] = parser.parse(
                            '{} {}'.format(
                                request.form.get('panel_availability_to_date-panel-x{}'.format(panel_number), ''),
                                request.form.get('panel_availability_to_time-panel-x{}'.format(panel_number), '')
                            )
                        )
                    except:
                        panel['availability_from'] = datetime.now()
                        panel['availability_to'] = datetime.now()
                    panel['availability_from_str'] = panel['availability_from'].strftime('%Y-%m-%d %H:%M:%S')
                    panel['availability_to_str'] = panel['availability_to'].strftime('%Y-%m-%d %H:%M:%S')
                    # collapsible
                    panel['collapsible'] = request.form.get(f'panel_collapsible-panel-x{panel_number}', 'disabled')
                    panel['collapsible_default_display'] = request.form.get(f'panel_collapsible_default_display-panel-x{panel_number}', 'show')
                    # panels trigger reload on save
                    panel['trigger_reload_on_save'] = request.form.get(f'panel_trigger_reload_on_save-panel-x{panel_number}', 'disabled')
                    # panel conditions
                    panel_conditions = request.form.get('rules_for_panel_conditions-panel-x{}'.format(panel_number))
                    if panel_conditions and panel_conditions != 'null' and utils.is_json(panel_conditions):
                        panel['conditions'] = json.loads(panel_conditions)
                    # add to panel to panels
                    panels.append(panel)
                portal.config['panels'] = panels
            # housekeeping config
            portal.config['default_table_uuid'] = portal.get_referenced_table_uuids()[0]
            if request.form['action'] == 'new':
                portal.config['created'] = datetime.now()
            if collective_asset_uuid is not None:
                portal.config['workflow_state'] = 'collective'
            else:
                portal.config['workflow_state'] = 'active'
            portal.config['modified'] = datetime.now()
            # save
            if portal.update():
                flash("Portal configuration successfully updated.", "success")
            else:
                flash("Unexpected error updating portal configuration.", "danger")
        # save collective, if applicable
        if collective_asset is not None:
            if collective_asset.set_metadata_from_form(request.form, process_share_referenced_columns=False if collective_asset_uuid else True) and collective_asset.update_referenced_columns_config():
                flash("Collective asset successfully updated.", "success")
            else:
                flash("Unexpected error updating Collective asset.", "warning")
        # render
        if collective_asset is not None:
            return redirect(url_for('collective.show_asset', asset_uuid=collective_asset.config['uuid']))
        else:
            if request.form.get('save_action') == 'save_and_preview':
                return redirect(url_for('portal.view_portal', portal_uuid=portal.config['uuid'], preview=1))
            else:
                return redirect(url_for('portal.edit_portal', portal_uuid=portal.config['uuid']))

@bp.route('/<portal_uuid>/clone', methods=['GET'])
@login_required
def clone_portal(portal_uuid):
    add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='clone')
    portal = Portal()
    cloned_portal = Portal()
    result = None
    if portal.load(portal_uuid) and portal.is_user_authorised():
        result = portal.clone()
        if result and cloned_portal.load(result):
            flash("Cloned successfully. You are now editing the clone.", "success")
        else:
            flash("Error while cloning.", "warning")
    else:
        flash("Insufficient permissions to read this portal.", "danger")
    if result:
        return redirect(url_for('portal.edit_portal', portal_uuid=cloned_portal.config['uuid']))
    else:
        return redirect(url_for('portal.edit_portal', portal_uuid=portal.config['uuid']))
    
@bp.route('/<portal_uuid>/delete', methods=['GET', 'DELETE'])
@login_required
def delete_portal(portal_uuid):
    add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='delete')
    portal = Portal()
    if portal.load(portal_uuid) and portal.is_user_authorised():
        if portal.delete():
            if request.method == 'GET':
                flash("Successfully deleted portal.", "success")
                return redirect(url_for('index.index'))
            elif request.method == 'DELETE':
                return 'OK'
        else:
            if request.method == 'GET':
                flash("Error deleting portal.", "warning")
                return redirect(url_for('portal.edit_filter', portal_uuid=portal.config['uuid']))
            elif request.method == 'DELETE':
                abort(400)
    else:
        if request.method == 'GET':
            flash("Unauthorised.", "danger")
            return render_template('denied.html')
        elif request.method == 'DELETE':
            abort(403)
    
@bp.route('/list', methods=['GET'])
@login_required
def list_portals():
    portals = list_authorised_portals()
    ret = []
    for p in portals:
        try:
            ret.append({
                'uuid': p['uuid'],
                'name': p['name'],
                'description': p['description'],
                'created': p['created'],
                'modified': p.get('modified'),
                'workflow_state': p['workflow_state'],
                'active': p['active'],
                'active_now': interpret_portal_availability(p)['available'], #p['active']['from'].date() <= datetime.now().date() and p['active']['to'].date() >= datetime.now().date() and p['active']['available']
                'active_now_messages': interpret_portal_availability(p)['messages']
            })
        except:
            pass
    return json.dumps(ret, default=str)

@bp.route('/check_advanced_expression', methods=['POST'])
@login_required
def check_advanced_expression():
    body = request.form.get('body', '')
    expr = BeautifulSoup(body, 'html.parser').get_text()
    expr = utils.clean_exprtk_expression(expr)
    result = utils.check_exprtk_expression(expr)
    return json.dumps(result)

@bp.route('/<portal_uuid>/view', methods=['GET'])
def view_portal(portal_uuid):
    add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='view_portal')
    portal = Portal()
    vars = {
        'rangeslider_config': {},
        'days_since_last_feedback_event': 0,
        'required_libraries': []
    }
    # parse args
    previewing = 'preview' in request.args.keys()
    identifier = request.args.get('identifier', None)
    if isinstance(identifier, str):
        identifier = identifier.strip()
    portal_uuid = utils.clean_uuid(portal_uuid)
    auth_user = ''
    logged_in_user_role = None # the highest role of the actually logged-in user
    user_authenticated = False
    erroring_panels = []
    # login checks as necessary since @login_required is not present
    if portal.load(portal_uuid):
        if portal.config['require_auth']:
            # authentication is required
            user_authenticated = False
        else:
            # authentication is not required
            if not identifier:
                flash("There seems to be a problem with the method used to access this portal. A user identifier was expected but not provided.", "warning")
            user_authenticated = True
            auth_user = identifier
            logged_in_user_role = 'anonymous'
        # determine the role of the actually logged-in user
        if not logged_in_user_role:
            logged_in_user_role = portal.get_user_highest_role()
            if logged_in_user_role == 'teacher' or logged_in_user_role == 'viewer':
                previewing = True
                # determine an identifier this user is authorised to access
                restrictor_column_uuid = portal.config.get(f'{logged_in_user_role}s_limit_by_columnuuid')
                if restrictor_column_uuid:
                    found_students = find_students_from_tables_by_term(
                        search_term='',
                        table_uuids=portal.get_referenced_table_uuids(),
                        restrictor_column_uuid=restrictor_column_uuid
                    )
                    if len(found_students) == 0:
                        flash("You are currently not authorised to view any students.", "danger")
                        return render_template('denied.html')
                    identifiers_permitted = set()
                    for found_student in found_students:
                        identifiers_permitted.update( [ found_student.get(f) for f in IDENTIFIER_FIELDS ] )
                    identifiers_permitted.remove(None)
                    if not identifier or identifier not in identifiers_permitted:
                        # don't show the specified identifier, and instead pick a new one
                        record_number = random.randint(0, len(found_students) - 1)
                        identifier = found_students[record_number]['sid']
        # see if we are 'previewing' i.e. viewing as staff
        if previewing:
            if not portal.is_user_authorised(roles=AUTHORISED_ROLES.keys()):
                flash(
                    Markup("You are not authorised to access this student portal. You may need to <a href=\"{}\">log in</a> as an authorised user.".format(
                        url_for('login.login', next=url_for(request.endpoint, **request.view_args))
                    )), 
                    "danger"
                )
                return render_template('denied.html')
            user_authenticated = True
            if not identifier:
                # find a random identifier
                auth_user = get_random_identifier_for_table(portal.config['default_table_uuid'])
                if auth_user is None:
                    flash("Cannot preview; no students found in associated list.", "danger")
                    return render_template('denied.html')
            else:
                auth_user = identifier
        else:
            _seconds_since_last_feedback_event = portal.seconds_since_last_feedback_event()
            vars['days_since_last_feedback_event'] = _seconds_since_last_feedback_event if _seconds_since_last_feedback_event == -1 else _seconds_since_last_feedback_event / 86400
        # check logged in user
        if not user_authenticated:
            if is_logged_in() and get_auth_user():
                user_authenticated = True
                auth_user = get_auth_user()
            else:
                return redirect(url_for('login.login', next=url_for(request.endpoint, **request.view_args)))
        if not logged_in_user_role:
            logged_in_user_role = 'student'
        vars['logged_in_user_role'] = logged_in_user_role
        # show the portal to authorised users
        if portal_uuid and auth_user and user_authenticated and logged_in_user_role:
            logging.info(f"Portal view [{portal_uuid}] [{auth_user}] [{logged_in_user_role}]")
            # see if portal is currently active
            available = portal.is_portal_available()
            if not available['available'] and not portal.is_user_authorised(roles=AUTHORISED_ROLES.keys()):
                flash("Sorry, this portal is unavailable.", "danger")
                for message in available['messages']:
                    flash(message[0], message[1])
                return render_template('denied.html')
            vars['available'] = available
            # see if the portal is available to the user type who is logged in
            if logged_in_user_role == 'teacher' or logged_in_user_role == 'viewer':
                if portal.config['active'].get(f'available_to_{logged_in_user_role}s') is False:
                    flash(f"Sorry, this portal is unavailable to {logged_in_user_role}s.", 'danger')
                    return render_template('denied.html')
            # set up some caching
            portal_column_references = portal.get_referenced_column_references()
            preloaded_db_results = get_db_results_for_columns_and_tables(portal_column_references)
            ## cache the tables
            preloaded_table_instances = {} # keyed by table_uuid
            if preloaded_db_results['table_results']:
                for _table_uuid, _db_result in preloaded_db_results['table_results'].items():
                    preloaded_table_instances[_table_uuid] = Table()
                    if not preloaded_table_instances[_table_uuid].load(
                        table_uuid=_table_uuid,
                        preloaded_db_result=[_db_result]
                    ):
                        preloaded_table_instances.pop(_table_uuid, None)
            ## cache the columns
            preloaded_column_instances = {} # keyed by column reference
            if preloaded_db_results['column_results']:
                for _column_reference, _db_result in preloaded_db_results['column_results'].items():
                    _table_uuid = preloaded_db_results['column_reference_to_table_uuid'].get(_column_reference)
                    _table_instance = preloaded_table_instances.get(_table_uuid)
                    if _table_instance:
                        preloaded_column_instances[_column_reference] = Column(_table_instance)
                        if not preloaded_column_instances[_column_reference].load(
                            column_reference=_column_reference,
                            default_table_uuid=_table_uuid,
                            preloaded_db_result=[_db_result]
                        ):
                            preloaded_column_instances.pop(_column_reference, None)
            ## cache the student_data instances
            preloaded_student_data_instances = {} # keyed by table_uuid
            for _table_uuid, _table_instance in preloaded_table_instances.items():
                preloaded_student_data_instances[_table_uuid] = StudentData(_table_instance)
                if not preloaded_student_data_instances[_table_uuid].find_student(auth_user):
                    preloaded_student_data_instances.pop(_table_uuid, None)
            ## set convenience variables
            default_table_uuid = portal.config['default_table_uuid']
            if default_table_uuid in preloaded_table_instances.keys():
                default_table_instance = preloaded_table_instances[default_table_uuid]
            else:
                default_table_instance = Table()
                default_table_instance.load(default_table_uuid)
            if default_table_uuid in preloaded_student_data_instances.keys():
                student_data = preloaded_student_data_instances[default_table_uuid]
            else:
                student_data = StudentData(default_table_instance)
                student_data.find_student(auth_user)
            
            # check if student exists and add if allowed
            if portal.config['if_student_unknown'] == 'addextra' and not student_data.find_student(auth_user):
                portal.add_extra_student(auth_user)
            # loop through panels
            panels = [] # This is used to render the final portal contents. It does not necessarily reflect portal.config['panels']
            for i_panel, panel in enumerate(portal.config['panels']):
                show_panel = False
                panel_referenced_tables = portal.get_referenced_table_uuids(only_panel_uuid=panel['uuid'])
                panel_default_table_uuid = panel_referenced_tables[0] if len(panel_referenced_tables) > 0 else portal.config['default_table_uuid']
                if panel_default_table_uuid in preloaded_student_data_instances.keys():
                    student_data = preloaded_student_data_instances[panel_default_table_uuid]
                else:
                    panel_default_table_instance = Table()
                    panel_default_table_instance.load(panel_default_table_uuid)
                    preloaded_table_instances[panel_default_table_uuid] = panel_default_table_instance
                    student_data = StudentData(panel_default_table_instance)
                    student_data.find_student(auth_user)
                    preloaded_student_data_instances[panel_default_table_uuid] = student_data
                # Evaluate whether current student should see this panel
                if panel['show_when'] == 'always':
                    show_panel = True
                elif panel['show_when'] == 'conditions':
                    conditions = Conditions(
                        identifier=auth_user,
                        conditions=panel['conditions'],
                        student_data=student_data,
                        default_table=preloaded_table_instances[panel_default_table_uuid],
                        preloaded_columns=preloaded_column_instances
                    )
                    show_panel = conditions.evaluate_conditions()
                # Evaluate whether now is the time to show this panel
                _availability = panel.get('availability', 'available')
                if _availability == 'unavailable':
                    show_panel = False
                elif _availability == 'available-between':
                    try:
                        if (panel.get('availability_from') <= datetime.now() <= panel.get('availability_to')):
                            show_panel = show_panel and True
                        else:
                            show_panel = False
                    except:
                        show_panel = False
                # Render
                if show_panel:
                    # first do a replacement of all global magic formatters
                    if re.search(utils.GLOBAL_MAGIC_FORMATTER_REFERENCE_PATTERN, panel['content']) is not None:
                        portal.config['panels'][i_panel]['content'] = substitute_text_variables(
                            input=panel['content'],
                            identifier=auth_user,
                            default_table_uuid=panel_default_table_uuid,
                            preloaded_student_data=student_data,
                            preloaded_columns=preloaded_column_instances,
                            preloaded_student_data_instances=preloaded_student_data_instances,
                            only_process_global_magic_formatters=True # IMPORTANT!
                        )['new_text']
                    # then continue
                    if panel['mode'] == 'write' or panel['mode'] == 'readonly-inputs' or panel['mode'] == 'show-inputs':
                        if student_data._id:
                            # display writeable panel
                            field_references = re.findall(utils.DELIMITED_FIELD_REFERENCE_PATTERN, panel['content'])
                            panel_content = panel['content']
                            for field_reference in field_references:
                                panel_content = panel_content.replace(field_reference, '$$$SRES_PANEL_SPLIT$$$')
                            split_panel_content = panel_content.split('$$$SRES_PANEL_SPLIT$$$')
                            for i, field_reference in enumerate(field_references):
                                if field_reference.startswith('$') and field_reference.endswith('$'):
                                    field_reference = field_reference[1:-1]
                                # then deal with the field reference part
                                if field_reference in preloaded_column_instances.keys():
                                    # load from cache
                                    column = preloaded_column_instances[field_reference]
                                else:
                                    # try load table from cache at least
                                    if panel_default_table_uuid in preloaded_table_instances.keys():
                                        column = Column(preloaded_table=preloaded_table_instances[panel_default_table_uuid])
                                    else:
                                        # otherwise fresh instantiation
                                        column = Column()
                                if column.load(field_reference):
                                    if field_reference not in preloaded_column_instances.keys():
                                        preloaded_column_instances[field_reference] = column
                                    if ((column.is_writeable_by_students(auth_user) and column.is_self_data_entry_enabled()) and panel['mode'] == 'write') or ((panel['mode'] == 'readonly-inputs' or panel['mode'] == 'show-inputs') and not column.is_system_column):
                                        # append the textual (non-field part)
                                        _panel = {
                                            'render': 'html',
                                            'content': split_panel_content[i]
                                        }
                                        if i == 0:
                                            _panel['collapsible'] = panel.get('collapsible', 'disabled')
                                            _panel['collapsible_default_display'] = panel.get('collapsible_default_display', 'show')
                                        panels.append(_panel)
                                        # append the input
                                        #data_to_display = student_data.get_data(
                                        #    column_uuid=column.config['uuid'],
                                        #    preloaded_column=column
                                        #)['data']
                                        #data_to_display = student_data.get_data_for_entry(column)['data']
                                        _data = student_data.get_data_for_entry(column)
                                        data_to_display = _data['data']
                                        utils.mark_multientry_labels_as_safe(column)
                                        _readonly = ''
                                        if panel['mode'] == 'readonly-inputs':
                                            _readonly = 'readonly'
                                        elif panel['mode'] == 'show-inputs' and (not column.is_writeable_by_students(auth_user) or not column.is_self_data_entry_enabled()):
                                            _readonly = 'readonly'
                                        if logged_in_user_role == 'viewer':
                                            _readonly = 'readonly'
                                        _panel = {
                                            'render': 'input',
                                            'table': column.table,
                                            'column': column,
                                            'student_identifier': auth_user,
                                            'readonly': _readonly,
                                            'unique_string': utils.create_uuid(),
                                            'data_to_display': data_to_display,
                                            'multiple_reports_meta': {
                                                'index': _data['report_index'],
                                                'count': _data['report_available_number_count']
                                            },
                                            'trigger_reload_on_save': True if panel.get('trigger_reload_on_save') == 'enabled' else False
                                        }
                                        # see if this split subpanel is part of a panel that needs to be collapsed
                                        if panel.get('collapsible') == 'enabled' or panel.get('collapsible') == 'linked':
                                            _panel['collapsible'] = 'linked'
                                        panels.append(_panel)
                                    else:
                                        #logging.debug(f'i={i} field_reference={field_reference} split_panel_content={split_panel_content[i]}')
                                        # column not enabled for write, so just substitute
                                        panel_content = substitute_text_variables(
                                            input=f'{split_panel_content[i]}${field_reference}$', #panel['content'],#split_panel_content[i], #
                                            identifier=auth_user,
                                            default_table_uuid=panel_default_table_uuid,
                                            preloaded_student_data=student_data,
                                            preloaded_columns=preloaded_column_instances,
                                            preloaded_student_data_instances=preloaded_student_data_instances
                                        )['new_text']
                                        _panel = {
                                            'render': 'html',
                                            'content': utils.rn_to_br(panel_content)
                                        }
                                        # see if this split subpanel is part of a panel that needs to be collapsed
                                        if panel.get('collapsible') == 'enabled' or panel.get('collapsible') == 'linked':
                                            _panel['collapsible'] = 'linked'
                                        # store
                                        panels.append(_panel)
                                    # slide subfield config
                                    if column.config['uuid'] is not None:
                                        vars['rangeslider_config'][column.config['uuid']] = column.get_slider_subfields_config()
                                elif column.is_system_column and panel_default_table_uuid and column.load(field_reference, default_table_uuid=panel_default_table_uuid):
                                    panel_content = substitute_text_variables(
                                        input=f'${field_reference}$',
                                        identifier=auth_user,
                                        default_table_uuid=panel_default_table_uuid,
                                        preloaded_student_data=student_data,
                                        preloaded_columns=preloaded_column_instances,
                                        preloaded_student_data_instances=preloaded_student_data_instances
                                    )['new_text']
                                    panels.append({
                                        'render': 'html',
                                        'content': split_panel_content[i] + panel_content
                                    })
                                else:
                                    logging.error(f"Could not load field reference {field_reference} in portal uuid {portal_uuid}")
                            panels.append({
                                'render': 'html',
                                'content': split_panel_content[-1]
                            })
                        else:
                            panel_content = '<div class="alert alert-danger">Sorry, could not find person with identifier {}.</div>'.format(auth_user)
                            panels.append({
                                'render': 'html',
                                'content': panel_content
                            })
                            erroring_panels.append(panel.get('uuid', 'unknown'))
                    else:
                        if student_data._id:
                            panel_content = substitute_text_variables(
                                input=panel['content'],
                                identifier=auth_user,
                                default_table_uuid=panel_default_table_uuid,
                                preloaded_student_data=student_data,
                                preloaded_columns=preloaded_column_instances,
                                preloaded_student_data_instances=preloaded_student_data_instances
                            )
                            panels.append({
                                'render': 'html',
                                'content': utils.rn_to_br(panel_content['new_text']),
                                'scripts': panel_content['scripts'],
                                'collapsible': panel.get('collapsible', 'disabled'),
                                'collapsible_default_display': panel.get('collapsible_default_display', 'show')
                            })
                            if 'sres-summary-representation-wordcloud' in panel_content['new_text']:
                                vars['required_libraries'].append('wordcloud')
                                vars['required_libraries'].append('summary-draw')
                            if 'sres-summary-representation-chart' in panel_content['new_text']:
                                vars['required_libraries'].append('google-charts')
                                vars['required_libraries'].append('summary-draw')
                        else:
                            panel_content = '<div class="alert alert-danger">Sorry, could not find person with identifier {}.</div>'.format(auth_user)
                            panels.append({
                                'render': 'html',
                                'content': panel_content
                            })
                            erroring_panels.append(panel.get('uuid', 'unknown'))
            vars['previewing'] = previewing
            vars['identifier'] = identifier
            vars['auth_user'] = auth_user
            vars['erroring_panels'] = erroring_panels
            vars['disable_feedback_request'] = True if erroring_panels else False
            vars['max_width'] = portal.get_max_width()
            # render
            return render_template('portal-view.html', portal=portal, vars=vars, panels=panels)
        else:
            flash("There appears to be a problem authenticating access to this portal.", "danger")
    else:
        flash("There was an error loading the requested portal.", "danger")
    return render_template('denied.html')

@bp.route('/<portal_uuid>/students', methods=['GET'])
@login_required
def get_students(portal_uuid):
    search_term = bleach.clean(request.args.get('search', ''))
    portal = Portal()
    if portal.load(portal_uuid):
        if portal.is_user_authorised(roles=AUTHORISED_ROLES.keys()):
            referenced_table_uuids = portal.get_referenced_table_uuids()
            restrictor_column_uuid = None
            user_highest_role = portal.get_user_highest_role()
            if user_highest_role == 'teacher' or user_highest_role == 'viewer':
                restrictor_column_uuid = portal.config.get(f'{user_highest_role}s_limit_by_columnuuid')
            found_students = find_students_from_tables_by_term(
                search_term=search_term,
                table_uuids=referenced_table_uuids,
                anonymise_identities=is_identity_anonymiser_active(),
                restrictor_column_uuid=restrictor_column_uuid
            )
            # return
            return json.dumps({'students':found_students})
        else:
            abort(403)
    else:
        abort(400)

def _is_user_authorised_for_column(column_uuid, identifier, auth_user):
    ret = {
        'status_code': None,
        'status_text': '',
        'student_data': None,
        'column': None,
        'is_authorised_staff_user': False,
        'is_authorised_student_user': None
    }
    column = Column()
    if column.load(column_uuid):
        ret['column'] = column
        is_authorised_staff_user = column.is_user_authorised(username=auth_user, authorised_roles=['user', 'administrator'])
        ret['is_authorised_staff_user'] = is_authorised_staff_user
        if (column.is_writeable_by_students() and column.is_self_data_entry_enabled()) or is_authorised_staff_user:
            student_data_a = StudentData(column.table)
            student_data_b = StudentData(column.table)
            if student_data_a.find_student(identifier) and (student_data_b.find_student(get_auth_user()) or is_authorised_staff_user):
                if student_data_a.config['sid'] == student_data_b.config['sid'] or is_authorised_staff_user:
                    if student_data_a.in_time_delay_lockout(column_uuid=column.config['uuid'], seconds_differential=column.config['custom_options']['time_delay_lockout_duration']):
                        # check time delay lockout
                        ret['status_code'] = 429
                        ret['status_text'] = 'Time delay lockout is active'
                    else:
                        ret['student_data'] = student_data_a
                        ret['is_authorised_student_user'] = True
                        ret['status_code'] = 200
                else:
                    ret['status_code'] = 403
                    ret['status_text'] = 'Unauthorised'
            else:
                ret['status_code'] = 404
                ret['status_text'] = 'Error identifying student'
        else:
            ret['status_code'] = 403
            ret['status_text'] = 'Unauthorised'
    else:
        ret['status_code'] = 400
        ret['status_text'] = 'Unexpected error'
    return ret

@bp.route('/<portal_uuid>/get_data', methods=['GET'])
@login_required
def get_data(portal_uuid):
    column_uuid = utils.clean_uuid(request.args.get('column_uuid'))
    identifier = bleach.clean(request.args.get('identifier'))
    report_index = bleach.clean(request.args.get('report_index', '-1'))
    auth_result = _is_user_authorised_for_column(column_uuid, identifier, get_auth_user())
    if auth_result['status_code'] == 200:
        student_data = auth_result['student_data']
        column = auth_result['column']
        _data = student_data.get_data_for_entry(column, report_index=report_index)
        ret = {
            identifier: {
                'data': json.dumps(_data['data']),
                'all_reports_data': _data['all_reports_data'],
                'multiple_reports_meta': {
                    'index': _data['report_index'],
                    'count': _data['report_available_number_count']
                }
            }
        }
        return json.dumps(ret, default=str)
    else:
        abort(auth_result['status_code'])

@bp.route('/<portal_uuid>/set_data', methods=['POST'])
@login_required
def set_data(portal_uuid):
    column_uuid = utils.clean_uuid(request.form.get('c'))
    identifier = bleach.clean(request.form.get('i'))
    data_type = request.form.get('t', '')
    report_index = bleach.clean(request.form.get('ri', ''))
    ret = {
        'identifier': None,
        'success': False,
        'data': None,
        'messages': [],
        'multiple_reports_meta': {
            'index': None,
            'count': None
        }
    }
    # check permissions
    auth_result = _is_user_authorised_for_column(column_uuid, identifier, get_auth_user())
    if auth_result['status_code'] == 200:
        student_data_a = auth_result['student_data']
        column = auth_result['column']
        if data_type == '':
            uncleaned_data = request.form.get('d')
            if column.config['type'] == 'multiEntry':
                # special cleaning approach for multiEntry columns
                data = utils.bleach_multientry_data(
                    data=uncleaned_data,
                    column=column,
                    bleach_all_subfields=True
                )
            else:
                data = bleach.clean(
                    uncleaned_data,
                    tags=utils.BLEACH_ALLOWED_TAGS,
                    attributes=utils.BLEACH_ALLOWED_ATTRIBUTES,
                    styles=utils.BLEACH_ALLOWED_STYLES
                )
            # save data
            result = student_data_a.set_data(
                column_uuid=column_uuid, 
                data=data,
                commit_immediately=True,
                preloaded_column=column,
                skip_auth_checks=True, # because auth already checked here
                auth_user_override=get_auth_user(),
                report_index=report_index,
                only_save_history_if_delta=False,
                authorised_as_student=auth_result['is_authorised_student_user']
            )
            ret['success'] = result['success']
            ret['data'] = data
            ret['identifier'] = identifier
            ret['messages'].extend(result['messages'])
            # if image column, need to format return data differently to provide access key/url
            if column.config['type'] == 'image':
                ret['data'] = {
                    'data': data,
                    'key': get_file_access_key(data),
                    'url': get_file_access_url(data, full_path=True)
                }
            # process notifyemail if needed
            if column.config['notify_email']['active'] == 'true':
                notify_email_result = student_data_a.send_notify_email(column.config['uuid'], bypass_auth_check=True)
                ret['notify_email'] = notify_email_result
                ret['messages'].extend(notify_email_result['messages'])
            # return multiple_reports_index if necessary
            ret['multiple_reports_meta']['index'] = result['multiple_reports_meta'].get('index', '')
            ret['multiple_reports_meta']['count'] = result['multiple_reports_meta'].get('count', '')
        elif data_type == 'image':
            data = bleach.clean(request.form.get('d'))
            result = student_data_a.set_data_image(
                column_uuid=column_uuid,
                image_data=data,
                save_image_only=True
            )
            ret['success'] = result['success']
            ret['data'] = result['new_image_filename']
            ret['identifier'] = identifier
            ret['messages'].extend(result['messages'])
        elif data_type == 'audio':
            f = request.files['d'];
            result = student_data_a.set_data_rich(
                table_uuid=column.table.config['uuid'],
                column_uuid=column_uuid,
                identifier=identifier,
                rich_data=f
            )
            ret['success'] = result['success']
            ret['data'] = result['saved_as_filename']
            ret['identifier'] = identifier
            ret['messages'].extend(result['messages'])
        elif data_type == 'file':
            ret['data'] = []
            for i, file in enumerate(request.files.getlist('files')):
                if i >= int(column.config['custom_options']['datatype_file_multiple_number']):
                    # reached the file upload limit
                    ret['messages'].append(("Too many files selected", "warning"))
                    break
                _result = student_data_a.set_data_rich(
                    table_uuid=column.table.config['uuid'],
                    column_uuid=column_uuid,
                    identifier=identifier,
                    rich_data=file
                )
                if i == 0:
                    ret['success'] = _result['success']
                else:
                    ret['success'] = ret['success'] and _result['success']
                if _result['success']:
                    ret['data'].append({
                        'original_filename': file.filename,
                        'saved_filename': _result['saved_as_filename'],
                        'url': get_file_access_url(_result['saved_as_filename'], full_path=True)
                    })
                ret['messages'].extend(_result['messages'])
            ret['identifier'] = identifier
        # return
        return json.dumps(ret)
    else:
        abort(make_response(auth_result['status_text'], auth_result['status_code']))

@bp.route('/<portal_uuid>/log', methods=['POST'])
def log_event(portal_uuid):
    element = request.form.get('e')
    details = request.form.get('d')
    action = request.form.get('a')
    identifier = request.form.get('i')
    portal = Portal()
    if portal.load(portal_uuid):
        add_interaction_event(
            source_asset_type='portal',
            source_asset_uuid=portal_uuid,
            parent=portal_uuid,
            action=action,
            data={
                'details': details,
                'element': element,
                'auth_user': get_auth_user()
            },
            target=identifier
        )
    return ""

@bp.route('/<portal_uuid>/logs', methods=['GET'])
@login_required
def view_logs(portal_uuid):
    add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='logs')
    portal = Portal()
    vars = {}
    vars['asset_name'] = 'portal'
    if portal.load(portal_uuid) and portal.is_user_authorised():
        # per-student interactions
        interaction_logs = portal.get_interaction_logs()
        vars['interaction_logs'] = interaction_logs
        # some stats
        vars['quick_stats'] = []
        ## hit count
        main_table_uuid = portal.get_referenced_table_uuids()[0]
        table = Table()
        main_table_student_count = -1
        if table.load(main_table_uuid):
            main_table_student_count = len(table.get_all_students_sids())
        vars['quick_stats'].append({
            'icon': 'users',
            'html': "{} out of {} students ({:.{prec}f}%) in <a href=\"{table_link}\">{table_name}</a> have opened this portal at least once.".format(
                len(interaction_logs['opened_by']),
                main_table_student_count,
                (len(interaction_logs['opened_by']) / main_table_student_count) * 100,
                prec=1,
                table_link=url_for('table.view_table', table_uuid=table.config['uuid']),
                table_name=table.get_full_name()
            )
        })
        ## total hit count
        vars['quick_stats'].append({
            'icon': 'door-open',
            'html': "Portal has been opened {} times in total.".format(interaction_logs['total_opens'])
        })
        # actions for each student
        vars['interaction_record_actions'] = [
            {
                'target_fn': 'previewPortalForStudent',
                'icon': 'eye',
                'text': '',
                'tooltip': 'Preview portal'
            }
        ]
        vars['show_interaction_details'] = True
        # feedback
        vars['feedback_was_requested'] = False if portal.config['feedback']['style'] == 'null' else True
        if vars['feedback_was_requested']:
            vote_stats = portal.get_feedback_stats()
            votes_display = []
            for vote in vote_stats['votes']:
                vote_count = vote['count']
                vote_count_substantiated = vote['count_substantiated']
                vote_text = vote['vote']
                vote_percent = vote_count / vote_stats['total_votes'] * 100 if vote_stats['total_votes'] > 0 else 0
                vote_percent_substantiated = vote_count_substantiated / vote_stats['total_votes_substantiated'] * 100 if vote_stats['total_votes_substantiated'] > 0 else 0
                votes_display.append('<strong>{}</strong>: {} ({:.{prec}f}%) [plus {} unsubstantiated votes]'.format(
                    vote_text,
                    vote_count_substantiated,
                    vote_percent_substantiated,
                    vote_count - vote_count_substantiated,
                    prec=1
                ))
            vars['quick_stats'].append({
                'icon': 'poll',
                'html': Markup('{} feedback votes ({} unique, {} substantiated) received to prompt <em>{}</em>. '.format(
                    vote_stats['total_votes'],
                    vote_stats['unique_votes'],
                    vote_stats['total_votes_substantiated'],
                    portal.config['feedback']['prompt']
                ) + '; '.join(votes_display) if len(votes_display) else "No feedback recorded")
            })
            # feedback comments
            vars['comments_by_vote'] = vote_stats['comments_by_vote']
        # render
        return render_template('interactions-view-log.html', vars=vars, asset=portal)
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')
    pass

@bp.route('/<portal_uuid>/feedback', methods=['POST'])
@login_required
def feedback_event(portal_uuid):
    portal_uuid = bleach.clean(portal_uuid)
    vote = bleach.clean(request.form.get('vote', ''))
    data = bleach.clean(request.form.get('data', '{}'))
    oid = bleach.clean(request.form.get('oid', ''))
    # try to add feedback event
    res = add_feedback_event(
        source_asset_type='portal',
        source_asset_uuid=portal_uuid,
        vote=vote,
        data=json.loads(data),
        target=get_auth_user(),
        _id=oid if oid else None
    )
    # return
    return json.dumps({
        'oid': str(res) if res else oid
    })

@bp.route('/<portal_uuid>/convert/filter', methods=['GET'])
@login_required
def convert_to_filter(portal_uuid):
    from sres.filters import Filter
    add_access_event(asset_type='portal', asset_uuid=portal_uuid, action='convert_to_filter')
    portal = Portal()
    if is_user_administrator('filter') or is_user_administrator('super'):
        # check permissions
        if not portal.load(portal_uuid):
            flash("Sorry, this portal could not be loaded.", "danger")
            return render_template('denied.html')
        if not portal.is_user_authorised():
            flash("Sorry, you do not appear to have the right permissions to proceed.", "danger")
            return render_template('denied.html')
        # continue
        filter = Filter()
        if not filter.create():
            flash("Problem creating new filter.", "danger")
            return render_template('denied.html')
        if filter._id:
            filter.config['name'] = f"Filter created from portal {portal.config['name']}"
            filter.config['description'] = portal.config['description']
            filter.config['administrators'] = portal.config['administrators']
            filter.config['contact_type'] = ['email']
            filter.config['email']['subject'] = ''
            filter.config['email']['body_first'] = ''
            filter.config['email']['body_last'] = ''
            filter.config['created'] = datetime.now()
            filter.config['modified'] = datetime.now()
            filter.config['workflow_state'] = 'active'
            # push portal panels over to filter sections
            filter.config['email']['sections'] = []
            for panel in portal.config['panels']:
                section = {
                    'show_when': panel.get('show_when', 'always'),
                    'conditions': panel.get('conditions', {}),
                    'content': panel.get('content', '')
                }
                filter.config['email']['sections'].append(section)
            # save
            if filter.update():
                flash(Markup("Filter created successfully from portal. <span class=\"fa fa-exclamation-triangle\"></span> You will need to set a few filter-specific settings and save this filter before it can be used, such as the primary conditions and some message details."), "success")
                return redirect(url_for('filter.edit_filter', filter_uuid=filter.config['uuid']))
            else:
                flash("There was a problem creating a filter from the portal.", "danger")
                return redirect(url_for('portal.edit_portal', portal_uuid=portal.config['uuid']))
    else:
        flash("Sorry, only administrators are authorised to access this feature.", "warning")
        return render_template('denied.html')
    