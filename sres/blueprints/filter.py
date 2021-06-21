from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app, Markup, abort)
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re
import logging
from dateutil import parser

from sres.auth import login_required, is_user_administrator, get_auth_user, get_auth_user_oid
from sres.filters import Filter, FEEDBACK_STYLES, list_authorised_filters, HIDEABLE_UI_ELEMENTS
from sres.columns import SYSTEM_COLUMNS, Column, column_oid_to_uuid, column_uuid_to_oid, MAGIC_FORMATTERS_LIST
from sres.tables import list_authorised_tables, Table, table_oid_to_uuid, table_uuid_to_oid
from sres import utils
from sres.files import get_file_access_url, GridFile
from sres.users import usernames_to_oids
from sres.collective_assets import CollectiveAsset
from sres.access_logs import add_access_event
from sres.conditions import OpenConditions
from sres.anonymiser import is_identity_anonymiser_active, anonymise_identifier, anonymise_within_content, anonymise
from sres.studentdata import StudentData
from sres.connector_canvas import CanvasConnector

bp = Blueprint('filter', __name__, url_prefix='/filters')

def _get_select_array(referenced_table_uuids):
    select_array = []
    for referenced_table_uuid in referenced_table_uuids:
        table_temp = Table()
        if table_temp.load(referenced_table_uuid) and table_temp.is_user_authorised():
            select_array.extend(table_temp.get_select_array(show_collapsed_multientry_option=True))
    return select_array
    
def _build_query_builder_filters_from_select_array(select_array):
    query_builder_filters = []
    for c in select_array:
        query_builder_filters.append({
            'id': c['value'],
            'label': c['full_display_text'],
            'type': 'string'
        })
    return query_builder_filters

@bp.route('/new', methods=['GET', 'POST'])
@login_required
def new_filter(searches=None):
    add_access_event(asset_type='filter', asset_uuid=None, action='new')
    vars = {}
    vars['mode'] = 'new'
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['MAGIC_FORMATTERS_LIST'] = MAGIC_FORMATTERS_LIST
    vars['FONT_FORMATS'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('FORMATTING', {}).get('FONT_FORMATS', '')
    vars['authorised_tables'] = list_authorised_tables()
    vars['ui_hide'] = {}
    vars['query_builder_filters'] = []
    vars['query_builder_rules'] = {}
    # determine if drawing from a source table to start with
    if request.args.get('source_table_uuid'):
        vars['referenced_table_uuids'] = [request.args.get('source_table_uuid')]
    else:
        vars['referenced_table_uuids'] = []
    # determine if an initial config is set
    if request.args.get('initial_config'):
        # read it
        initial_config = request.args.get('initial_config')
        initial_config = json.loads(utils.from_b64(initial_config))
        # starting rules for queryBuilder
        vars['query_builder_rules'] = initial_config.get('conditions', {})
        # starting filters for queryBuilder
        vars['query_builder_filters'] = []
        select_array = _get_select_array(vars['referenced_table_uuids'])
        if select_array:
            vars['query_builder_filters'] = _build_query_builder_filters_from_select_array(select_array)
    # render
    if is_user_administrator('filter') or is_user_administrator('super'):
        filter = Filter()
        filter.config['administrators'] = [get_auth_user_oid()]
        filter.config['uuid'] = '__new__'
        return render_template('filter-edit.html', filter=filter, vars=vars)
    else:
        flash("Sorry, only filter administrators are authorised to access this feature.", "warning")
        return render_template('denied.html')

@bp.route('/<filter_uuid>/edit', methods=['GET', 'POST'])
@login_required
def edit_filter(filter_uuid, collective_mode=None, collective_asset_uuid=None, collective_vars=None):
    add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='edit')
    vars = {}
    filter = Filter()
    vars['mode'] = 'edit'
    vars['cloned'] = True if request.args.get('cloned') else False
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['MAGIC_FORMATTERS_LIST'] = MAGIC_FORMATTERS_LIST
    vars['FONT_FORMATS'] = current_app.config['SRES'].get('LOCALISATIONS', {}).get('FORMATTING', {}).get('FONT_FORMATS', '')
    vars['authorised_tables'] = list_authorised_tables()
    # set some ui toggles
    ui_hide = request.args.get('uih', '')
    vars['ui_hide'] = {}
    for el in HIDEABLE_UI_ELEMENTS:
        vars['ui_hide'][el[1]] = True if el[0] in ui_hide else False
    vars['uih'] = request.args.get('uih')
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
    # try load the filter
    try:
        if request.method == 'POST' and request.form['action'] == 'new':
            # adding a new filter
            if is_user_administrator('filter') or is_user_administrator('super'):
                pass
            else:
                flash("Sorry, only filter administrators are authorised to access this feature.", "warning")
                return render_template('denied.html')
        else:
            if collective_asset_uuid is None or collective_asset_uuid == '':
                if not filter.load(filter_uuid):
                    flash("Sorry, this filter could not be loaded.", "danger")
                    return render_template('denied.html')
                if not filter.is_user_authorised():
                    flash("Sorry, you do not appear to have the right permissions to proceed.", "danger")
                    return render_template('denied.html')
                if collective_asset_uuid == '':
                    # adding a new collective asset
                    collective_asset = CollectiveAsset('filter')
                    if collective_asset.create() and collective_asset.set_new_asset(filter):
                        filter = collective_asset.asset
                    else:
                        flash("Sorry, there was a problem making the Collective asset.", "danger")
                        return render_template('denied.html')
            else:
                # collective asset exists
                collective_asset = CollectiveAsset('filter')
                if collective_asset.load(collective_asset_uuid):
                    filter = collective_asset.asset
                    vars['collective_sharing_asset'] = collective_asset
                else:
                    flash("Sorry, there was a problem loading the Collective asset.", "danger")
                    return render_template('denied.html')
    except Exception as e:
        logging.exception(e)
        flash("Sorry, this filter could not be loaded. You may not have the right permissions, or the filter may not exist.", "warning")
        return render_template('denied.html')
    # continue
    if request.method == 'GET':
        add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='view')
        # starting rules for queryBuilder
        vars['query_builder_rules'] = filter.config['conditions']
        # starting filters for queryBuilder
        vars['query_builder_filters'] = []
        select_array = []
        if filter.is_collective_asset or request.args.get('from_collective_asset_uuid'):
            if request.args.get('from_collective_asset_uuid'):
                collective_asset = CollectiveAsset()
                collective_asset.load(request.args.get('from_collective_asset_uuid'))
            select_array = collective_asset.get_select_array_for_referenced_columns()
            referenced_table_uuids = []
        else:
            referenced_table_uuids = filter.get_referenced_table_uuids()
            select_array = _get_select_array(referenced_table_uuids)
        if select_array:
            vars['query_builder_filters'] = _build_query_builder_filters_from_select_array(select_array)
        vars['referenced_table_uuids'] = referenced_table_uuids
        # attachments
        for i in range(0, len(filter.config['email']['attachments'])):
            filter.config['email']['attachments'][i]['access_url'] = get_file_access_url(filter.config['email']['attachments'][i]['filename'])
        # filter status
        if collective_mode:
            vars['filter_disabled'] = ''
        else:
            vars['filter_disabled'] = 'disabled' if len(filter.config['run_history']) > 0 else ''
        # tracking
        tracking_column = Column()
        tracking_column.load(filter.config['tracking_record'][0].get('column_uuid'))
        vars['tracking_column'] = tracking_column
        if not request.args.get('from_collective_asset_uuid') and not collective_asset_uuid:
            # check tracking counter table
            if len(referenced_table_uuids) and referenced_table_uuids[0] != filter.config['tracking_record'][0]['table_uuid']:
                flash(Markup("<span class=\"fa fa-exclamation-triangle\"></span> The tracking column may be in the wrong list. If so, message reads will not be easily tracked. To correct this, choose to create a new tracking column or select an existing column from the correct list."), "danger")
            # check referenced_table_uuids
            if len(referenced_table_uuids) == 0:
                flash("A list related to this filter could not be identified. Often this is caused by the failure to specify any references to data columns. This must be rectified before running the filter.", "danger")
            if len(referenced_table_uuids) > 1:
                flash(Markup("<span class=\"fa fa-info-circle\"></span> This filter appears to point to more than one list. If this is intentional, you can ignore this warning. If not, you will need to ensure all column references point to the same list, otherwise you risk the filter failing to identify the appropriate students."), "warning")
            # check if scheduled send is active
            if filter.is_send_schedule_active():
                scheduled_run_utc_ts = filter.get_send_schedule_run_utc_ts()
                flash(Markup(f"<span class=\"fa fa-clock\"></span> This filter is scheduled to send later on <span class=\"sres_schedule_run_utc_ts\" data-sres-schedule-run-utc-ts=\"{scheduled_run_utc_ts}\">{scheduled_run_utc_ts}</span>. <span class=\"sres_schedule_cancel\"></span>"), 'info')
            # if sending via canvasinbox, check if a canvas course is connected
            if 'canvasinbox' in filter.config['contact_type']:
                canvas_connector = CanvasConnector()
                canvas_connector.load_connections(filter.config['tracking_record'][0].get('table_uuid'))
                canvas_connector.load_connected_course_ids()
                if len(canvas_connector.connected_course_ids) == 0:
                    flash(Markup("<span class=\"fa fa-exclamation-triangle\"></span> This filter is set to send messages via the Canvas inbox, but you have not connected the <a href=\"{view_list_href}\">associated list</a> to Canvas. A Canvas connection must be made first.".format(
                        view_list_href=url_for('table.view_table', table_uuid=filter.config['tracking_record'][0].get('table_uuid'))
                    )), 'danger')
        # render
        return render_template('filter-edit.html', filter=filter, vars=vars)
    elif request.method == 'POST':
        #print(request.form)
        #print(request.files)
        if request.form['action'] == 'new':
            add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='create')
            if not filter.create():
                flash("Problem creating new filter.", "danger")
        elif request.form['action'] == 'edit' and collective_asset_uuid is None and filter._id is None:
            add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='update')
            if filter.load(filter_uuid) and filter.is_user_authorised():
                pass
            else:
                flash("Problem loading filter.", "danger")
        if filter._id:
            # general config
            filter.config['name'] = request.form.get('filter_name')
            filter.config['description'] = request.form.get('filter_description')
            filter.config['administrators'] = usernames_to_oids(request.form.getlist('authorised_administrators'))
            # primary conditions
            primary_conditions = request.form.get('rules_for_primary_conditions')
            if utils.is_json(primary_conditions):
                filter.config['conditions'] = json.loads(primary_conditions)
            filter.config['advanced_conditions']['enabled'] = True if 'conditions_use_advanced' in request.form.keys() else False
            filter.config['advanced_conditions']['expression'] = request.form.get('conditions_advanced_expression')
            # contact types
            filter.config['contact_type'] = []
            for contact_type in [v for k, v in request.form.items() if k.startswith('contact_type_')]:
                filter.config['contact_type'].append(contact_type)
            # email config
            filter.config['email']['sender']['name'] = request.form.get('sender_name')
            filter.config['email']['sender']['email'] = request.form.get('sender_email')
            filter.config['email']['addresses']['reply_to'] = request.form.get('reply_to_email')
            filter.config['email']['addresses']['cc'] = request.form.get('cc_email')
            filter.config['email']['addresses']['bcc'] = request.form.get('bcc_email')
            filter.config['email']['addresses']['to'] = request.form.get('to_email', '')
            filter.config['email']['subject'] = request.form.get('email_subject')
            filter.config['email']['body_first'] = request.form.get('email_body_first')
            filter.config['email']['body_last'] = request.form.get('email_body_last')
            # email additional sections
            if utils.is_json(request.form.get('sections')):
                sections = []
                for section_number in json.loads(request.form.get('sections')):
                    section = {
                        'show_when': request.form.get('section_showwhen_section_x{}'.format(section_number), 'always'),
                        'content': request.form.get('email_body_section_section_x{}'.format(section_number), ''),
                        'conditions': {"condition": "AND", "rules": []},
                        'display_order': section_number
                    }
                    section_conditions = request.form.get('rules_for_section_conditions_section_x{}'.format(section_number))
                    if section_conditions and section_conditions != 'null' and utils.is_json(section_conditions):
                        section['conditions'] = json.loads(section_conditions)
                    sections.append(section)
                filter.config['email']['sections'] = sections
            # TODO 'target_column': None, # ObjectId
            
            # email attachments - uploads
            attachments = request.files.getlist('email_attachments')
            for attachment in attachments:
                if not attachment:
                    continue
                new_filename = utils.create_uuid() + '.' + attachment.filename.split('.')[-1]
                gf = GridFile('files')
                if gf.save_file(attachment, new_filename):
                    filter.add_attachment(
                        local_filename=new_filename, 
                        original_filename=attachment.filename, 
                        file_size=gf.bytes
                    )
                else:
                    flash("Problem adding an attachment.", "warning")
                    logging.error('Problem adding an attachment [{}] [{}]'.format(filter.config['uuid'], attachment.filename))
            # email attachments - deletions
            for delete_key in [k for k in request.form.keys() if k.startswith('email_attachments_delete_')]:
                filename = request.form.get(delete_key)
                if not filter.delete_attachment(filename):
                    flash("Problem deleting an attachment.", "warning")
            # email feedback
            filter.config['email']['feedback'] = FEEDBACK_STYLES[request.form.get('feedback_request', 'helpfulyesno')]
            # housekeeping config
            if request.form['action'] == 'new':
                filter.config['created'] = datetime.now()
            if collective_asset_uuid is not None:
                filter.config['workflow_state'] = 'collective'
            else:
                filter.config['workflow_state'] = 'active'
            filter.config['modified'] = datetime.now()
            # sms
            # TODO
            """
                'sms': {
                    'body': '',
                    'runs_remaining': 0,
                    'target_column': None # ObjectId
                },
            """
            # tracking record
            tracking_column = Column()
            if request.form.get('email_tracking_column_action') == 'new':
                if 'email_tracking_column_name' in request.form.keys():
                    referenced_table_uuids = filter.get_referenced_table_uuids(skip_tracking_record=True)
                    if len(referenced_table_uuids):
                        probable_table_uuid = referenced_table_uuids[0]
                        tracking_column.create(table_uuid=probable_table_uuid)
                        tracking_column.config['name'] = request.form.get('email_tracking_column_name')
                        tracking_column.config['active']['from'] = datetime.now()
                        tracking_column.config['active']['to'] = datetime.now()
                        if tracking_column.update():
                            filter.config['tracking_record'][0] = {
                                'table_uuid': tracking_column.table.config['uuid'],
                                'column_uuid': tracking_column.config['uuid']
                            }
                            flash("A new tracking column was created in {}.".format(tracking_column.table.get_full_name()), "success")
                        else:
                            flash("Problem creating the tracking column.", "warning")
                    else:
                        flash("A list related to this filter could not be identified. Often this is caused by the failure to specify any references to data columns. This must be rectified before running the filter.", "danger")
            elif request.form.get('email_tracking_column_action') == 'current':
                filter.config['tracking_record'][0] = {
                    'table_uuid': request.form.get('tracking_record_tableuuid', ''),
                    'column_uuid': request.form.get('tracking_record_columnuuid', '')
                }
            elif request.form.get('email_tracking_column_action') == 'another':
                if request.form.get('tracking_column_use_existing') and tracking_column.load(request.form.get('tracking_column_use_existing')):
                    filter.config['tracking_record'][0] = {
                        'table_uuid': tracking_column.table.config['uuid'],
                        'column_uuid': tracking_column.config['uuid']
                    }
            # save
            if filter.update():
                flash("Filter configuration successfully updated.", "success")
            else:
                flash("Unexpected error updating filter configuration.", "danger")
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
                # save and preview action
                return redirect(url_for('filter.preview_filter', filter_uuid=filter.config['uuid'], uih=ui_hide))
            else:
                # standard save action
                return redirect(url_for('filter.edit_filter', filter_uuid=filter.config['uuid'], uih=ui_hide))

@bp.route('/<filter_uuid>/clone', methods=['GET'])
@login_required
def clone_filter(filter_uuid):
    add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='clone')
    filter = Filter()
    cloned_filter = Filter()
    result = None
    if filter.load(filter_uuid) and filter.is_user_authorised():
        result = filter.clone()
        if result and cloned_filter.load(result):
            flash("Cloned successfully. You are now editing the clone.", "success")
        else:
            flash("Error while cloning.", "warning")
    else:
        flash("Insufficient permissions to read this filter.", "danger")
    if result:
        return redirect(url_for('filter.edit_filter', filter_uuid=cloned_filter.config['uuid'], cloned=True))
    else:
        return redirect(url_for('filter.edit_filter', filter_uuid=filter.config['uuid']))
    
@bp.route('/<filter_uuid>/delete', methods=['GET', 'DELETE'])
@login_required
def delete_filter(filter_uuid):
    add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='delete')
    filter = Filter()
    if filter.load(filter_uuid) and filter.is_user_authorised():
        if filter.delete():
            if request.method == 'GET':
                flash("Successfully deleted filter.", "success")
                return redirect(url_for('index.index'))
            elif request.method == 'DELETE':
                return 'OK'
        else:
            if request.method == 'GET':
                flash("Error deleting filter.", "warning")
                return redirect(url_for('filter.edit_filter', filter_uuid=filter.config['uuid']))
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
def list_filters():
    filters = list_authorised_filters()
    ret = [
        {
            'uuid': f['uuid'],
            'name': f['name'],
            'description': f['description'],
            'created': f['created'],
            'modified': f.get('modified'),
            'workflow_state': f['workflow_state'],
            'run_history': f['run_history']
        }
        for f in filters
    ]
    return json.dumps(ret, default=str)

@bp.route('/check_advanced_expression', methods=['POST'])
@login_required
def check_advanced_expression():
    body = request.form.get('body', '')
    expr = BeautifulSoup(body, 'html.parser').get_text()
    expr = utils.clean_exprtk_expression(expr)
    result = utils.check_exprtk_expression(expr)
    return json.dumps(result)

@bp.route('/<filter_uuid>/preview', methods=['GET'])
@login_required
def preview_filter(filter_uuid):
    add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='preview')
    filter = Filter()
    vars = {}
    vars['mode'] = 'preview'
    if filter.load(filter_uuid) and filter.is_user_authorised():
        vars['contact_type'] = filter.config['contact_type']
        # preflight checks
        pre_run_checks = filter.pre_run_check()
        if not pre_run_checks['success']:
            for message in pre_run_checks['messages']:
                flash(message[0], message[1])
            return redirect(url_for('filter.edit_filter', filter_uuid=filter_uuid))
        # run
        results = filter.run_conditions()
        # format the results
        formatted_results = {
            k: {
                'sid': v['sid'],
                'display_sid': v['sid'],
                'email': v['email'],
                'display_email': v['email'],
                'preferred_name': v['preferred_name'],
                'surname': v['surname'],
            } for k, v in results['data'].items()
        }
        if is_identity_anonymiser_active():
            for k, v in formatted_results.items():
                for f in ['display_sid', 'display_email', 'preferred_name', 'surname']:
                    formatted_results[k][f] = anonymise(f, v[f])
        formatted_headers = {
            'sid':              {'display':'SID',               'class':'sid',              'hide_from_table':True},
            'display_sid':      {'display':'SID',               'class':'display_sid',      'hide_from_table':False},
            'email':            {'display':'Email',             'class':'email',            'hide_from_table':True},
            'display_email':    {'display':'Email',             'class':'email',            'hide_from_table':False},
            'preferred_name':   {'display':'Preferred name',    'class':'preferred_name',   'hide_from_table':False},
            'surname':          {'display':'Surname',           'class':'surname',          'hide_from_table':False}
        }
        for column_reference in results['all_column_references']:
            column = Column()
            # add to formatted outputs
            formatted_headers[column_reference] = {
                'display': column_reference,
                'class': re.sub("[^A-Z0-9a-z_]", '_', column_reference),
                'hide_from_table': False
            }
            if column.load(column_reference):
                # add nice display to formatted_headers
                formatted_headers[column_reference]['display'] = column.get_friendly_name()
                # add to formatted_results
                if column.subfield is None or column.subfield == '':
                    for key, result in results['data'].items():
                        formatted_results[key][column_reference] = result[column_reference] if column_reference in result else 'xx'
                else:
                    for key, result in results['data'].items():
                        if column.config['uuid'] in result and column.subfield < len(result[column.config['uuid']]):
                            formatted_results[key][column_reference] = result[column.config['uuid']][column.subfield]
                        else:
                            formatted_results[key][column_reference] = ''
            else:
                formatted_results[key][column_reference] = result[column_reference]
        # any warnings about the results?
        vars['results_warnings'] = {
            'missing_email_count': 0
        }
        for k, v in results['data'].items():
            if not v['email']:
                vars['results_warnings']['missing_email_count'] += 1
        # get messages already sent
        targets = [v['email'] for k, v in results['data'].items()]
        targets = list(set(targets))
        vars['already_sent_7'] = filter.count_filters_run_per_targets(
            targets=targets,
            from_date=datetime.now() - timedelta(days=7),
            to_date=datetime.now()
        )
        vars['already_sent_14'] = filter.count_filters_run_per_targets(
            targets=targets,
            from_date=datetime.now() - timedelta(days=14),
            to_date=datetime.now()
        )
        vars['already_sent_31'] = filter.count_filters_run_per_targets(
            targets=targets,
            from_date=datetime.now() - timedelta(days=31),
            to_date=datetime.now()
        )
        # check if scheduled send is active
        if filter.is_send_schedule_active():
            scheduled_run_utc_ts = filter.get_send_schedule_run_utc_ts()
            flash(Markup(f"<span class=\"fa fa-clock\"></span> This filter is scheduled to send later on <span class=\"sres_schedule_run_utc_ts\" data-sres-schedule-run-utc-ts=\"{scheduled_run_utc_ts}\">{scheduled_run_utc_ts}</span>. <span class=\"sres_schedule_cancel\"></span>"), 'info')
        # if sending via canvasinbox, check if a canvas course is connected
        if 'canvasinbox' in filter.config['contact_type']:
            canvas_connector = CanvasConnector()
            canvas_connector.load_connections(filter.config['tracking_record'][0].get('table_uuid'))
            canvas_connector.load_connected_course_ids()
            if len(canvas_connector.connected_course_ids) == 0:
                flash(Markup("<span class=\"fa fa-exclamation-triangle\"></span> This filter is set to send messages via the Canvas inbox, but you have not connected the <a href=\"{view_list_href}\">associated list</a> to Canvas. A Canvas connection must be made first.".format(
                    view_list_href=url_for('table.view_table', table_uuid=filter.config['tracking_record'][0].get('table_uuid'))
                )), 'danger')
        # render
        return render_template('filter-run.html', filter=filter, vars=vars, results=formatted_results, headers=formatted_headers)
    else:
        flash("Insufficient permissions to read this filter.", "danger")
        
@bp.route('/<filter_uuid>/message/<mode>', methods=['POST', 'DELETE'])
@login_required
def run_filter_message(filter_uuid, mode='preview'):
    filter = Filter()
    if filter.load(filter_uuid):
        if filter.is_user_authorised():
            if request.method == 'POST':
                identifiers = request.form.getlist('identifiers[]')
                #logging.debug(str(identifiers))
                if mode == 'preview':
                    results = filter.get_personalised_message(identifiers=identifiers, mode=mode)
                    if is_identity_anonymiser_active():
                        for i, result in enumerate(results):
                            results[i]['email']['target']['email'] = anonymise('email', results[i]['email']['target']['email'])
                            results[i]['email']['target']['name'] = anonymise('full_name', results[i]['email']['target']['name'])
                elif mode == 'queue':
                    results = filter.queue_send(identifiers=identifiers, auth_username=get_auth_user())
                elif mode == 'schedule':
                    scheduled_ts_utc = request.form.get('scheduled_ts_utc')
                    scheduled_dt = parser.isoparse(scheduled_ts_utc)
                    ignorelist_identifiers = request.form.getlist('ignorelist_identifiers[]')
                    reminder_hours_advance = request.form.get('reminder_hours_advance')
                    results = filter.schedule_send(
                        identifiers=identifiers,
                        ignorelist_identifiers=ignorelist_identifiers,
                        auth_username=get_auth_user(),
                        scheduled_dt=scheduled_dt,
                        reminder_hours_advance=reminder_hours_advance,
                        advanced_schedule=True
                    )
                return json.dumps(results)
            elif request.method == 'DELETE':
                if mode == 'schedule':
                    return json.dumps({
                        'success': filter.delete_scheduled_send()
                    })
        else:
            abort(403)
    else:
        abort(400)

@bp.route('/<filter_uuid>/add_run_history', methods=['POST'])
@login_required
def add_run_history(filter_uuid):
    filter = Filter()
    if filter.load(filter_uuid):
        if filter.is_user_authorised():
            return json.dumps(filter.add_run_history())
        else:
            abort(403)
    else:
        abort(400)

@bp.route('/<filter_uuid>/logs/messages', methods=['GET'])
@login_required
def get_sent_messages(filter_uuid):
    vars = {}
    filter = Filter()
    if filter.load(filter_uuid) and filter.is_user_authorised():
        target = request.args.get('target')
        if target:
            sent_messages = filter.get_sent_messages(targets=[target])
            if is_identity_anonymiser_active():
                for i, sent_message in enumerate(sent_messages):
                    student_data = StudentData(filter.get_referenced_table_uuids()[0])
                    if student_data.find_student(sent_message['target']):
                        # try do a brute force replace
                        sent_messages[i]['message']['subject'] = anonymise_within_content(sent_message['message']['subject'], student_data)
                        sent_messages[i]['message']['body'] = anonymise_within_content(sent_message['message']['body'], student_data)
                    sent_messages[i]['target'] = anonymise_identifier(sent_message['target'])
            return json.dumps(sent_messages, default=str)
        else:
            abort(400)
    else:
        abort(403)

@bp.route('/<filter_uuid>/logs', methods=['GET'])
@login_required
def view_logs(filter_uuid):
    add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='logs')
    vars = {}
    vars['asset_name'] = 'filter'
    filter = Filter()
    if filter.load(filter_uuid) and filter.is_user_authorised():
        if len(filter.config['run_history']) == 0:
            flash("This filter has not yet completed running.", "warning")
        if filter.is_send_queue_active():
            flash("There is currently an active send queue.", "warning")
        vars['feedback_was_requested'] = False if filter.config['email']['feedback']['style'] == 'null' else True
        # quick stats
        vars['quick_stats'] = []
        count_recipients = filter.get_recipient_sent_count()
        vars['count_recipients'] = count_recipients
        count_opens = filter.get_recipient_open_count()
        vote_stats = filter.get_feedback_stats()
        ## student count
        main_table_uuid = filter.get_referenced_table_uuids()[0]
        table = Table()
        main_table_student_count = -1
        if table.load(main_table_uuid):
            main_table_student_count = len(table.get_all_students_sids())
        ## run history
        if filter.config['run_history']:
            runs = []
            for h in filter.config['run_history']:
                if isinstance(h['timestamp'], datetime):
                    ts = h['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                else:
                    try:
                        ts = parser.parse(h['timestamp'])
                        ts = ts.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        ts = str(h['timestamp'])
                runs.append('{} by {}'.format(ts, h.get('by', 'unknown')))
            vars['quick_stats'].append({
                'icon': 'clock',
                'html': 'Sent on ' + ', '.join(runs)
            })
        ## messages sent
        html = '{} messages sent'.format(count_recipients)
        if main_table_student_count > 0 and table._id is not None:
            html += ' ({:.{prec}f}% of {student_count} students currently in {table_name})'.format(
                (count_recipients / main_table_student_count) * 100,
                prec=1,
                student_count=main_table_student_count,
                table_name=table.get_full_name()
            )
        vars['quick_stats'].append({
            'icon': 'envelope',
            'html': html
        })
        ## messages opened
        vars['quick_stats'].append({
            'icon': 'envelope-open',
            'html': '{} ({:.{prec}f}%) recipients opened their message at least once'.format(
                count_opens,
                0 if count_recipients == 0 else (count_opens / count_recipients) * 100,
                prec=1
            )
        })
        ## feedback
        RECIPIENT_COUNT_THRESHOLD = 4
        if not vars['feedback_was_requested']:
            pass
        else:
            if count_recipients >= RECIPIENT_COUNT_THRESHOLD:
                # only show these stats if there exists a fair number of recipients
                votes_display = []
                for vote in vote_stats['votes']:
                    vote_count = vote['count']
                    vote_text = vote['vote']
                    vote_percent = vote_count / vote_stats['total_votes'] * 100 if vote_stats['total_votes'] > 0 else 0
                    votes_display.append('{}: {} ({:.{prec}f}%)'.format(
                        vote_text,
                        vote_count,
                        vote_percent,
                        prec=1
                    ))
                vars['quick_stats'].append({
                    'icon': 'poll',
                    'html': Markup('{} feedback votes ({} unique) received to prompt <em>{}</em>. '.format(
                        vote_stats['total_votes'],
                        vote_stats['unique_votes'],
                        filter.config['email']['feedback']['prompt']
                    ) + '; '.join(votes_display) if len(votes_display) else "No feedback recorded")
                })
                # feedback comments
                vars['comments_by_vote'] = vote_stats['comments_by_vote']
            else:
                vars['comments_by_vote'] = {}
                if vote_stats['total_votes'] > 0:
                    vars['feedback_explanation'] = f"Some feedback has been received but is not shown to protect the identity of students, since the number of recipients is less than {RECIPIENT_COUNT_THRESHOLD}."
        # per-student interactions
        vars['interaction_logs'] = filter.get_interaction_logs()
        vars['is_identity_anonymiser_active'] = is_identity_anonymiser_active()
        # actions for each student
        vars['interaction_record_actions'] = [
            {
                'target_fn': 'loadFilterSentMessage',
                'icon': 'envelope-open-text',
                'text': '',
                'tooltip': 'Load sent message'
            }
        ]
        vars['show_interaction_details'] = True
        # render
        return render_template('interactions-view-log.html', vars=vars, asset=filter)
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')
    

@bp.route('/preview_conditions', methods=['POST'])
@login_required
def preview_conditions():
    conditions = request.form.get('conditions')
    if not utils.is_json(conditions):
        abort(400)
    conditions = json.loads(conditions)
    cond = OpenConditions(conditions)
    results = cond.run_conditions(check_table_permissions=True, user_oid=get_auth_user_oid())
    return json.dumps(results)

@bp.route('/<filter_uuid>/convert/portal', methods=['GET'])
@login_required
def convert_to_portal(filter_uuid):
    from sres.portals import Portal
    add_access_event(asset_type='filter', asset_uuid=filter_uuid, action='convert_to_portal')
    filter = Filter()
    if is_user_administrator('filter') or is_user_administrator('super'):
        # check permissions
        if not filter.load(filter_uuid):
            flash("Sorry, this filter could not be loaded.", "danger")
            return render_template('denied.html')
        if not filter.is_user_authorised():
            flash("Sorry, you do not appear to have the right permissions to proceed.", "danger")
            return render_template('denied.html')
        # continue
        portal = Portal()
        if not portal.create():
            flash("Problem creating new portal.", "danger")
            return render_template('denied.html')
        if portal._id:
            portal.config['name'] = f"Portal created from filter {filter.config['name']}"
            portal.config['description'] = filter.config['description']
            portal.config['administrators'] = filter.config['administrators']
            portal.config['created'] = datetime.now()
            portal.config['modified'] = datetime.now()
            portal.config['workflow_state'] = 'active'
            # convert sections to panels
            portal.config['panels'] = []
            portal.config['panels'].append({
                'uuid': utils.create_uuid(),
                'show_when': 'always',
                'mode': 'read',
                'content': filter.config['email']['body_first'],
                'availability': 'available'
            })
            for section in filter.config['email']['sections']:
                panel = {
                    'uuid': utils.create_uuid(),
                    'show_when': section.get('show_when', 'always'),
                    'conditions': section.get('conditions', {}),
                    'mode': 'read',
                    'content': section.get('content', ''),
                    'availability': 'available'
                }
                portal.config['panels'].append(panel)
            portal.config['panels'].append({
                'uuid': utils.create_uuid(),
                'show_when': 'always',
                'mode': 'read',
                'content': filter.config['email']['body_last'],
                'availability': 'available'
            })
            # save
            if portal.update():
                flash(Markup("Portal created successfully from filter. <span class=\"fa fa-exclamation-triangle\"></span> You will need to set a few portal-specific settings and save this portal before it can be used, such as active dates."), "success")
                return redirect(url_for('portal.edit_portal', portal_uuid=portal.config['uuid']))
            else:
                flash("There was a problem creating a portal from the filter.", "danger")
                return redirect(url_for('filter.edit_filter', filter_uuid=filter.config['uuid']))
    else:
        flash("Sorry, only administrators are authorised to access this feature.", "warning")
        return render_template('denied.html')
