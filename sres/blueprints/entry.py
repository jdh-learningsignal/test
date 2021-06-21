from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, abort, jsonify, current_app, send_from_directory, Markup, Response, stream_with_context)
import os
import json
import urllib
import logging

from sres.auth import login_required, get_auth_user_oid, is_user_administrator, get_auth_user
from sres.tables import Table
from sres.columns import Column, SYSTEM_COLUMNS
from sres.users import usernames_to_oids
from sres.studentdata import StudentData, get_random_identifier_for_table
from sres import utils
from sres.scanner import get_scanner_launch_uri, _is_ios
from sres.ldap import find_user_by_username
from sres.access_logs import add_access_event
from sres.anonymiser import is_identity_anonymiser_active, anonymise, anonymise_identifier

bp = Blueprint('entry', __name__, url_prefix='/entry')

@bp.route('/sw.js', methods=['GET'])
def add_value_service_worker():
    return send_from_directory('static/js/', 'sw.js')

@bp.route('/ping', methods=['GET'])
def ping():
    return 'OK'

@bp.route('/offline', methods=['GET'])
def offline_home():
    vars = {}
    return render_template('offline-home.html', table=Table(), column=Column(), vars=vars)
    pass

@bp.route('/preview/table/<table_uuid>/columns/<column_uuid>/single', methods=['GET'])
@login_required
def preview_add_value(table_uuid, column_uuid):
    add_access_event(asset_type='collective.column', asset_uuid=column_uuid, action='preview', related_asset_type='table', related_asset_uuid=table_uuid)
    column = Column()
    if column.load(column_uuid):
        if not column.is_user_authorised(authorised_roles=['user', 'administrator']):
            abort(403)
        class _DummyTable:
            def __init__(self):
                self._id = '_preview'
                self.config = {
                    'uuid': 'preview'
                }
            def get_full_name(self):
                return ''
        student_data = StudentData(table=_DummyTable(), preview_only=True)
        vars = {
            'auth_user': get_auth_user(),
            'unique_string': utils.create_uuid(),
            'rangeslider_config': { column.config['uuid']: column.get_slider_subfields_config() },
            'identifier': 'preview',
            'data_to_display': '',
            'quick_info': student_data.get_quick_info(column_uuid=column_uuid, type='single', preloaded_column=column, preloaded_student_data=student_data)['quick_info_html']
        }
        return render_template('add-value.html', table=_DummyTable(), column=column, vars=vars)
    else:
        abort(400)

@bp.route('/table/<table_uuid>/columns/<column_uuid>/single', methods=['GET'])
@login_required
def add_value(table_uuid, column_uuid, identifier=None):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='view', related_asset_type='table', related_asset_uuid=table_uuid)
    #table = Table()
    column = Column()
    vars = {
        'required_libraries': []
    }
    vars['auth_user'] = get_auth_user()
    vars['unique_string'] = utils.create_uuid()
    identifier = request.args.get('identifier')
    barcode = request.args.get('BARCODE')
    sdak = request.args.get('sdak') # student direct access key
    vars['sdak'] = sdak
    vars['sda_mode'] = 'single'
    vars['hide_navbar'] = True if request.args.get('show_nav') == '0' else False
    vars['show_nav'] = request.args.get('show_nav', '1')
    vars['hide_quickinfo'] = True if request.args.get('show_qi') == '0' else False
    vars['show_qi'] = request.args.get('show_qi', '1')
    vars['peer_data_entry'] = False
    vars['user_is_staff'] = False
    if True: #table.load(table_uuid):
        if column.load(column_uuid):
            student_data = StudentData(column.table)
            if column.is_user_authorised(authorised_roles=['user', 'administrator'], sdak=sdak):
                vars['user_is_table_admin'] = column.table.is_user_authorised()
                if not column.is_active() and not column.table.is_user_authorised():
                    flash('You are not authorised for access at this particular time.', 'danger')
                    return render_template('denied.html')
                vars['entry_page_navbar_brand_text'] = column.config['custom_options']['entry_page_navbar_brand_text']
                # slider subfield config
                vars['rangeslider_config'] = {
                    column.config['uuid']: column.get_slider_subfields_config()
                }
                if sdak:
                    # redirect according to workflow
                    if not identifier:
                        # student direct access initial login; determine action
                        if column.is_peer_data_entry_enabled() and column.is_writeable_by_students() and student_data.find_student(get_auth_user()) and column.is_student_direct_access_allowed():
                            # just continue??
                            vars['peer_data_entry'] = True
                            pass
                        elif column.is_student_direct_access_allowed():
                            # see if adding a student is allowed
                            if column.config['custom_options']['student_data_entry_if_student_unknown'] == 'addextra':
                                _ret = student_data.add_single_student_from_scratch(get_auth_user())
                                if _ret['success']:
                                    flash("Successfully added you to the list.", "success")
                                else:
                                    flash("We encountered some problems adding you to the list.", "danger")
                            # redirect with identifier
                            _identifier = None
                            _preview = ''
                            if student_data.find_student(get_auth_user()):
                                _identifier = student_data.config['sid']
                            elif column.table.is_user_authorised():
                                # user is an admin for the table, so show the preview mode
                                # find a random identifier
                                _identifier = get_random_identifier_for_table(column.table.config['uuid'])
                                if _identifier is None:
                                    flash("Cannot preview; no students found in associated list.", "danger")
                                    return render_template('denied.html')
                                _preview = '1'
                            else:
                                flash("Cannot find {}.".format(get_auth_user()), "danger")
                                return render_template('denied.html')
                            # perform the redirection
                            if _identifier:
                                return redirect(url_for('entry.add_value', 
                                    table_uuid=column.table.config['uuid'], 
                                    column_uuid=column.config['uuid'],
                                    sdak=sdak,
                                    show_nav=request.args.get('show_nav', 1),
                                    show_qi=request.args.get('show_qi', 1),
                                    identifier=_identifier,
                                    preview=_preview,
                                    _external=True
                                ))
                        else:
                            flash("Sorry, there is a configuration error.", "danger")
                            if not column.is_peer_data_entry_enabled():
                                flash("Peer data entry is currently not enabled.", "danger")
                            if not column.is_writeable_by_students():
                                flash("This is currently not writeable by students.", "danger")
                            if not column.is_student_direct_access_allowed():
                                flash("Direct access is not enabled for this method.", "danger")
                            return render_template('denied.html')
                    else:
                        # student direct access mode; identifier supplied
                        if column.table.is_user_authorised():
                            # previewing SDA as a table admin
                            vars['previewing'] = True
                            vars['previewing_sda_allowed'] = column.is_user_authorised_for_sda(sdak, 'single', override_user_identifier=identifier)
                        else:
                            student_data_a = StudentData(column.table)
                            student_data_b = StudentData(column.table)
                            if student_data_a.find_student(get_auth_user()) and student_data_b.find_student(identifier):
                                if column.is_peer_data_entry_enabled() and column.is_writeable_by_students():
                                    # peer entry mode
                                    vars['peer_data_entry'] = True
                                    # check allowed
                                    if not column.is_peer_data_entry_allowed(student_data_a, student_data_b):
                                        flash("Unauthorised - you are not authorised to access this student.", "danger")
                                        return render_template('denied.html')
                                else:
                                    #print('xx', student_data_a.config, student_data_b.config)
                                    if student_data_a._id != student_data_b._id:
                                        flash("Unauthorised - requestor is not the same as target.", "danger")
                                        return render_template('denied.html')
                else:
                    vars['user_is_staff'] = True
                # make scan_url
                scan_url_kwargs = {
                    'table_uuid': column.table.config['uuid'],
                    'column_uuid': column.config['uuid'],
                    '_external': True
                }
                if not _is_ios():
                    scan_url_kwargs['identifier'] = '{CODE}'
                if sdak:
                    scan_url_kwargs['sdak'] = sdak
                vars['scan_url'] = get_scanner_launch_uri(
                    urllib.parse.unquote(url_for('entry.add_value', **scan_url_kwargs))
                )
                # pre-process multientry option labels for safe
                utils.mark_multientry_labels_as_safe(column)
                # continue
                if barcode or identifier:
                    if student_data.find_student(barcode or identifier):
                        vars['identifier'] = student_data.config['sid']
                        # load existing data, or not
                        _existing_data = student_data.get_data_for_entry(column)
                        vars['data_to_display'] = _existing_data['data']
                        vars['multiple_reports_meta'] = {
                            'index': _existing_data['report_index'],
                            'count': _existing_data['report_available_number_count']
                        }
                        vars['load_existing_data_mode'] = column.config['custom_options']['load_existing_data']
                        vars['multiple_report_mode_enabled'] = column.has_multiple_report_mode_enabled()
                        # quickinfo
                        _quick_info = student_data.get_quick_info(
                            column_uuid=column_uuid,
                            type='single',
                            preloaded_column=column,
                            preloaded_student_data=student_data
                        )
                        vars['quick_info'] = _quick_info['quick_info_html']
                        vars['quick_info_scripts'] = _quick_info['quick_info_scripts']
                        if 'DataTable' in vars['quick_info_scripts']:
                            vars['required_libraries'].append('datatables')
                            if 'sres-multiple-reports-table-td-wrap' in vars['quick_info_scripts']:
                                vars['required_libraries'].append('sres.multiple-reports-table')
                            if 'buttons' in vars['quick_info_scripts']:
                                vars['required_libraries'].append('datatables.buttons')
                                if 'print' in vars['quick_info_scripts']:
                                    vars['required_libraries'].append('datatables.buttons.print')
                                if 'excelHtml5' in vars['quick_info_scripts']:
                                    vars['required_libraries'].append('datatables.buttons.excelHtml5')
                        # render
                        return render_template('add-value.html', table=column.table, column=column, vars=vars)
                    else:
                        flash('Identifier {} could not be found.'.format(identifier), 'warning')
                        pass
                else:
                    # no identifier specified, continue to seek identifier
                    vars['no_identifier'] = True
                    return render_template('add-value.html', table=column.table, column=column, vars=vars)
            elif sdak and column.is_student_direct_access_allowed() and column.config['custom_options']['student_data_entry_if_student_unknown'] == 'addextra' and not student_data.find_student(get_auth_user()):
                # check if we should be adding the user as a new student in the list
                _result = student_data.add_single_student_from_scratch(get_auth_user())
                if _result['success']:
                    return redirect(url_for('entry.add_value', 
                        table_uuid=column.table.config['uuid'], 
                        column_uuid=column.config['uuid'],
                        sdak=sdak,
                        _external=True
                    ))
                else:
                    flash("Cannot find {}, and could not add.".format(get_auth_user()), "danger")
                    return render_template('denied.html')
            else:
                if student_data.find_student(get_auth_user()):
                    flash('Sorry, you are not authorised to access this. This page is for staff only. If your teacher has given you a link to this page, please ask them to provide you with the link for students.', 'danger')
                else:
                    flash('Sorry, you are not authorised to access this.', 'danger')
        else:
            flash('Error loading specified column.', 'warning')
    else:
        flash('Error loading specified table.', 'warning')
    return render_template('denied.html')

@bp.route('/table/<table_uuid>/columns/<column_uuid>/roll', methods=['GET'])
@login_required
def add_value_roll(table_uuid, column_uuid):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='view', related_asset_type='table', related_asset_uuid=table_uuid)
    table = Table()
    column = Column()
    vars = {}
    vars['auth_user'] = get_auth_user()
    vars['unique_string'] = utils.create_uuid()
    sdak = request.args.get('sdak', '') # student direct access key
    vars['sdak'] = sdak
    vars['sda_mode'] = 'roll'
    vars['hide_navbar'] = True if request.args.get('show_nav') == '0' else False
    vars['show_nav'] = request.args.get('show_nav', '1')
    vars['user_is_staff'] = False
    identifier = request.args.get('identifier')
    if table.load(table_uuid):
        if column.load(column_uuid):
            student_data = StudentData(table)
            if column.is_user_authorised(authorised_roles=['user', 'administrator'], sdak=sdak, sda_mode='roll'):
                if not column.is_active() and not column.table.is_user_authorised():
                    flash('You are not authorised for access at this particular time.', 'danger')
                    return render_template('denied.html')
                # prepare vars
                vars['truncate_data_display_after'] = 20
                vars['entry_page_navbar_brand_text'] = column.config['custom_options']['entry_page_navbar_brand_text']
                vars['rollview_pagination_page_length_default'] = column.config['custom_options']['rollview_pagination_page_length_default']
                # permission vars
                vars['user_is_staff'] = column.is_user_authorised(authorised_roles=['user', 'administrator'])
                vars['user_is_table_admin'] = table.is_user_authorised()
                # groups
                vars['shown_groups'] = []
                vars['shown_groups_friendly'] = []
                shown_groups = request.args.get('group')
                if shown_groups and utils.is_json(shown_groups):
                    shown_groups = json.loads(shown_groups)
                    if not isinstance(shown_groups, list):
                        shown_groups = []
                    for shown_group in shown_groups:
                        if shown_group == '':
                            vars['shown_groups_friendly'].append('Everyone')
                        else:
                            vars['shown_groups_friendly'].append(shown_group)
                vars['shown_groups'] = shown_groups if shown_groups is not None else []
                vars['show_everyone'] = False
                if (isinstance(shown_groups, list) and '' in shown_groups) or shown_groups is None or len(shown_groups) == 0:
                    vars['show_everyone'] = True
                # permissions
                vars['is_list_admin'] = table.is_user_authorised()
                vars['authorised_to_edit'] = True # if we have made it here then authorisation is assumed
                # quickinfo
                vars['show_quickinfo_template'] = column.config['custom_options']['quickinfo_rollview']
                vars['show_quickinfo'] = False if vars['show_quickinfo_template'] == '' else True
                show_quickinfo_priority = column.config['custom_options']['quickinfo_rollview_priority']
                vars['show_quickinfo_priority'] = 50 if show_quickinfo_priority == '' or not utils.is_number(show_quickinfo_priority) else show_quickinfo_priority
                vars['show_quickinfo_header'] = column.config['custom_options']['quickinfo_rollview_header']
                # popout mode
                vars['editor_mode_popout'] = True if column.config['custom_options']['rollview_popout_editor'] == 'popout' else False
                # data column display
                vars['data_column_class'] = 'never' if column.config['custom_options']['rollview_display_raw_data_column'] == 'false' else ''
                # hide sid?
                vars['show_sid_column'] = False if column.config['custom_options']['rollview_display_identifier_column'] == 'hide' else True
                # hide names?
                vars['show_name_columns'] = False if column.config['custom_options']['rollview_display_name_columns'] == 'hide' else True
                # show group columnn?
                vars['show_group_column'] = False if column.config['custom_options']['rollview_display_group_column'] == 'hide' else True
                # grouping
                vars['grouping_column'] = column.config['custom_options']['grouping_column']
                restrict_by_username_column = column.config['custom_options']['restrict_by_username_column']
                vars['restrict_by_username_column'] = restrict_by_username_column
                all_student_data = []
                if vars['is_list_admin'] and not sdak:
                    vars['grouping_column_groups'] = column.get_grouping_column_unique_groups()
                    show_quickinfo = ('group' in request.args.keys() or len(vars['grouping_column_groups']) == 0)
                    all_student_data = table.load_all_students(
                        groups_to_check=vars['shown_groups'],
                        grouping_column=vars['grouping_column'],
                        target_column=column,
                        show_quickinfo=show_quickinfo,
                        show_everyone=vars['show_everyone']
                    )
                else:
                    _override_username = None
                    if vars['is_list_admin']:
                        vars['previewing'] = True
                        if identifier is None:
                            # get a random one
                            identifier = get_random_identifier_for_table(table.config['uuid'])
                        vars['identifier'] = identifier
                        _override_username = identifier
                        vars['previewing_sda_allowed'] = column.is_user_authorised_for_sda(sdak, 'roll', override_user_identifier=identifier)
                    vars['grouping_column_groups'] = column.get_grouping_column_unique_groups(
                        restrict_by_username_column=restrict_by_username_column,
                        sdak=sdak
                    )
                    show_quickinfo = ('group' in request.args.keys() or len(vars['grouping_column_groups']) == 0)
                    all_student_data = table.load_all_students(
                        restrict_by_username_column=restrict_by_username_column,
                        groups_to_check=vars['shown_groups'],
                        grouping_column=vars['grouping_column'],
                        target_column=column,
                        show_quickinfo=show_quickinfo,
                        show_everyone=vars['show_everyone'],
                        sdak=sdak,
                        override_user_identifier=_override_username
                    )
                # preprocess student_list to lighten load on jinja
                vars['student_list'] = []
                vars['student_groups'] = {}
                # slide subfield config
                vars['rangeslider_config'] = {
                    column.config['uuid']: column.get_slider_subfields_config()
                }
                # see if anonymisation is needed
                if is_identity_anonymiser_active():
                    for i, student_data in enumerate(all_student_data):
                        for k in ['display_sid', 'preferred_name', 'surname', 'email']:
                            if k in student_data.keys():
                                all_student_data[i][k] = anonymise(k, student_data[k])
                # put student data into var
                for student_data in all_student_data:
                    # determine if user has permission to access this student
                    user_allowed_access = True
                    if vars['restrict_by_username_column'] != '':
                        user_allowed_access = student_data['is_username_allowed_access']
                    if user_allowed_access:
                        if vars['show_everyone'] or student_data['is_in_group']:
                            vars['student_list'].append(student_data)
                            vars['student_groups'][student_data['sid']] = student_data['groups']
                # rendering fix
                utils.mark_multientry_labels_as_safe(column)
                # render - stream or direct depending on numbers
                if len(vars['student_list']) < 50:
                    return render_template('add-value-roll.html', table=table, column=column, vars=vars)
                else:    
                    return Response(stream_with_context(stream_template('add-value-roll.html', table=table, column=column, vars=vars)))
            elif sdak and column.is_student_direct_access_allowed() and column.config['custom_options']['student_data_entry_if_student_unknown'] == 'addextra' and not student_data.find_student(get_auth_user()):
                # check if we should be adding the user as a new student in the list
                _result = student_data.add_single_student_from_scratch(get_auth_user())
                if _result['success']:
                    return redirect(url_for('entry.add_value_roll', 
                        table_uuid=table.config['uuid'], 
                        column_uuid=column.config['uuid'],
                        sdak=sdak,
                        show_nav=request.args.get('show_nav', 1),
                        _external=True
                    ))
                else:
                    flash("Cannot find {}, and could not add.".format(get_auth_user()), "danger")
                    return render_template('denied.html')
            else:
                if student_data.find_student(get_auth_user()):
                    flash('Sorry, you are not authorised to access this. This page is for staff only. If your teacher has given you a link to this page, please ask them to provide you with the link for students.', 'danger')
                else:
                    flash('Sorry, you are not authorised to access this.', 'danger')
        else:
            flash('Error loading specified column.', 'warning')
    else:
        flash('Error loading specified table.', 'warning')
    return render_template('denied.html')

@bp.route('/table/<table_uuid>/columns/<column_uuid>/bulk', methods=['GET'])
@login_required
def add_value_bulk(table_uuid, column_uuid):
    add_access_event(asset_type='column', asset_uuid=column_uuid, action='view', related_asset_type='table', related_asset_uuid=table_uuid)
    table = Table()
    vars = {}
    vars['unique_string'] = utils.create_uuid()
    vars['auth_user'] = get_auth_user()
    vars['identifier'] = ''
    if table.load(table_uuid):
        column = Column(table)
        if column.load(column_uuid):
            if column.is_user_authorised(authorised_roles=['user', 'administrator']):
                if not column.is_active() and not column.table.is_user_authorised():
                    flash('You are not authorised for access at this particular time.', 'danger')
                    return render_template('denied.html')
                utils.mark_multientry_labels_as_safe(column)
                vars['entry_page_navbar_brand_text'] = column.config['custom_options']['entry_page_navbar_brand_text']
                # slide subfield config
                vars['rangeslider_config'] = {
                    column.config['uuid']: column.get_slider_subfields_config()
                }
                return render_template('add-value-bulk.html', vars=vars, table=table, column=column)
    flash("Unauthorised", "danger")
    return render_template('denied.html')

@bp.route('/table/<table_uuid>/columns/<column_uuid>/qi/<mode>', methods=['GET'])
@login_required
def get_quick_info(table_uuid, column_uuid, mode='roll'):
    table = Table()
    identifier = request.args.get('identifier')
    wait_for_data_stabilisation = True if request.args.get('wait', '') == 'wait' else False # e.g. wait for aggregations to complete
    sdak = request.args.get('sdak') # student direct access key
    if table.load(table_uuid):
        column = Column(table)
        if column.load(column_uuid):
            if column.is_user_authorised(authorised_roles=['user', 'administrator'], sdak=sdak):
                student_data = StudentData(table)
                if sdak:
                    identifier = get_auth_user()
                if student_data.find_student(identifiers=identifier):
                    if wait_for_data_stabilisation and student_data.still_waiting_for_aggregation(column):
                        # inform the client that data is still being processed...
                        return '{}', 202
                    return json.dumps({
                        'data': student_data.get_quick_info(
                            column_uuid=column_uuid,
                            type=mode,
                            preloaded_column=column
                        )['quick_info_html'],
                        'identifier': identifier
                    })
                else:
                    abort(404)
            else:
                abort(403)
        else:
            abort(400)
    else:
        abort(400)

@bp.route('/table/<table_uuid>/student/<mode>', methods=['GET'])
@login_required
def view_single_student(table_uuid, mode=''):
    """
        table_uuid (str)
        mode (str) identify|new|view
    """
    add_access_event(asset_type='table', asset_uuid=table_uuid, action='view_single_student', related_asset_type='table', related_asset_uuid=table_uuid)
    vars = {}
    vars['unique_string'] = utils.create_uuid()
    vars['auth_user'] = get_auth_user()
    vars['identifier_found'] = False
    table = Table()
    if table.load(table_uuid) and table.is_user_authorised():
        student_data = StudentData(table)
        vars['mode'] = mode
        identifier = request.args.get('identifier')
        barcode = request.args.get('BARCODE')
        if mode == 'view' and (identifier or barcode):
            vars['identifier'] = barcode or identifier
            vars['identifier_found'] = student_data.find_student(vars['identifier'])
        vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
        vars['is_identity_anonymiser_active'] = is_identity_anonymiser_active()
        columns = table.load_all_columns()
        column = { # dummy
            'config': {
                'uuid': '__column_uuid__'
            }
        }
        if _is_ios():
            vars['scan_url'] = get_scanner_launch_uri(
                urllib.parse.unquote(url_for('entry.view_single_student', table_uuid=table.config['uuid'], mode='view', _external=True))
            )
        else:
            vars['scan_url'] = get_scanner_launch_uri(
                urllib.parse.unquote(url_for('entry.view_single_student', table_uuid=table.config['uuid'], mode='view', identifier='{CODE}', _external=True))
            )
        # slide subfield config
        vars['rangeslider_config'] = {}
        for column_uuid, column in columns.items():
            vars['rangeslider_config'][column_uuid] = column.get_slider_subfields_config()
        # render
        return render_template('add-value-person.html', vars=vars, table=table, columns=columns, column=column, student_data=student_data)
    else:
        flash("You are not authorised to complete this action.", "danger")
        return render_template('denied.html')

def stream_template(template_name, **context):
    current_app.update_template_context(context)
    t = current_app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    rv.enable_buffering(5)
    return rv
