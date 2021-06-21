from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app, Markup, abort)
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
import re
import logging
import pandas
from copy import deepcopy
from urllib import parse

from sres.auth import login_required, is_user_administrator, get_auth_user, get_auth_user_oid
from sres.studentdata import StudentData
from sres.summaries import Summary, Representation, CALCULATION_MODES, PRESENTATION_MODES, list_summaries_for_table, CALCULATION_MODES_GLOBAL_EXTRA_CONFIG, PRESENTATION_MODES_GLOBAL_EXTRA_CONFIG
from sres.tables import list_authorised_tables, Table
from sres.access_logs import add_access_event
from sres.columns import SYSTEM_COLUMNS, Column
from sres import utils

bp = Blueprint('summary', __name__, url_prefix='/')

@bp.route('/tables/<table_uuid>/summaries', methods=['GET'])
@login_required
def view_summaries(table_uuid):
    vars = {
        'calculation_modes': CALCULATION_MODES,
        'presentation_modes': PRESENTATION_MODES,
        'calculation_modes_global_extra_config': CALCULATION_MODES_GLOBAL_EXTRA_CONFIG,
        'presentation_modes_global_extra_config': PRESENTATION_MODES_GLOBAL_EXTRA_CONFIG,
        'mode': 'table'
    }
    table = Table()
    if not table.load(table_uuid):
        flash("Cannot load specified list.", "danger")
        return render_template('denied.html')
    if not table.is_user_authorised():
        flash("Sorry, only administrators are authorised to access this feature.", "danger")
        return render_template('denied.html')
    vars['is_list_admin'] = table.is_user_authorised()
    vars['authorised_tables'] = [table.config]
    vars['table_uuid'] = table.config['uuid']
    vars['available_columns'] = table.get_select_array(show_collapsed_multientry_option=True)
    vars['all_columns'] = vars['available_columns']
    return render_template('summary-view.html', table=table, vars=vars)

@bp.route('/tables/<table_uuid>/columns/<column_uuid>/summaries', methods=['GET'])
@login_required
def view_summaries_for_column(table_uuid, column_uuid):
    vars = {
        'calculation_modes': CALCULATION_MODES,
        'presentation_modes': PRESENTATION_MODES,
        'calculation_modes_global_extra_config': CALCULATION_MODES_GLOBAL_EXTRA_CONFIG,
        'presentation_modes_global_extra_config': PRESENTATION_MODES_GLOBAL_EXTRA_CONFIG,
        'mode': 'column'
    }
    column = Column()
    if not column.load(column_uuid):
        flash("Cannot load specified column.", "danger")
        return render_template('denied.html')
    if not column.table.is_user_authorised(categories=['user', 'auditor', 'administrator']):
        flash("Sorry, you are not authorised to access this.", "danger")
        return render_template('denied.html')
    vars['is_list_admin'] = column.table.is_user_authorised()
    vars['authorised_tables'] = [column.table.config]
    vars['table_uuid'] = column.table.config['uuid']
    vars['available_columns'] = column.table.get_select_array(show_collapsed_multientry_option=True, only_column_uuids=[column.config['uuid']])
    vars['all_columns'] = column.table.get_select_array(show_collapsed_multientry_option=True)
    return render_template('summary-view.html', table=column.table, column=column, vars=vars)

@bp.route('/tables/<table_uuid>/summaries/list', methods=['GET'])
@login_required
def list_summaries(table_uuid):
    column_uuid = request.args.get('column_uuid')
    table = Table()
    # access and permission checks
    if not table.load(table_uuid):
        abort(400)
    if not table.is_user_authorised(categories=['user', 'auditor', 'administrator']):
        abort(403)
    if column_uuid is not None:
        column = Column()
        if not column.load(column_uuid):
            abort(400)
    # load up the relevant summaries
    summaries = list_summaries_for_table(table.config['uuid'], column_uuid)
    # add canonical reference string
    for i, summary in enumerate(summaries):
        summaries[i]['canonical_reference'] = f"$SMY_{summary['uuid']}$"
    # return
    return json.dumps({
        'summaries': summaries
    }, default=str)

@bp.route('/tables/<table_uuid>/summaries/<summary_uuid>', methods=['GET', 'PUT', 'POST', 'DELETE'])
@login_required
def crud_summaries(table_uuid, summary_uuid):
    table = Table()
    column = Column()
    if not table.load(table_uuid):
        abort(400)
    if not table.is_user_authorised():
        abort(403)
    if request.method == 'GET':
        # get the summary
        summary = Summary()
        if not summary.load(summary_uuid):
            abort(400)
        return json.dumps({
            'success': True,
            'config': summary.config
        }, default=str)
    elif request.method in ['PUT', 'POST', 'DELETE']:
        # load config
        column_reference = request.form.get('col')
        calculation_mode = request.form.get('calc_mode')
        calculation_mode_extra_config = request.form.get('calc_mode_extra_config')
        presentation_mode = request.form.get('pres_mode')
        presentation_mode_extra_config = request.form.get('pres_mode_extra_config')
        grouping_mode = request.form.get('group_mode', 'disabled')
        grouping_comparison_mode = request.form.get('group_comparison_mode', 'disabled')
        grouping_column_reference = request.form.get('group_column_reference', '')
        name = request.form.get('name')
        description = request.form.get('description', '')
        # checks
        if not column_reference or not calculation_mode or not presentation_mode or not name:
            abort(400)
        # parse
        column_references = parse.unquote(column_reference).split(',')
        for column_reference in column_references:
            column = Column()
            if not column.load(column_reference):
                abort(404)
            if not column.table.is_user_authorised(categories=['user', 'auditor', 'administrator']):
                abort(403)
        summary = Summary(column)
        # act
        if request.method == 'POST' or request.method == 'PUT':
            if request.method == 'POST':
                # create new summary
                summary_uuid = summary.create()
            elif request.method == 'PUT':
                if not summary.load(summary_uuid):
                    abort(400)
            summary.config['table_uuid'] = table.config['uuid']
            summary.config['column_uuid'] = column.config['uuid']
            summary.config['name'] = name
            summary.config['description'] = description
            summary.config['created'] = datetime.now()
            summary.config['modified'] = datetime.now()
            summary.config['workflow_state'] = 'active'
            summary.config['representation_config']['calculation']['mode'] = calculation_mode
            summary.config['representation_config']['presentation']['mode'] = presentation_mode
            summary.config['representation_config']['grouping']['mode'] = grouping_mode
            summary.config['representation_config']['grouping']['comparison_mode'] = grouping_comparison_mode
            summary.config['representation_config']['grouping']['column_reference'] = grouping_column_reference
            summary.config['column_reference'] = column_references # save it in parsed list form
            # calculation extra config
            if 'extra_config' not in summary.config['representation_config']['calculation'].keys():
                summary.config['representation_config']['calculation']['extra_config'] = deepcopy(Representation.default_config['calculation']['extra_config'])
            if utils.is_json(calculation_mode_extra_config):
                calculation_mode_extra_config = json.loads(calculation_mode_extra_config)
            else:
                calculation_mode_extra_config = {}
            for config_id, v in Representation.default_config['calculation']['extra_config'].items():
                if config_id in calculation_mode_extra_config.keys():
                    summary.config['representation_config']['calculation']['extra_config'][config_id] = calculation_mode_extra_config[config_id]
            # presentation extra config
            if 'extra_config' not in summary.config['representation_config']['presentation'].keys():
                summary.config['representation_config']['presentation']['extra_config'] = deepcopy(Representation.default_config['presentation']['extra_config'])
            if utils.is_json(presentation_mode_extra_config):
                presentation_mode_extra_config = json.loads(presentation_mode_extra_config)
            else:
                presentation_mode_extra_config = {}
            for config_id, v in Representation.default_config['presentation']['extra_config'].items():
                if config_id in presentation_mode_extra_config.keys():
                    summary.config['representation_config']['presentation']['extra_config'][config_id] = presentation_mode_extra_config[config_id]
            # update db
            if not summary.update():
                abort(400)
            # return
            return json.dumps({
                'success': True,
                'config': summary.config,
                'canonical_reference': f"$SMY_{summary.config['uuid']}$"
            }, default=str)
        elif request.method == 'DELETE':
            if not summary.load(summary_uuid):
                abort(400)
            if summary.delete():
                return json.dumps({
                    'success': True
                })
            else:
                abort(400)

@bp.route('/tables/<table_uuid>/summaries/representation', methods=['GET'])
@login_required
def view_representation(table_uuid):
    vars = {}
    
    column_references = request.args.get('col', '')
    if not column_references:
        abort(400)
    # parse
    column_references = parse.unquote(column_references).split(',')
    for column_reference in column_references:
        column = Column()
        if not column.load(column_reference):
            abort(404)
        if not column.table.is_user_authorised(categories=['user', 'auditor', 'administrator']):
            # see if user is a student
            student_data = StudentData(column.table)
            if student_data.find_student(get_auth_user()):
                # OK
                pass
            else:
                abort(403)
    
    rep = Representation()
    
    rep.config['calculation']['mode'] = request.args.get('calc_mode', 'distribution')
    rep.config['presentation']['mode'] = request.args.get('pres_mode', 'chart_column')
    rep.config['grouping']['mode'] = request.args.get('group_mode', 'disabled')
    rep.config['grouping']['comparison_mode'] = request.args.get('group_comp_mode', 'disabled')
    rep.config['grouping']['column_reference'] = request.args.get('group_col', '')
    grouping_values = parse.unquote(request.args.get('group_vals', '')).split(',')
    if rep.config['grouping']['mode'] == 'enabled' and rep.config['grouping']['column_reference'] and ((len(grouping_values) == 0) or (len(grouping_values) == 1 and len(grouping_values[0]) == 0)):
        # default to everyone
        grouping_values = ['$ALL$']
    
    for config_id, v in rep.config['calculation']['extra_config'].items():
        if request.args.get(f'calc_mode_{config_id}', None) is not None:
            if 'extra_config' not in rep.config['calculation'].keys():
                rep.config['calculation']['extra_config'] = {}
            rep.config['calculation']['extra_config'][config_id] = request.args.get(f'calc_mode_{config_id}')
    for config_id, v in rep.config['presentation']['extra_config'].items():
        if request.args.get(f'pres_mode_{config_id}', None) is not None:
            if 'extra_config' not in rep.config['presentation'].keys():
                rep.config['presentation']['extra_config'] = {}
            rep.config['presentation']['extra_config'][config_id] = request.args.get(f'pres_mode_{config_id}')
    
    result = rep.calculate(column_references, grouping_values)
    
    return json.dumps({
        'data_array': result['data_array'],
        'data_text': result['data_text'],
        'y_axis_label': result['y_axis_label'],
        'x_axis_label': result.get('x_axis_label', ''),
        'column_name': result['column_friendly_names'],
        'possible_grouping_values': result['possible_grouping_values'],
        'grouping_column_reference': result['grouping_column_reference'],
        'grouping_column_name': result['grouping_column_name']
    }, default=str)
