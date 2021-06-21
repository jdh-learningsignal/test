from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, current_app)
from dateutil import parser
from datetime import datetime
import json

from sres.users import usernames_to_oids
from sres.auth import login_required, is_user_administrator, get_auth_user, is_logged_in, get_auth_user_oid
from sres import utils
from sres.insights import Insight, _FORM_OPTIONS, list_authorised_insights, run_insight
from sres.columns import SYSTEM_COLUMNS, Column
from sres.tables import list_authorised_tables, Table, load_authorised_tables
from sres.access_logs import add_access_event

bp = Blueprint('insight', __name__, url_prefix='/insights')

@bp.route('/new', methods=['GET'])
@login_required
def new_insight():
    add_access_event(asset_type='insight', asset_uuid=None, action='new')
    vars = {}
    vars['mode'] = 'new'
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['authorised_tables'] = list_authorised_tables()
    vars['query_builder_filters'] = []
    vars['query_builder_rules'] = {}
    vars['referenced_table_uuids'] = []
    if is_user_administrator('list') or is_user_administrator('super'):
        insight = Insight()
        insight.config['administrators'] = [get_auth_user_oid()]
        insight.config['uuid'] = '__new__'
        vars['teacher_allocation_column'] = Column()
        vars['grouping_column'] = Column()
        vars['form_options'] = _FORM_OPTIONS
        return render_template('insight-edit.html', insight=insight, vars=vars)
    else:
        flash("Sorry, only administrators are authorised to access this feature.", "warning")
        return render_template('denied.html')

@bp.route('/<insight_uuid>/edit', methods=['GET', 'POST'])
@login_required
def edit_insight(insight_uuid):
    add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='edit')
    vars = {}
    insight = Insight()
    vars['mode'] = 'edit'
    vars['SYSTEM_COLUMNS'] = SYSTEM_COLUMNS
    vars['authorised_tables'] = list_authorised_tables()
    try:
        if request.method == 'POST' and request.form.get('action') == 'new':
            # adding a new insight
            if is_user_administrator('filter') or is_user_administrator('super'):
                pass
            else:
                flash("Sorry, only administrators are authorised to access this feature.", "warning")
                return render_template('denied.html')
        else:
            if not insight.load(insight_uuid):
                raise
            if not insight.is_user_authorised():
                raise
    except:
        raise
        flash("Sorry, this insight could not be loaded. You may not have the right permissions, or the insight may not exist.", "warning")
        return render_template('denied.html')
    # continue
    if request.method == 'GET':
        add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='view')
        # starting filters for queryBuilder
        vars['query_builder_filters'] = [] # TODO
        # teacher allocation column
        vars['teacher_allocation_column'] = Column()
        if insight.config['alert_config']['teacher_allocation_columnuuid']:
            vars['teacher_allocation_column'].load(insight.config['alert_config']['teacher_allocation_columnuuid'])
        # grouping column
        vars['grouping_column'] = Column()
        if insight.config['alert_config']['grouping_columnuuid']:
            vars['grouping_column'].load(insight.config['alert_config']['grouping_columnuuid'])
        # form options
        vars['form_options'] = _FORM_OPTIONS
        # referenced_table_uuids
        referenced_table_uuids = insight.get_referenced_table_uuids()
        #for referenced_table_uuid in referenced_table_uuids: # TODO if implementing custom conditions trigger
        #    table_temp = Table()
        #    if table_temp.load(referenced_table_uuid):
        #        select_array = table_temp.get_select_array(show_collapsed_multientry_option=True)
        #        for c in select_array:
        #            vars['query_builder_filters'].append({
        #                'id': c['value'],
        #                'label': c['full_display_text'],
        #                'type': 'string'
        #            })
        # referenced tables instances
        vars['referenced_table_uuids'] = referenced_table_uuids
        vars['referenced_tables_instances'] = []
        for referenced_table_uuid in referenced_table_uuids:
            table = Table()
            if table.load(referenced_table_uuid):
                vars['referenced_tables_instances'].append(table)
        # render
        return render_template('insight-edit.html', insight=insight, vars=vars)
    elif request.method == 'POST':
        if request.form['action'] == 'new':
            add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='create')
            if not insight.create():
                flash("Problem creating new insight.", "danger")
        elif request.form['action'] == 'edit':
            add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='update')
            if insight.load(insight_uuid) and insight.is_user_authorised():
                pass
            else:
                flash("Problem loading insight.", "danger")
        if insight._id:
            # general config
            insight.config['name'] = request.form.get('insight_name')
            insight.config['description'] = request.form.get('insight_description')
            insight.config['administrators'] = usernames_to_oids(request.form.getlist('authorised_administrators'))
            # alert
            insight.config['alert_frequency'] = int(request.form.get('alert_frequency')) or insight.config['alert_frequency']
            insight.config['alert_startfrom'] = parser.parse(request.form.get('alert_startfrom')) or insight.config['alert_startfrom']
            insight.config['alert_endby'] = parser.parse(request.form.get('alert_endby')) or insight.config['alert_endby']
            # alert_config
            insight.config['alert_config']['recipient_emails'] = request.form.get('alert_recipient_emails') or insight.config['alert_config']['recipient_emails']
            insight.config['alert_config']['teacher_allocation_columnuuid'] = request.form.get('alert_teacher_allocation_columnuuid', '') #or insight.config['alert_config']['teacher_allocation_columnuuid']
            insight.config['alert_config']['when_no_students_identified'] = request.form.get('alert_when_no_students_identified', insight.config['alert_config']['when_no_students_identified'])
            insight.config['alert_config']['grouping_columnuuid'] = request.form.get('alert_grouping_columnuuid', '') #or insight.config['alert_config']['grouping_columnuuid']
            insight.config['alert_config']['alert_starttime'] = request.form.get('alert_starttime') or insight.config['alert_config']['alert_starttime']
            insight.config['alert_config']['alert_interval'] = request.form.get('alert_interval') or insight.config['alert_config']['alert_interval']
            # trigger_config
            insight.config['trigger_config']['trigger_type'] = request.form.get('trigger_type') or insight.config['trigger_config']['trigger_type']
            insight.config['trigger_config']['trigger_config_quartiles_range'] = request.form.get('trigger_config_quartiles_range') or insight.config['trigger_config']['trigger_config_quartiles_range']
            insight.config['trigger_config']['trigger_config_quartiles_combiner'] = request.form.get('trigger_config_quartiles_combiner') or insight.config['trigger_config']['trigger_config_quartiles_combiner']
            insight.config['trigger_config']['trigger_config_matching_count_comparator'] = request.form.get('trigger_config_matching_count_comparator') or insight.config['trigger_config']['trigger_config_matching_count_comparator']
            insight.config['trigger_config']['trigger_config_matching_count'] = request.form.get('trigger_config_matching_count') or insight.config['trigger_config']['trigger_config_matching_count']
            insight.config['trigger_config']['trigger_config_matching_method'] = request.form.get('trigger_config_matching_method') or insight.config['trigger_config']['trigger_config_matching_method']
            insight.config['trigger_config']['trigger_config_matching_value'] = request.form.get('trigger_config_matching_value') or insight.config['trigger_config']['trigger_config_matching_value']
            insight.config['trigger_config']['trigger_config_trending_direction'] = request.form.get('trigger_config_trending_direction') or insight.config['trigger_config']['trigger_config_trending_direction']
            insight.config['trigger_config']['trigger_config_trending_data_conversion'] = request.form.get('trigger_config_trending_data_conversion') or insight.config['trigger_config']['trigger_config_trending_data_conversion']
            insight.config['trigger_config']['trigger_config_distance_value'] = request.form.get('trigger_config_distance_value') or insight.config['trigger_config']['trigger_config_distance_value']
            insight.config['trigger_config']['trigger_config_distance_direction'] = request.form.get('trigger_config_distance_direction') or insight.config['trigger_config']['trigger_config_distance_direction']
            insight.config['trigger_config']['trigger_config_distance_combiner'] = request.form.get('trigger_config_distance_combiner') or insight.config['trigger_config']['trigger_config_distance_combiner']
            insight.config['trigger_config']['trigger_config_ml_outliers_data_conversion_non_numerical'] = request.form.get('trigger_config_ml_outliers_data_conversion_non_numerical') or insight.config['trigger_config']['trigger_config_ml_outliers_data_conversion_non_numerical']
            insight.config['trigger_config']['trigger_config_select_columns'] = request.form.get('trigger_config_select_columns') or insight.config['trigger_config']['trigger_config_select_columns'] # a comma-separated list of column references
            insight.config['trigger_config']['trigger_config_columns_ignore_state'] = request.form.get('trigger_config_columns_ignore_state') or insight.config['trigger_config']['trigger_config_columns_ignore_state']
            # email
            insight.config['content_email_from_name'] = request.form.get('content_email_from_name') or insight.config['content_email_from_name']
            insight.config['content_email_from_address'] = request.form.get('content_email_from_address') or insight.config['content_email_from_address']
            # content
            insight.config['content_email_subject'] = request.form.get('content_email_subject') or insight.config['content_email_subject']
            insight.config['content_email_first'] = request.form.get('content_email_first') or insight.config['content_email_first']
            insight.config['content_per_student'] = request.form.get('content_per_student') or insight.config['content_per_student']
            insight.config['content_email_last'] = request.form.get('content_email_last') or insight.config['content_email_last']
            # content_config
            insight.config['content_config']['content_config_summary_totals'] = True if request.form.get('content_config_summary_totals') else False
            insight.config['content_config']['content_config_summary_logic'] = True if request.form.get('content_config_summary_logic') else False
            # housekeeping config
            insight.config['default_table_uuid'] = insight.get_referenced_table_uuids()[0]
            if request.form['action'] == 'new':
                insight.config['created'] = datetime.now()
            insight.config['workflow_state'] = 'active'
            insight.config['modified'] = datetime.now()
            # save
            if insight.update():
                flash("Insight successfully updated.", "success")
            else:
                flash("Unexpected error updating insight.", "danger")
            # schedule as necessary
            result = insight.schedule()
            for message in result['messages']:
                flash(message[0], message[1])
        # render
        return redirect(url_for('insight.edit_insight', insight_uuid=insight.config['uuid']))

@bp.route('/<insight_uuid>/clone', methods=['GET'])
@login_required
def clone_insight(insight_uuid):
    add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='clone')
    insight = Insight()
    cloned_insight = Insight()
    result = None
    if insight.load(insight_uuid) and insight.is_user_authorised():
        result = insight.clone()
        if result and cloned_insight.load(result):
            flash("Cloned successfully. You are now editing the clone.", "success")
        else:
            flash("Error while cloning.", "warning")
    else:
        flash("Insufficient permissions to read this insight.", "danger")
    if result:
        return redirect(url_for('insight.edit_insight', insight_uuid=cloned_insight.config['uuid']))
    else:
        return redirect(url_for('insight.edit_insight', insight_uuid=insight.config['uuid']))
    
@bp.route('/<insight_uuid>/delete', methods=['GET', 'DELETE'])
@login_required
def delete_insight(insight_uuid):
    add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='delete')
    insight = Insight()
    if insight.load(insight_uuid) and insight.is_user_authorised():
        if insight.delete():
            if request.method == 'GET':
                flash("Successfully deleted insight.", "success")
                return redirect(url_for('index.index'))
            elif request.method == 'DELETE':
                return 'OK'
        else:
            if request.method == 'GET':
                flash("Error deleting insight.", "warning")
                return redirect(url_for('insight.edit_filter', insight_uuid=insight.config['uuid']))
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
def list_insights():
    insights = list_authorised_insights()
    ret = [
        {
            'uuid': i['uuid'],
            'name': i['name'],
            'description': i['description'],
            'created': i['created'],
            'modified': i.get('modified'),
            'workflow_state': i['workflow_state'],
            'active_now': i['alert_startfrom'].date() <= datetime.now().date() and i['alert_endby'].date() >= datetime.now().date()
        }
        for i in insights
    ]
    return json.dumps(ret, default=str)

@bp.route('/<insight_uuid>/preview', methods=['GET'])
@login_required
def preview_insight(insight_uuid):
    add_access_event(asset_type='insight', asset_uuid=insight_uuid, action='preview')
    insight = Insight()
    vars = {}
    if insight.load(insight_uuid) and insight.is_user_authorised():
        # TODO
        run = True if request.args.get('run') else False
        res = run_insight(insight.config['uuid'], send_email=run, return_html=True)
        return render_template('insight-view.html', insight=insight, vars=vars, alerts=res['alerts'])
    else:
        flash("There appears to be a problem loading or authenticating access to this insight.", "danger")
        return render_template('denied.html')


