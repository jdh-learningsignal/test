from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, abort, current_app)
import json
import logging
import bleach

from sres.auth import is_logged_in, get_auth_user, login_required, is_user_administrator, is_user_asset_administrator_anywhere, is_user_list_auditor_anywhere
from sres.users import ADMIN_CATEGORIES
from sres.tables import list_authorised_tables, Table, format_full_name
from sres.columns import Column, SYSTEM_COLUMNS
from sres.filters import list_authorised_filters, Filter
from sres.portals import list_authorised_portals, Portal
from sres.insights import list_authorised_insights, Insight
from sres.access_logs import get_recent_accesses as get_recent_accesses_from_logs
from sres.change_history import get_recent_change_histories_for_table
from sres.search import search_haystacks
from sres.logs import get_latest_feedback_events_many
from sres import utils
from sres import dashboard_staff
from sres.anonymiser import is_identity_anonymiser_active

from sres.shorturl import shorten as _shorten_url

bp = Blueprint("index", __name__, url_prefix="/")

@bp.route("/", methods=["GET"])
@login_required
def index():
    vars = {
        'admin_levels': [],
        'tabs_meta': {
            'dashboard': {
                'menu_item': "Dashboard",
                'display': "Welcome to SRES",
                'hint': "An overview of engagement and relationships through SRES."
            },
            'lists': {
                'menu_item': "Lists",
                'display': "Lists",
                'hint': "Collect, curate, manipulate, and analyse student data."
            },
            'filters': {
                'menu_item': "Filters",
                'display': "Filters and messaging",
                'hint': "Create and send personalised messages to students."
            },
            'portals': {
                'menu_item': "Portals",
                'display': "Student portals",
                'hint': "Programmable web pages for students to see and enter personalised information."
            },
            'insights': {
                'menu_item': "Insights",
                'display': "Teacher insights",
                'hint': "Automated, scheduled highlights and alerts about your students."
            },
            'collective': {
                'menu_item': "Collective",
                'display': "The SRES Collective",
                'hint': "A place to share good and innovative practice by sharing columns, filters, and portals with other teachers."
            },
            'tags': {
                'menu_item': "Tags",
                'display': "Tags",
                'hint': "Tags let you associate data with metatags and aggregate student achievement across multiple columns."
            },
            'data_entry': {
                'menu_item': "Active data entry",
                'display': "Active data entry",
                'hint': "Enter data for students."
            },
            'search': {
                'menu_item': "Search SRES",
                'display': "Search SRES",
                'hint': "Search for anything or anyone within your SRES."
            },
            'superadmin': {
                'menu_item': "Superadmin tools",
                'display': "Tools for superadministrators",
                'hint': "Be careful here..."
            },
            'help': {
                'menu_item': "Help",
                'display': "Help with SRES",
                'hint': "Learn more about SRES and how to use it to engage and relate with your sudents."
            }
        },
        'search_sres_haystacks': [
            {
                'type': 'tables',
                'display_icon': 'table',
                'display': 'Lists',
                'checked_by_default': True,
                'hint': "Names and codes"
            },
            {
                'type': 'columns',
                'display_icon': 'columns',
                'display': 'Columns',
                'checked_by_default': True,
                'hint': "Names, descriptions, and settings"
            },
            {
                'type': 'filters',
                'display_icon': 'filter',
                'display': 'Filters',
                'checked_by_default': False,
                'hint': "Names, descriptions, and content"
            },
            {
                'type': 'portals',
                'display_icon': 'window-maximize',
                'display': 'Portals',
                'checked_by_default': False,
                'hint': "Names, descriptions, and content"
            },
            {
                'type': 'identifiers',
                'display_icon': 'user',
                'display': 'Students',
                'checked_by_default': False,
                'hint': "Search students by names and identifiers"
            },
            {
                'type': 'student_data',
                'display_icon': 'file',
                'display': 'Student data',
                'checked_by_default': False,
                'hint': "Search in student data (WARNING: This is a slow search that will take a while. We recommend switching off the other searches.)"
            }
        ]
    }
    for category, config in ADMIN_CATEGORIES.items():
        if is_user_administrator(category=category) or is_user_administrator(category='super'):
            vars['admin_levels'].append('_{}'.format(category))
    vars['is_list_admin_somewhere'] = is_user_asset_administrator_anywhere(asset_type='list')
    vars['is_list_auditor_somewhere'] = is_user_list_auditor_anywhere()
    vars['is_user_admin_somewhere'] = True if vars['admin_levels'] else False
    vars['help_html_page'] = current_app.config['SRES'].get('HELPBOX', {}).get('HTML_PAGE', None)
    
    return render_template('index-staff.html', vars=vars)

@bp.route("/shorten_url", methods=["POST"])
@login_required
def shorten_url():
    long_url = request.form.get('long_url')
    if long_url:
        result = _shorten_url(long_url)
        return json.dumps(result)
    else:
        abort(400)

@bp.route("/recents/feedback", methods=["GET"])
@login_required
def get_recent_feedback():
    ret = dashboard_staff.get_recent_feedback()
    return json.dumps(ret, default=str)

@bp.route("/recents/notables", methods=["GET"])
@login_required
def get_recent_notables():
    ret = {
        'notables': []
    }
    
    notable_columns_per_table = dashboard_staff.get_notable_columns()
    
    ignore_column_uuids_for_frequent_blanks = []
    
    for table_uuid, notable_columns in notable_columns_per_table.items():
        for column_reference, notable_column in notable_columns.items():
            
            _notable = {
                'table_uuid': table_uuid,
                'table_name': notable_column['column'].table.get_full_name(),
                'column_uuid': notable_column['column'].config['uuid'],
                'column_reference': column_reference,
                'column_name': notable_column['column'].get_friendly_name(show_table_info=False, get_text_only=True),
                'reason': notable_column['reason'],
                'meta': notable_column['meta']
            }
            
            # ignore some columns
            if 'tracking counter for' in _notable['column_name'].lower():
                continue
            
            initial_filter_config = ''
            
            if _notable['reason'] == 'most_frequent':
                if _notable['meta']['most_frequent_value'] == '':
                    if notable_column['column'].config['uuid'] in ignore_column_uuids_for_frequent_blanks:
                        continue
                    pc = int((_notable['meta']['most_frequent_value_frequency']) * 100)
                    _notable['explanation'] = f"Data is missing for {pc}% of students in the column {_notable['column_name']}."
                    # add this column to the ignore pile so that, for example, subfields don't all get listed as being blank if the main column is blank!
                    ignore_column_uuids_for_frequent_blanks.append(notable_column['column'].config['uuid'])
                    # make buttons
                    initial_filter_config = utils.to_b64(json.dumps({
                        'conditions': {
                            'condition': 'AND',
                            'rules': [
                                {
                                    'id': column_reference,
                                    'field': column_reference,
                                    'type': 'string',
                                    'input': 'text',
                                    'operator': 'is_empty',
                                    'value': ''
                                }
                            ],
                            'not': False,
                            'valid': True
                        }
                    }))
                else:
                    pc = int((1.0 - _notable['meta']['most_frequent_value_frequency']) * 100)
                    v = _notable['meta']['most_frequent_value']
                    _notable['explanation'] = f"{pc}% of students do not have the most common value which is {v} in the column {_notable['column_name']}."
                    # make buttons
                    initial_filter_config = utils.to_b64(json.dumps({
                        'conditions': {
                            'condition': 'AND',
                            'rules': [
                                {
                                    'id': column_reference,
                                    'field': column_reference,
                                    'type': 'string',
                                    'input': 'text',
                                    'operator': 'not_equal',
                                    'value': v
                                }
                            ],
                            'not': False,
                            'valid': True
                        }
                    }))
            elif _notable['reason'] == 'lower_than_minus_1z':
                n = _notable['meta']['count']
                mean = '{:.3f}'.format(_notable['meta']['mean'])
                _notable['explanation'] = f"There are {n} students who are more than 1 standard deviation below the mean {mean} in the column {_notable['column_name']}."
                # make buttons
                initial_filter_config = utils.to_b64(json.dumps({
                    'conditions': {
                        'condition': 'AND',
                        'rules': [
                            {
                                'id': column_reference,
                                'field': column_reference,
                                'type': 'string',
                                'input': 'text',
                                'operator': 'less_or_equal',
                                'value': _notable['meta']['mean'] - _notable['meta']['std_dev']
                            },
                            {
                                'id': column_reference,
                                'field': column_reference,
                                'type': 'string',
                                'input': 'text',
                                'operator': 'is_not_empty',
                                'value': ''
                            }
                        ],
                        'not': False,
                        'valid': True
                    }
                }))
            elif _notable['reason'] == 'higher_than_plus_1z':
                n = _notable['meta']['count']
                mean = '{:.3f}'.format(_notable['meta']['mean'])
                _notable['explanation'] = f"There are {n} students who are more than 1 standard deviation above the mean {mean} in the column {_notable['column_name']}."
                # make buttons
                initial_filter_config = utils.to_b64(json.dumps({
                    'conditions': {
                        'condition': 'AND',
                        'rules': [
                            {
                                'id': column_reference,
                                'field': column_reference,
                                'type': 'string',
                                'input': 'text',
                                'operator': 'greater_or_equal',
                                'value': _notable['meta']['mean'] + _notable['meta']['std_dev']
                            }
                        ],
                        'not': False,
                        'valid': True
                    }
                }))
            # add buttons
            if initial_filter_config:
                _notable['buttons'] = [
                    {
                        'url': url_for(
                                'filter.new_filter',
                                source_table_uuid=notable_column['column'].table.config['uuid'],
                                initial_config=initial_filter_config
                            ),
                        'linkText': 'Create a filter to send a message to these students',
                        'icon': 'filter'
                    },
                    {
                        'url': url_for('table.view_table', table_uuid=notable_column['column'].table.config['uuid']),
                        'linkText': 'View list',
                        'icon': 'table'
                    }
                ]
            ret['notables'].append(_notable)
    
    return json.dumps(ret, default=str)

@bp.route("/recents/accesses/<asset_type>", methods=["GET"])
@login_required
def get_recent_accesses(asset_type):
    ret = dashboard_staff.get_recent_accesses(asset_type)
    return json.dumps(ret, default=str)

@bp.route("/search_sres", methods=["GET"])
@login_required
def search_sres():
    if is_user_administrator('list') or is_user_administrator('filter') or is_user_administrator('super'):
        ret = {
            'results': [],
            'all_results_count': 0
        }
        term = request.args.get('term', '')
        term = bleach.clean(term)
        haystack_types = request.args.getlist('search_sres_haystack_types')
        
        results = search_haystacks(term, haystack_types)['results']
        ret['results'] = results[:110]
        ret['all_results_count'] = len(results)
        
        return json.dumps(ret, default=str)
    else:
        abort(403)

def _instantiate_asset_by_asset_type(asset_type):
    if asset_type == 'table':
        asset = Table()
    elif asset_type == 'column':
        asset = Column()
    elif asset_type == 'filter':
        asset = Filter()
    elif asset_type == 'portal':
        asset = Portal()
    elif asset_type == 'insight':
        asset = Insight()
    return asset

@bp.route("/dashboard/pin/<asset_type>/<asset_uuid>", methods=['POST', 'DELETE'])
@login_required
def pin_asset(asset_type, asset_uuid):
    # instantiate and check permissions
    asset = _instantiate_asset_by_asset_type(asset_type)
    if asset.load(asset_uuid) and asset.is_user_authorised():
        pass # all good
    else:
        abort(403)
    result = dashboard_staff.pin_asset_to_dashboard(
        action='pin' if request.method == 'POST' else 'unpin',
        asset_type=asset_type,
        asset_uuid=asset_uuid
    )
    return json.dumps(result, default=str)

@bp.route("/dashboard/pinned", methods=['GET'])
@login_required
def get_pinned_assets(asset_type=None):
    results = dashboard_staff.get_pinned_dashboard_assets(asset_type)
    ret = []
    for asset_uuid, result in results.items():
        # instantiate
        asset = _instantiate_asset_by_asset_type(result['asset_type'])
        # try and load
        if asset.load(asset_uuid) and asset.is_user_authorised():
            pass # all good
        else:
            continue
        # load details for return
        if result['asset_type'] == 'table':
            _full_name = format_full_name(asset.config)
            asset_name = _full_name['code_and_name']
            asset_description = _full_name['year_and_semester']
        elif result['asset_type'] in ['column', 'filter', 'portal', 'insight']:
            asset_name = asset.config['name']
            asset_description = asset.config['description']
        _asset_meta = {
            'asset_type': result['asset_type'],
            'asset_uuid': asset_uuid,
            'title': asset_name,
            'subtitle': asset_description
        }
        # additional details depending on asset type
        if result['asset_type'] == 'filter':
            _asset_meta['filter_sent'] = True if len(asset.config['run_history']) > 0 else False
        elif result['asset_type'] == 'portal':
            _asset_meta['times_opened'] = asset.get_interaction_logs().get('total_opens', 0)
            _asset_meta['availability'] = asset.is_portal_available()
        _asset_meta['workflow_state'] = asset.config.get('workflow_state', '')
        # add to return list
        ret.append(_asset_meta)
    return json.dumps({
        'pinned': ret
    }, default=str)

@bp.route("/anonymiser", methods=['GET', 'PUT'])
@login_required
def toggle_anonymiser():
    if request.method == 'PUT':
        session['identity_anonymiser_active'] = not is_identity_anonymiser_active()
    return json.dumps({
        'anonymiser_active': is_identity_anonymiser_active()
    })
    
