import json
import logging
import bleach
import pandas

from sres.auth import is_logged_in, get_auth_user, login_required, is_user_administrator, is_user_asset_administrator_anywhere
from sres.users import ADMIN_CATEGORIES
from sres.tables import list_authorised_tables, Table
from sres.columns import Column, SYSTEM_COLUMNS
from sres.filters import list_authorised_filters, Filter
from sres.portals import list_authorised_portals, Portal
from sres.insights import list_authorised_insights
from sres.access_logs import get_recent_accesses as get_recent_accesses_from_logs
from sres.change_history import get_recent_change_histories_for_table, get_recently_updated_columns_for_table
from sres.search import search_haystacks
from sres.logs import get_latest_feedback_events_many
from sres import utils
from sres.db import DbCookie

PINNED_ASSETS_DBC_KEY = 'sres.index.dashboard.pinned_assets'

def get_recent_feedback(days=31):

    ret = {
        'recents': []
    }
    
    my_filters = list_authorised_filters(only_where_user_is_admin=True)
    my_filters_uuids = [ f['uuid'] for f in my_filters ]
    my_portals = list_authorised_portals(only_where_user_is_admin=True)
    my_portals_uuids = [ p['uuid'] for p in my_portals ]
    
    recent_feedback_events = get_latest_feedback_events_many(
        source_asset_uuids=(my_filters_uuids + my_portals_uuids),
        days=days
    )
    
    assets_with_feedback = {}
    
    for recent_feedback_event in recent_feedback_events:
        asset_uuid = recent_feedback_event['source_asset_uuid']
        asset_type = recent_feedback_event['source_asset_type']
        if asset_uuid not in assets_with_feedback.keys():
            if asset_type == 'filter':
                asset = Filter()
            elif asset_type == 'portal':
                asset = Portal()
            if asset.load(asset_uuid):
                assets_with_feedback[asset_uuid] = {
                    'asset_instance': asset,
                    'asset_type': asset_type,
                    'asset_name': asset.config['name'],
                    'asset_description': asset.config['description']
                }
    
    feedback_for_assets = {}
    for asset_uuid, asset in assets_with_feedback.items():
        feedback_for_asset = asset['asset_instance'].get_feedback_stats(days=days)
        # get the positive feedback comments
        if feedback_for_asset.get('comments_by_vote', {}).get('Yes', []): # yes hard coding hmm TODO update based on valence
            ret['recents'].append(
                {
                    'type': 'positive_comments',
                    'asset_type': asset['asset_type'],
                    'asset_name': asset['asset_name'],
                    'asset_description': asset['asset_description'],
                    'asset_uuid': asset['asset_instance'].config['uuid'],
                    'valence': 1,
                    'days': days,
                    'recent_positive_feedback': feedback_for_asset.get('comments_by_vote', {}).get('Yes', [])
                }
            )
        # get any troublesome feedback votes
        total_votes = feedback_for_asset.get('total_votes')
        negative_votes = feedback_for_asset.get('votes_keyed', {}).get('No', {}).get('count', 0)
        if total_votes and negative_votes:
            if negative_votes / total_votes > 0.25:
                ret['recents'].append(
                    {
                        'type': 'many_negative_votes',
                        'asset_type': asset['asset_type'],
                        'asset_name': asset['asset_name'],
                        'asset_description': asset['asset_description'],
                        'asset_uuid': asset['asset_instance'].config['uuid'],
                        'valence': -1,
                        'days': days,
                        'percentage_negative_votes': int(negative_votes / total_votes * 100)
                    }
                )
    
    return ret

def get_recent_accesses(asset_type):
    ret = {
        'recents': []
    }
    if asset_type == 'table':
        ret['recents'] = get_recent_accesses_from_logs(asset_type=asset_type)['results']
        for i, recent_table in enumerate(ret['recents'][:4]):
            ch = get_recent_change_histories_for_table(table_uuid=recent_table['asset_uuid'])
            active_column_uuids = set([ x['column_uuid'] for x in ch ])
            ret['recents'][i]['active_columns_count'] = len(active_column_uuids)
            unique_identifiers = set([ x['identifier'] for x in ch ])
            ret['recents'][i]['unique_identifiers_with_change_history'] = len(unique_identifiers)
    elif asset_type == 'column':
        ret['recents'] = get_recent_accesses_from_logs(
            asset_type=asset_type,
            actions=['view', 'edit'],
            methods=['GET']
        )['results']
    elif asset_type == 'filter':
        ret['recents'] = get_recent_accesses_from_logs(
            asset_type=asset_type,
            actions=['view', 'edit', 'preview'],
            methods=['GET']
        )['results']
        for i, recent_filter in enumerate(ret['recents'][:4]):
            filter = Filter()
            if recent_filter['asset_uuid'] and filter.load(recent_filter['asset_uuid']):
                ret['recents'][i]['filter_sent'] = len(filter.config['run_history']) > 0
                ret['recents'][i]['count_opens'] = filter.get_recipient_open_count()
                ret['recents'][i]['count_recipients'] = filter.get_recipient_sent_count()
                ret['recents'][i]['percent_opens'] = int(ret['recents'][i]['count_opens'] / ret['recents'][i]['count_recipients'] * 100) if ret['recents'][i]['count_recipients'] else ''
                if ret['recents'][i]['count_recipients'] >= 10:
                    # only show these stats if there exists a fair number of recipients
                    vote_stats = filter.get_feedback_stats()
                    ret['recents'][i]['feedback_summary'] = f"{filter.config['email']['feedback']['prompt']} "
                    votes_display = []
                    for vote in vote_stats['votes']:
                        votes_display.append('{}: {} ({:.{prec}f}%)'.format(
                            vote['vote'],
                            vote['count'],
                            vote['count'] / vote_stats['total_votes'] * 100 if vote_stats['total_votes'] > 0 else 0,
                            prec=1
                        ))
                    if votes_display:
                        ret['recents'][i]['feedback_summary'] += ' '.join(votes_display)
                        ret['recents'][i]['feedback_recent_comments'] = vote_stats['comments_most_recent'][:3]
                    else:
                        # if no votes, then blank out feedback
                        ret['recents'][i]['feedback_summary'] = None
    elif asset_type == 'portal':
        ret['recents'] = get_recent_accesses_from_logs(
            asset_type=asset_type,
            actions=['view', 'edit', 'preview'],
            methods=['GET']
        )['results']
        for i, recent_portal in enumerate(ret['recents'][:4]):
            portal = Portal()
            if recent_portal['asset_uuid'] and portal.load(recent_portal['asset_uuid']):
                interaction_logs = portal.get_interaction_logs()
                ret['recents'][i]['availability'] = portal.is_portal_available()
                ret['recents'][i]['students_opened'] = len(interaction_logs['opened_by'])
                ret['recents'][i]['times_opened'] = interaction_logs['total_opens']
                vote_stats = portal.get_feedback_stats()
                ret['recents'][i]['feedback_summary'] = f"{portal.config['feedback']['prompt']} "
                votes_display = []
                for vote in vote_stats['votes']:
                    votes_display.append('{}: {} ({:.{prec}f}%)'.format(
                        vote['vote'],
                        vote['count'],
                        vote['count'] / vote_stats['total_votes'] * 100 if vote_stats['total_votes'] > 0 else 0,
                        prec=1
                    ))
                if votes_display:
                    ret['recents'][i]['feedback_summary'] += ' '.join(votes_display)
                    ret['recents'][i]['feedback_recent_comments'] = vote_stats['comments_most_recent'][:3]
                else:
                    # if no votes, then blank out feedback
                    ret['recents'][i]['feedback_summary'] = None
    elif asset_type == 'insight':
        # nothing to return yet really
        pass
    
    return ret

def pin_asset_to_dashboard(action, asset_type, asset_uuid):
    dbc = DbCookie()
    current_pins = get_pinned_dashboard_assets()
    ret = {
        'success': False,
        'action': None
    }
    if asset_uuid in current_pins.keys():
        if action == 'unpin':
            # delete
            current_pins.pop(asset_uuid, None)
            ret['action'] = 'deleted'
        elif action == 'pin':
            # move to front
            _current_pins = {}
            _current_pins[asset_uuid] = current_pins.pop(asset_uuid, None)
            for asset_uuid, current_pin in current_pins.items():
                _current_pins[asset_uuid] = current_pin
            current_pins = _current_pins
            ret['action'] = 'to_first'
    else:
        if action == 'pin':
            current_pins[asset_uuid] = {
                'asset_type': asset_type,
                'asset_uuid': asset_uuid
            }
            ret['action'] = 'pinned'
    ret['success'] = dbc.set(
        key=PINNED_ASSETS_DBC_KEY,
        value=json.dumps(current_pins)
    )
    return ret

def get_pinned_dashboard_assets(asset_type=None):
    dbc = DbCookie()
    current_pins = json.loads(dbc.get(
        key=PINNED_ASSETS_DBC_KEY,
        default='{}'
    ))
    if asset_type is not None:
        _current_pins = {}
        for asset_uuid, current_pin in current_pins.items():
            if current_pin.get('asset_type') == asset_type:
                _current_pins[asset_uuid] = current_pin
        current_pins = _current_pins
    return current_pins

def get_notable_columns():
    pinned_tables = get_pinned_dashboard_assets(asset_type='table')
    notable_columns_per_table = {}
    for table_uuid, pinned_table in pinned_tables.items():
        # get the columns most recently updated
        recently_updated_columns_uuids = get_recently_updated_columns_for_table(table_uuid)
        # instantiate and load table
        table = Table()
        if table.load(table_uuid):
            notable_columns_per_table[table_uuid] = {}
            # get the relevant data
            data = table.export_data_to_df(
                only_column_uuids=recently_updated_columns_uuids,
                return_just_df=True,
                do_not_rename_headers=True
            ).get('data', [])
            # make a dataframe
            df_data = pandas.DataFrame(data)
            headers = list(df_data.columns)
            # set a few things
            MIN_THRESHOLD = 0.01
            MAX_THRESHOLD = 0.33
            IGNORE_HEADERS = [ c['name'].lower() for c in SYSTEM_COLUMNS ]
            IGNORE_HEADERS.append('canvas_avatar')
            # iterate over the columns in the dataframe to determine any interesting circumstances
            notable_headers = []
            for header in headers:
                #logging.debug(f"HEADER: {header}")
                #logging.debug(df_data[header])
                # ignore some headers
                if header.lower() in IGNORE_HEADERS:
                    continue
                # quick and dirty detection of potentially divergent data
                value_counts = df_data[header].fillna('').astype(str).value_counts(normalize=True, sort=True, dropna=False)
                if len(value_counts) >= 2:
                    if value_counts[0] > (1.0 - MAX_THRESHOLD) and value_counts[0] < (1.0 - MIN_THRESHOLD):
                        # i.e. between 2/3 and all of the data are a single value
                        # i.e. there are between 0.33 and 0.01 of the data being something different to the majority
                        notable_headers.append(header)
                        column = Column(table)
                        if column.load(header):
                            notable_columns_per_table[table_uuid][column.column_reference] = {
                                'column': column,
                                'reason': 'most_frequent',
                                'meta': {
                                    'most_frequent_value': value_counts.index[0],
                                    'most_frequent_value_frequency': value_counts[0]
                                }
                            }
                # more intelligent detection based on numeric values
                dtype = df_data.dtypes[header]
                _series = pandas.to_numeric(df_data[header], errors='coerce')
                if not _series.isnull().all():
                    _series.dropna(inplace=True)
                    # perhaps try z scores
                    std_dev = _series.std(ddof=0)
                    mean = _series.mean()
                    _zscores = (_series - mean) / std_dev
                    count_lower_than_minus_1z = _zscores.le(-1.0).sum()
                    count_higher_than_plus_1z = _zscores.ge(1.0).sum()
                    if count_lower_than_minus_1z > 1 or count_higher_than_plus_1z > 1:
                        notable_headers.append(header)
                        column = Column(table)
                        if column.load(header):
                            if count_lower_than_minus_1z > 1:
                                notable_columns_per_table[table_uuid][column.column_reference] = {
                                    'column': column,
                                    'reason': 'lower_than_minus_1z',
                                    'meta': {
                                        'count': count_lower_than_minus_1z,
                                        'mean':  mean,
                                        'std_dev': std_dev
                                    }
                                }
                            if count_higher_than_plus_1z > 1:
                                notable_columns_per_table[table_uuid][column.column_reference] = {
                                    'column': column,
                                    'reason': 'higher_than_plus_1z',
                                    'meta': {
                                        'count': count_higher_than_plus_1z,
                                        'mean':  mean,
                                        'std_dev': std_dev
                                    }
                                }
    return notable_columns_per_table


