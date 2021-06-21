from flask import g, request
from datetime import datetime, timedelta
import json
from bson import ObjectId
import logging
from natsort import natsorted, ns

from sres.db import _get_db
from sres.auth import get_auth_user, get_auth_user_oid
from sres import utils
from sres.tables import format_full_name as format_full_table_name

def add_access_event(asset_type=None, asset_uuid=None, action=None, related_asset_type=None, related_asset_uuid=None):
    """
        Adds a document to db.access_logs
        Returns the ObjectId of the inserted or updated document.
        
        asset_type (string)
        asset_uuid (string)
    """
    db = _get_db()
    username = get_auth_user()
    if username:
        # only log if user is logged in and known
        try:
            record = {
                'timestamp': datetime.now(),
                'username': username,
                'user_oid': get_auth_user_oid(username),
                'ip_address': utils.get_client_ip_address(),
                'asset_type': asset_type,
                'asset_uuid': asset_uuid,
                'related_asset_type': related_asset_type,
                'related_asset_uuid': related_asset_uuid,
                'request_endpoint': request.endpoint,
                'request_method': request.method,
                'action': action
            }
            result = db.access_logs.insert_one(record)
            return result.inserted_id
        except Exception as e:
            logging.error(e)
            return None
    else:
        return None

def get_recent_accesses(asset_type=None, actions=None, human_readable=True, methods=None, days=31):
    """Returns a list of recent entries from db.access_logs.
    
        asset_type (str) table|filter|portal|insight etc.
        actions (list of str or None) If None, returns any action.
        human_readable (boolean) Whether to return a representation that is human readable.
        methods (list of str, or None) HTTP methods e.g. GET PUT POST
        days (int) Number of days to consider as 'recent'
    """
    db = _get_db()
    username = get_auth_user()
    ret = {
        'success': False,
        'results': []
    }
    if username and asset_type in ['table', 'column', 'filter', 'portal', 'insight']:
        db_filter = {
            'username': username,
            'asset_type': asset_type
        }
        # actions
        if actions is not None and type(actions) is list:
            db_filter['action'] = {'$in': actions}
        elif actions is not None and type(actions) is str:
            db_filter['action'] = actions
        # methods
        if methods is not None and type(methods) is list:
            db_filter['request_method'] = {'$in': methods}
        # timestamp
        db_filter['timestamp'] = {'$gte': (datetime.now() - timedelta(days=days))}
        # read from DB
        cursor = db.access_logs.find(db_filter).sort([('timestamp', -1)])
        results = list(cursor)
        # find most common and most recent
        recents = {}
        for result in results:
            _asset_uuid = result.get('asset_uuid')
            if _asset_uuid not in recents.keys():
                recents[_asset_uuid] = {
                    'asset_uuid': _asset_uuid,
                    'asset_type': result.get('asset_type'),
                    'related_asset_uuid': result.get('related_asset_uuid'),
                    'related_asset_type': result.get('related_asset_type'),
                    'logged_records': 1,
                    'most_recent_timestamp': result.get('timestamp')
                }
            else:
                recents[_asset_uuid]['logged_records'] += 1
                if recents[_asset_uuid]['most_recent_timestamp'] < result.get('timestamp'):
                    recents[_asset_uuid]['most_recent_timestamp'] = result.get('timestamp')
        # interpret, if necessary
        if human_readable:
            if asset_type == 'table':
                # try direct database access for speed
                db_filter = {
                    'uuid': {
                        '$in': list(recents.keys())
                    }
                }
                tables = list(db.tables.find(db_filter))
                tables = { t['uuid']:t for t in tables }
                for _asset_uuid, recent in recents.items():
                    if _asset_uuid in tables.keys():
                        _display = format_full_table_name(tables[_asset_uuid])
                        recents[_asset_uuid]['title'] = _display['code_and_name']
                        recents[_asset_uuid]['subtitle'] = _display['year_and_semester']
                        recents[_asset_uuid]['workflow_state'] = tables[_asset_uuid].get('workflow_state', '')
            elif asset_type == 'column':
                # try direct database access for speed
                db_filter = {
                    'uuid': {
                        '$in': list(recents.keys())
                    }
                }
                columns = list(db.columns.find(db_filter))
                columns = { c['uuid']:c for c in columns }
                for _asset_uuid, recent in recents.items():
                    if _asset_uuid in columns.keys():
                        recents[_asset_uuid]['title'] = columns[_asset_uuid]['name']
                        recents[_asset_uuid]['subtitle'] = columns[_asset_uuid]['description']
                        recents[_asset_uuid]['workflow_state'] = columns[_asset_uuid].get('workflow_state', '')
            elif asset_type == 'filter':
                # try direct database access for speed
                db_filter = {
                    'uuid': {
                        '$in': list(recents.keys())
                    }
                }
                filters = list(db.filters.find(db_filter))
                filters = { f['uuid']:f for f in filters }
                for _asset_uuid, recent in recents.items():
                    if _asset_uuid in filters.keys():
                        recents[_asset_uuid]['title'] = filters[_asset_uuid]['name']
                        recents[_asset_uuid]['subtitle'] = filters[_asset_uuid]['description']
                        recents[_asset_uuid]['workflow_state'] = filters[_asset_uuid].get('workflow_state', '')
            elif asset_type == 'portal':
                # try direct database access for speed
                db_filter = {
                    'uuid': {
                        '$in': list(recents.keys())
                    }
                }
                portals = list(db.portals.find(db_filter))
                portals = { p['uuid']:p for p in portals }
                for _asset_uuid, recent in recents.items():
                    if _asset_uuid in portals.keys():
                        recents[_asset_uuid]['title'] = portals[_asset_uuid]['name']
                        recents[_asset_uuid]['subtitle'] = portals[_asset_uuid]['description']
                        recents[_asset_uuid]['workflow_state'] = portals[_asset_uuid].get('workflow_state', '')
        # order
        recents = list(recents.values())
        recents = natsorted(recents, key=lambda x: (x['logged_records'], x['most_recent_timestamp']), reverse=True)
        # return
        ret['results'] = recents
        ret['success'] = True
    return ret




