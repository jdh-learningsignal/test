from flask import request
from datetime import datetime, timedelta
import json
from bson import ObjectId
import logging
from natsort import natsorted, ns

from sres.db import _get_db
from sres.auth import get_auth_user, get_auth_user_oid
from sres import utils

def save_change_history(identifier, records=[], username=None, caller_override=''):
    """
        Saves one or many change history records.
        
        Each record {
            column_uuid (string)
            table_uuid (string)
            existing_data (string, or will be cast to a string)
            new_data (string, or will be cast to a string)
            report_number (None|int) Optional
            report_workflow_state (None|str) Optional; active|deleted
        }
    """
    db = _get_db()
    # collate the data to save
    histories = []
    # determine username
    if not username:
        username = get_auth_user()
    # determine caller
    try:
        caller = request.full_path
    except:
        caller = caller_override
    # parse records
    for record in records:
        history = {
            'old_value': str(record['existing_data']),
            'new_value': str(record['new_data']),
            'caller': caller,
            'timestamp': datetime.now(),
            'auth_user': username,
            'identifier': identifier,
            #'table': column_ids[column_uuid]['table'],
            #'column': column_ids[column_uuid]['_id'],
            'column_uuid': record['column_uuid'],
            'table_uuid': record['table_uuid']
        }
        if record.get('real_auth_user'):
            history['real_auth_user'] = record.get('real_auth_user')
        if record.get('report_number') is not None and utils.is_number(record.get('report_number')):
            history['report_number'] = str(record.get('report_number'))
            history['report_workflow_state'] = record.get('report_workflow_state', 'active')
        
        #print('a history in save_change_history')
        #print(str(history))
        
        histories.append(history)
    # save to db
    result = db.change_history.insert_many(histories)
    # return
    return result.acknowledged

def get_recent_change_histories_for_table(table_uuid, days=7):
    db = _get_db()
    _column_uuids = list(db.columns.find({'table_uuid': table_uuid}, ['uuid']))
    column_uuids = [ c['uuid'] for c in _column_uuids ]
    ch = get_change_history(
        column_uuids=column_uuids,
        only_after=(datetime.now() - timedelta(days=days))
    )
    return ch

def get_recently_updated_columns_for_table(table_uuid, days=10):
    db = _get_db()
    db_filter = {
        'table_uuid': table_uuid,
        'column_uuid': { '$regex': '^COL_' }
    }
    if days is not None:
        db_filter['timestamp'] = {'$gte': (datetime.now() - timedelta(days=days))}
    results = db.change_history.find(db_filter, {'column_uuid': 1, '_id': 0}).sort([('timestamp', -1)])
    results = list(results)
    results = list(set([ r['column_uuid'] for r in results ]))
    return results

def get_distinct_auth_users(column_uuids=None):
    """Returns the distinct ```auth_user```s
    column_uuids (list of strings) Must be supplied
    """
    db = _get_db()
    if column_uuids is None or not column_uuids:
        return []
    results = db.change_history.find(
        {
            'column_uuid': {'$in': column_uuids}
        }
    ).distinct('auth_user')
    return results

def get_change_history(column_uuids=None, max_rows=0, auth_users=None, only_after=None, only_before=None, sid=None, email=None, return_cursor=False, sids=None):
    """
        Returns a list of db.change_history documents for the specified column_uuids.
        If max_rows == 1, this still returns a single-element list.
        
        column_uuids (list of strings) Must be supplied
        max_rows (int)
        auth_users (list of string usernames)
        only_after (datetime|None)
        only_before (datetime|None)
        sid (str or None) Specify a filter for the identifier
        email (str or None) Specify a filter for the identifier
        return_cursor (boolean) If True, returns cursor instead of list
        sids (list or None) Specify a filter for a number of SID identifiers
    """
    db = _get_db()
    if column_uuids is None or not column_uuids:
        return []
    else:
        filters = [
            {
                'column_uuid': {'$in': column_uuids}
            }
        ]
        if sid is not None or email is not None:
            filters.append(
                {
                    'identifier': {'$in': [sid, email]}
                }
            )
        if sids is not None and type(sids) is list:
            filters.append(
                {
                    'identifier': { '$in': sids }
                }
            )
        # authuser filter
        if auth_users:
            filters.append({'auth_user': {'$in': auth_users}})
        # datetime filters
        if only_after is not None and isinstance(only_after, datetime):
            filters.append({'timestamp': {'$gte': only_after}})
        if only_before is not None and isinstance(only_before, datetime):
            filters.append({'timestamp': {'$lte': only_before}})
        # search
        results = db.change_history.find({'$and': filters}).sort([
            ('timestamp', -1)
        ])
        if results.count() > 0:
            if max_rows > 0:
                return list(results)[:max_rows]
            else:
                return results if return_cursor else list(results)
        else:
            return []

def revert_change_history(_id, column_uuid, student_data, on_behalf_of=False):
    """
        Applies a saved history. Expects that this instance has a student loaded already.
        
        _id (ObjectId) The _id of the db.change_history document to be restored
        column_uuid (string) The column to restore to
        student_data (StudentData)
    """
    db = _get_db()
    ret = {
        'success': False,
        'messages': []
    }
    # get current value
    results = db.change_history.find({
        'column_uuid': column_uuid,
        'identifier': {'$in': [student_data.config['sid'], student_data.config['email']]},
        '_id': ObjectId(_id)
    })
    # set again
    if results.count() == 1:
        results = list(results)
        res = student_data.set_data(
            column_uuid=column_uuid,
            data=results[0]['new_value'],
            commit_immediately=True,
            auth_user_override=results[0]['auth_user'] if on_behalf_of else None,
            trigger_apply_to_others=True
        )
        ret['success'] = res['success']
        ret['messages'].extend(res['messages'])
    else:
        ret['messages'].append(("Could not find unique history entry.", "warning"))
    return ret

