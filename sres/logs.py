from flask import g, request
from datetime import datetime, timedelta
import json
from bson import ObjectId
import logging

from sres.db import _get_db
from sres import utils

def log_message_send(target, contact_type, message, source_asset_type, source_asset_uuid, log_uuid=None, identifier=None):
    """
        Records a message send event in db.message_send_logs
        Returns string log_uuid if insert acknowledged, otherwise None.
        
        target (string) Recipient identifier e.g. email address
        contact_type (string) mode of contact e.g. email|sms
        message (dict) describing the message
        source_asset_type (string) source of the message e.g. filter
        source_asset_uuid (string uuid) uuid of the source e.g. a filter_uuid
        log_uuid (string uuid)
        identifier (string) usually SID
    """
    db = _get_db()
    # create a log uuid if not provided
    if log_uuid is None:
        log_uuid = utils.create_uuid(sep='-')
    # parse
    record = {
        'target': target,
        'type': contact_type,
        'message': message,
        'source_asset_type': source_asset_type,
        'source_asset_uuid': source_asset_uuid,
        'uuid': log_uuid,
        'sent': datetime.now(),
        'identifier': str(identifier)
    }
    # write to db
    result = db.message_send_logs.insert_one(record)
    if result.acknowledged:
        return log_uuid
    else:
        return None

def get_send_logs(source_asset_type=None, source_asset_uuid=None, targets=None, log_uuid=None, from_date=None, to_date=None):
    """
        Returns a list of dicts, direct from db.message_send_logs.
        
        source_asset_type (string)
        source_asset_uuid (string uuid)
        targets (list of strings)
        log_uuid (string uuid)
        from_date (datetime)
        to_date (datetime)
    """
    db = _get_db()
    filter = {}
    if source_asset_type:
        filter['source_asset_type'] = source_asset_type
    if source_asset_uuid:
        filter['source_asset_uuid'] = source_asset_uuid
    if targets:
        filter['target'] = {'$in': targets}
    if log_uuid:
        filter['uuid'] = log_uuid
    if from_date and to_date:
        filter['sent'] = {
            '$gte': from_date,
            '$lte': to_date
        }
    results = db.message_send_logs.find(filter)
    return list(results)

def add_interaction_event(source_asset_type, source_asset_uuid, parent, action, data, target):
    """
        Adds a record to db.interactions
        
        source_asset_type (string)
        source_asset_uuid (string)
        parent (string)
        action (string)
        data (any) Will be json.dumps'ed if pymongo saving fails.
        target (string)
    """
    db = _get_db()
    record = {
        'timestamp': datetime.now(),
        'ip_address': utils.get_client_ip_address(),
        'source_asset_type': source_asset_type,
        'source_asset_uuid': source_asset_uuid,
        'parent': parent,
        'action': action,
        'data': data,
        'target': target
    }
    try:
        db.interaction_logs.insert_one(record)
    except: # TODO more selective except?
        record['data'] = json.dumps(record['data'])
        db.interaction_logs.insert_one(record)
    return None

def log_email_url_click(url, log_uuid):
    logs = get_send_logs(log_uuid=log_uuid)
    for log in logs:
        if log['source_asset_type'] == 'filter' and log['source_asset_uuid']:
            # log it
            add_interaction_event(
                source_asset_type='filter', 
                source_asset_uuid=log['source_asset_uuid'], 
                parent=log_uuid, 
                action='click', 
                data={
                    'url': url
                }, 
                target=log['target']
            )

def log_email_open(log_uuid):
    from sres.filters import Filter
    logs = get_send_logs(log_uuid=log_uuid)
    print('send logs', logs)
    for log in logs:
        if log['source_asset_type'] == 'filter' and log['source_asset_uuid']:
            # log it
            print('adding interaction event', log['source_asset_uuid'], log['target'])
            add_interaction_event(
                source_asset_type='filter', 
                source_asset_uuid=log['source_asset_uuid'], 
                parent=log_uuid, 
                action='open', 
                data={}, 
                target=log['target']
            )
            filter = Filter()
            if filter.load(log['source_asset_uuid']):
                # increment it
                filter.increment_counter(log_uuid)

def get_interaction_logs(source_asset_type, source_asset_uuid, all_targets=[], parent=None, log_uuid=None):
    db = _get_db()
    ret = {
        'records': {},
        'urls': []
    }
    records = db.interaction_logs.find(
        {
            'source_asset_type': source_asset_type,
            'source_asset_uuid': source_asset_uuid
        }
    )
    for record in records:
        if type(record['target']) is list:
            _record_targets, _topology = utils.flatten_list(record['target'])
        else:
            _record_targets = [record['target']]
        for _record_target in _record_targets:
            if _record_target not in ret['records'].keys():
                ret['records'][_record_target] = {
                    'loguuid': record['parent'],
                    'target': _record_target,
                    'opens': 0,
                    'clicks': {}
                }
            if record['action'] == 'open':
                ret['records'][_record_target]['opens'] += 1
            elif record['action'] == 'click':
                url = None
                if 'data' in record.keys() and record['data'] and 'url' in record['data'].keys():
                    url = record['data']['url']
                if url:
                    if url in ret['records'][_record_target]['clicks'].keys():
                        ret['records'][_record_target]['clicks'][url] += 1
                    else:
                        ret['records'][_record_target]['clicks'][url] = 1
                if url not in  ret['urls']:
                     ret['urls'].append(url)
    # fill for targets with no interaction logs
    for target in all_targets:
        if target not in ret['records'].keys():
            ret['records'][target] = {
                'target': target,
                'opens': 0,
                'clicks': { u: '' for u in ret['urls'] }
            }
    return ret

def add_feedback_event(source_asset_type=None, source_asset_uuid=None, parent=None, vote=None, data=None, target=None, _id=None):
    """
        Adds or updates a document to db.feedback_logs
        Returns the ObjectId of the inserted or updated document. Returns None if no update took place.
        
        source_asset_type (string)
        source_asset_uuid (string)
        parent (string)
        vote (string)
        data (any) Will be json.dumps'ed if pymongo saving fails.
        target (string)
        _id (ObjectId or str) Specified if we are updating an existing entry in db.feedback_logs
    """
    db = _get_db()
    record = {
        'timestamp': datetime.now(),
        'ip_address': utils.get_client_ip_address(),
    }
    if source_asset_type: record['source_asset_type'] = source_asset_type
    if source_asset_uuid: record['source_asset_uuid'] = source_asset_uuid
    if parent: record['parent'] = parent
    if vote: record['vote'] = vote
    if data: record['data'] = data
    if target: record['target'] = target
    if _id:
        if isinstance(_id, str):
            _id = ObjectId(_id)
        result = db.feedback_logs.update_one({'_id': _id}, {'$set': record}, upsert=True)
        return result.upserted_id
    else:
        result = db.feedback_logs.insert_one(record)
        return result.inserted_id

def get_feedback_logs(days=31):
    """A superadmin method. Retrieves all the available feedback logs."""
    db = _get_db()
    ret = {
        'success': False,
        'logs': []
    }
    records = db.feedback_logs.find(
        {
            'timestamp': {'$gte': (datetime.now() - timedelta(days=days))}
        }
    ).sort([('timestamp', -1)])
    records = list(records)
    ret['success'] = True
    ret['logs'] = records
    return ret

def get_feedback_stats(source_asset_type, source_asset_uuid, days=None):
    """Retrieve feedback events and statistics from db.feedback_logs
        
        source_asset_type (str)
        source_asset_uuid (str)
        days (int or None) Recency of feedback events to return
    """
    db = _get_db()
    ret = {
        'votes': [],
        'votes_keyed': {},
        'total_votes': 0,
        'total_votes_substantiated': 0,
        'unique_votes': 0,
        'comments_by_vote': {},
        'comments_most_recent': []
    }
    db_filter = {
        'source_asset_type': source_asset_type,
        'source_asset_uuid': source_asset_uuid
    }
    if days:
        db_filter['timestamp'] = {'$gte': (datetime.now() - timedelta(days=days))}
    records = db.feedback_logs.find(db_filter).sort([('timestamp', -1)])
    count_by_vote = {}
    voting_targets = []
    for record in records:
        ret['total_votes'] += 1
        voting_targets.append(record.get('target'))
        if record.get('vote'):
            if record['vote'] not in count_by_vote.keys():
                count_by_vote[record['vote']] = 1
                ret['comments_by_vote'][record['vote']] = []
            else:
                count_by_vote[record['vote']] += 1
        if 'data' in record.keys() and record['data'] and record['data'].get('comment'):
            ret['comments_by_vote'][record['vote']].append(record['data']['comment'])
            ret['total_votes_substantiated'] += 1
            ret['comments_most_recent'].append(record['data']['comment'])
    ret['unique_votes'] = len(set(voting_targets))
    ret['votes'] = [
        {
            'vote': v, # the vote option e.g. 'Yes', 'No'
            'count': e,
            'count_substantiated': len(ret['comments_by_vote'].get(v, []))
        }
        for v, e in count_by_vote.items()
    ]
    ret['votes_keyed'] = {
        v: {
            'vote': v, # the vote option e.g. 'Yes', 'No'
            'count': e,
            'count_substantiated': len(ret['comments_by_vote'].get(v, []))
        }
        for v, e in count_by_vote.items()
    }
    return ret

def get_latest_feedback_events(source_asset_type, source_asset_uuid, target=None, records_to_return=1):
    """Returns a list"""
    db = _get_db()
    filter = {
        'source_asset_type': source_asset_type,
        'source_asset_uuid': source_asset_uuid
    }
    if target is not None:
        filter['target'] = target
    events = list(db.feedback_logs.find(filter).sort([('timestamp', 1)]))
    #logging.debug(str(events))
    if len(events) == 0:
        return []
    if records_to_return == -1:
        return events
    elif records_to_return > 0:
        return events[0:records_to_return]
    else:
        return []
    
def get_latest_feedback_events_many(source_asset_uuids=None, source_asset_types=None, records_to_return=-1, days=31):
    """Returns a list of feedback events for the specified source asset uuid(s) and/or type(s).
        
        source_asset_uuids (list of str, or None)
        source_asset_types (list of str, or None)
        records_to_return (int) -1 for all records otherwise number of records
        days (int or None) Recency of feedback events to return
    """
    db = _get_db()
    db_filter = {}
    if source_asset_uuids is not None:
        db_filter['source_asset_uuid'] = {'$in': source_asset_uuids}
    if source_asset_types is not None:
        db_filter['source_asset_type'] = {'$in': source_asset_types}
    if days:
        db_filter['timestamp'] = {'$gte': (datetime.now() - timedelta(days=days))}
    events = list(db.feedback_logs.find(db_filter).sort([('timestamp', -1)]))
    # return
    if len(events) == 0:
        return []
    if records_to_return == -1:
        return events
    elif records_to_return > 0:
        return events[0:records_to_return]
    else:
        return []



