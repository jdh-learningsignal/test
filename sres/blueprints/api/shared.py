from flask import abort, jsonify
import logging
import json

def api_abort(status_code, messages):
    if type(messages) is str:
        messages = [ messages ]
    resp = jsonify({
        'messages': messages,
        'status_code': status_code
    })
    resp.status_code = status_code
    return resp

def parse_unknown_list(unknown_list):
    if type(unknown_list) is list:
        return unknown_list
    elif type(unknown_list) is str:
        try:
            return json.loads(unknown_list)
        except:
            logging.error(f'Could not parse unknown_list {unknown_list}')
            return []


