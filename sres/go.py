from flask import url_for

import re
import logging
from bson import ObjectId

from sres.db import _get_db

def make_go_url(original_url):
    return url_for('go.go_code', code=encode_go_url(original_url), _external=True)

def encode_go_url(url):
    db = _get_db()
    result = db.go_codes.update_one(
        { 'url': url }, 
        { '$set': { 'url': url } }, 
        upsert=True
    )
    if result.upserted_id is None:
        # already exists
        result = db.go_codes.find_one( { 'url': url } )
        if result:
            return result['_id']
    else:
        return str(result.upserted_id)
    
def decode_go_code(code):
    code = re.sub("[^A-Z0-9a-z_]", "", code)
    if code:
        try:
            db = _get_db()
            result = list(db.go_codes.find({'_id': ObjectId(code)}))
            if len(result) == 1:
                return result[0].get('url')
            elif len(result) == 0:
                logging.warning(f"No db match to code {code}")
                return None
            else:
                logging.warning(f"More than one match to code {code}")
                return None
        except:
            logging.warning(f"Could not decode go code {code}")
            return None
    else:
        return None
