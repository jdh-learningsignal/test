from copy import deepcopy
from datetime import datetime, timedelta
import logging
import collections

from sres.db import _get_db
from sres import utils
from sres.auth import get_auth_user_oid

from bson import ObjectId

def get_all_tags():
    db = _get_db()
    return list(db.tags.find())

def get_these_tags(tag_ids):
    db = _get_db()
    return list(db.tags.find({'_id': {'$in': tag_ids}}).sort([('name', 1)]))

def get_all_tags_except_these(tags_already_used):
    db = _get_db()
    return list(db.tags.find({'_id': {'$nin': tags_already_used}}).sort([('name', 1)]))

class Tag:
    
    default_config = {
        'name': '',
        'uuid': None,
        'creator_user_oid': None,
        'tag_group_id': None
    }
    
    def __init__(self):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
    
    def create(self, name, tag_group_objectid=None):
        self.config['uuid'] = utils.create_uuid()
        self.config['name'] = name
        self.config['creator_user_oid'] = get_auth_user_oid()
        if tag_group_objectid != '':
            self.config['tag_group_id'] = ObjectId(tag_group_objectid)
        result = self.db.tags.insert_one(self.config)          
        return self.config['name']

    def load(self, tag_uuid=None):
        result = self.db.tags.find_one({'uuid': tag_uuid})
        self._load_db_result(result)
        return result
    
    def _load_db_result(self, db_result):
        if db_result:
            # load a db result into self.config
            for key, value in self.default_config.items():
                try:
                    if isinstance(self.config[key], collections.Mapping):
                        # is dict-type so try and merge
                        self.config[key] = {**value, **db_result[key]}
                    else:
                        self.config[key] = db_result[key]
                except:
                    self.config[key] = value
            self._id = db_result.get('_id')

    def delete(self, tag_uuid=None):
        self.db.tags.remove({'uuid': tag_uuid})

    def is_user_creator(self, tag_uuid=None):
        if tag_uuid is not None:
            self.load(tag_uuid)
        else:
            # assume already loaded
            pass
        if self.config.get('creator_user_oid') is not None and self.config.get('creator_user_oid') == get_auth_user_oid():
            return True
        return False
        
    def num_columns_with_this_tag(self, tag_uuid=None):
        return self.db.columns.find({'tags': {'$all': [ObjectId(tag_uuid)]}}).count()

    def update_tag_name(self, tag_uuid=None, tag_name=None):
        self.db.tags.update({'uuid': tag_uuid}, {'$set': {'name': tag_name}})

    def update_tag_group(self, tag_group_objectid=None, tag_uuid=None):
        if tag_group_objectid == '':
            self.db.tags.update({'uuid': tag_uuid}, {'$unset': {'tag_group_id':1}})
        else:
            self.db.tags.update({'uuid': tag_uuid}, {'$set': {'tag_group_id': ObjectId(tag_group_objectid)}})
