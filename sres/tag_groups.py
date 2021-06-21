from copy import deepcopy
from datetime import datetime, timedelta
import collections
import logging

from sres.db import _get_db
from sres import utils
from sres.auth import get_auth_user_oid

from bson import ObjectId

def get_all_tag_groups():
    db = _get_db()
    tag_groups_with_number_tags_in_group = []
    tag_groups = list(db.tag_groups.find())
    for tag_group in tag_groups:
        elem = {'name': tag_group['name'],
                'uuid': tag_group['uuid'],
                '_id':  tag_group['_id'],
                'number_of_tags_in_this_group': db.tags.find({'tag_group_id': ObjectId(tag_group['_id'])}).count()}
        tag_groups_with_number_tags_in_group.append(elem)
    return tag_groups_with_number_tags_in_group

class TagGroup:
    
    default_config = {
        'name': '',
        'uuid': None,
        'creator_user_oid': None
    }
    
    def __init__(self):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
    
    def create(self, name):
        self.config['uuid'] = utils.create_uuid()
        self.config['name'] = name
        self.config['creator_user_oid'] = get_auth_user_oid()        
        result = self.db.tag_groups.insert_one(self.config)
        return self.config['name']
        #if result.acknowledged and self.load(self.config['uuid']):
        #    return self.config['uuid']
        #else:
        #    return None

    def delete(self, tag_group_uuid=None):
        self.db.tag_groups.remove({'uuid': tag_group_uuid})

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
        
    def load_from_objectid(self, objectid=None):
        result = self.db.tag_groups.find_one({'_id': ObjectId(objectid)})
        self._load_db_result(result)
        return result

    def load_from_uuid(self, uuid=None):
        result = self.db.tag_groups.find_one({'uuid': uuid})
        self._load_db_result(result)
        return result
        
    def is_user_creator(self, uuid=None):
        if uuid is not None:
            self.load_from_uuid(uuid)
        else:
            # assume already loaded
            pass
        if self.config.get('creator_user_oid') is not None and self.config.get('creator_user_oid') == get_auth_user_oid():
            return True
        return False

    def get_tags(self, objectid=None):
        result = self.db.tags.find({'tag_group_id': ObjectId(objectid)})
        return list(result)

    def update_tag_group_name(self, tag_group_uuid=None, tag_group_name=None):
        self.db.tag_groups.update({'uuid': tag_group_uuid}, {'$set': {'name': tag_group_name}})
    
