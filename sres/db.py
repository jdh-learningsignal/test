from flask import g, current_app
from pymongo import MongoClient, ReadPreference
from datetime import datetime, timedelta
import os, sys
import logging

_DB_INDEXES = {
    'tables': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('code', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('workflow_state', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('semester', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('year', -1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('staff.administrators', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('staff.users', 1)
            ],
            'unique': False
        }
    ],
    'columns': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('table_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('aggregated_by', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('name', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('workflow_state', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('type', 1)
            ],
            'unique': False
        }
    ],
    'data': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('table_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('sid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('email', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('username', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('alternative_id1', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('alternative_id2', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('table_uuid', 1),
                ('sid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('status', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('table', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('surname', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('preferred_name', 1)
            ],
            'unique': False
        }
    ],
    'change_history': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('column_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('identifier', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('timestamp', -1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('table_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('auth_user', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('report_number', -1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('table_uuid', 1),
                ('column_uuid', 1),
                ('identifier', 1)
            ],
            'unique': False
        }
    ],
    'collective_assets': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('description', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('name', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('parent_collective_asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('shared_by', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('type', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('visibility', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('workflow_state', 1)
            ],
            'unique': False
        }
    ],
    'cookies': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('key', 1),
                ('user', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('key', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('expires', -1)
            ],
            'unique': False
        }
    ],
    'feedback_logs': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('target', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('timestamp', -1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('source_asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('parent', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('source_asset_type', 1)
            ],
            'unique': False
        }
    ],
    'files.chunks': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('files_id', 1),
                ('n', 1)
            ],
            'unique': True
        }
    ],
    'files.files': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('filename', 1),
                ('uploadDate', 1)
            ],
            'unique': True
        }
    ],
    'temp.chunks': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('files_id', 1),
                ('n', 1)
            ],
            'unique': True
        }
    ],
    'temp.files': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('filename', 1),
                ('uploadDate', 1)
            ],
            'unique': True
        }
    ],
    'filters': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': True
        }
    ],
    'insights': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        }
    ],
    'portals': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        }
    ],
    'job_claims': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('job_id', 1)
            ],
            'unique': True
        }
    ],
    'sres.apscheduler': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('next_run_time', 1)
            ],
            'unique': False
        }
    ],
    'sres.sessions': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('id', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('expiration', -1)
            ],
            'unique': False
        }
    ],
    'interaction_logs': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('source_asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('source_asset_type', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('parent', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('timestamp', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('target', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('action', 1)
            ],
            'unique': False
        }
    ],
    'message_send_logs': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('source_asset_type', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('source_asset_uuid', 1)
            ],
            'unique': False
        }
    ],
    'tag_groups': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('name', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        }
    ],
    'tags': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('name', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('tag_group_id', 1)
            ],
            'unique': False
        }
    ],
    'users': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('username', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('email', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('api_keys_list', 1)
            ],
            'unique': False
        }
    ],
    'access_logs': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('timestamp', -1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('username', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('asset_type', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('asset_type', 1),
                ('asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('related_asset_type', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('related_asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('related_asset_type', 1),
                ('related_asset_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('action', 1)
            ],
            'unique': False
        }
    ],
    'lti': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('consumer_resource_link_id', 1)
            ],
            'unique': False
        }
    ],
    'summaries': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('uuid', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('table_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('column_uuid', 1)
            ],
            'unique': False
        },
        {
            'keys': [
                ('workflow_state', 1)
            ],
            'unique': False
        }
    ],
    'go_codes': [
        {
            'keys': [
                ('_id', 1)
            ],
            'unique': True
        },
        {
            'keys': [
                ('url', 1)
            ],
            'unique': True
        }
    ]
}

def _get_db(read_preference='primaryPreferred'):
    """Thread-safe and request/current_app-less db getter?????"""
    # import config for db directly from instance - this is hacky...
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from instance.config import MONGO_URI, MONGO_DB
    client = MongoClient(MONGO_URI)
    if read_preference == 'primaryPreferred':
        return client.get_database(MONGO_DB, read_preference=ReadPreference.PRIMARY_PREFERRED)
    elif read_preference == 'secondaryPreferred':
        return client.get_database(MONGO_DB, read_preference=ReadPreference.SECONDARY_PREFERRED)
    else:
        # default
        return client[MONGO_DB]

def _check_mongo_indexes(collection):
    db = _get_db()
    indexes = db[collection].index_information()
    return indexes
    
def _create_mongo_index(collection, keys, unique=False):
    db = _get_db()
    res = db[collection].create_index(keys, unique=unique, background=True)
    return res

class DbCookie:
    
    def __init__(self, username_override=None):
        from sres.users import User
        from sres.auth import get_auth_user
        self.db = _get_db()
        self.user = User()
        if username_override is None:
            self.user.find_user(username=get_auth_user())
        else:
            self.user.find_user(username=username_override)
    
    def set(self, key, value='', expires=None, use_key_as_is=False):
        prefix = self.user.config['username']
        key = key if use_key_as_is == True else '{}.{}'.format(prefix, key)
        expires = expires if expires is not None else datetime.now() + timedelta(days=3650)
        try:
            result = self.db.cookies.update_one(
                {
                    'key': key,
                    'user': self.user._id
                },
                { '$set': {
                        'key': key,
                        'user': self.user._id,
                        'value': value,
                        'expires': expires
                    }
                },
                upsert=True
            )
            return result.acknowledged
        except Exception as e:
            logging.error('DbCookie.set [{}]'.format(key))
            logging.exception(e)
            return False
        
    def get(self, key, default=''):
        prefix = self.user.config['username']
        results = list(self.db.cookies.find({
            'key': '{}.{}'.format(prefix, key),
            'user': self.user._id
        }))
        if len(results) == 1:
            return results[0]['value']
        else:
            return default
    
    def delete(self, key):
        result = self.db.cookies.delete_one({
            'key': '{}.{}'.format(self.user.config['username'], key),
            'user': self.user._id
        })
        return result.acknowledged
    
    def get_like(self, key_pattern, ignore_username=False, default=[], get_latest_only=False):
        """Returns the raw cookie saved in db.cookies - this will be a dict, 
            unlike get() which parses the value."""
        filter = {}
        filter['key'] = {'$regex': key_pattern, '$options': 'i'}
        if not ignore_username:
            filter['user'] = self.user._id
        results = list(self.db.cookies.find(filter).sort([('expires', -1)]))
        if len(results) == 0:
            return default
        else:
            if get_latest_only:
                return results[0]
            else:
                return results
    
    
    
    
    
