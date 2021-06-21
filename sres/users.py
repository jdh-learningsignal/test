from flask import g
from copy import deepcopy
from natsort import natsorted, ns
from bson import ObjectId
import re
import logging
import secrets
import string
from datetime import datetime

from sres.db import _get_db
from sres import utils

ADMIN_CATEGORIES = {
    'list': {
        'name': 'list',
        'display': "List administrators",
        'description': "These users are able to create new lists."
    },
    'filter': {
        'name': 'filter',
        'display': "Filter administrators",
        'description': "These users are able to create new filters and view/edit filters they have been given access to."
    },
    'super': {
        'name': 'super',
        'display': "List administrators",
        'description': "These users can do everything."
    }
}

def oids_to_usernames(oids=None, return_attribute='username'):
    """ 
        Takes a list of oids (ObjectId) and returns a dict mapping oids (as objects) 
        to the requested user attribute (as strings)
    """
    ret = {}
    if oids is None or (type(oids) is list and len(oids) == 0) or not oids:
        return ret
    db = _get_db()
    if return_attribute in ['email', 'username', 'sid', '_id']:
        # More efficient - single database callable
        filter = []
        for oid in oids:
            filter.append({'_id': oid})
        results = db.users.find(
            {'$or': filter}
        )
        for result in results:
            ret[result['_id']] = result[return_attribute]
    return ret

def oids_to_display_names(oids=None):
    """
        Takes a list of oids (ObjectId or str) and returns a dict mapping oids to a
        friendlier representation of the user.
    """
    ret = {}
    if oids is None or (type(oids) is list and len(oids) == 0) or not oids:
        return ret
    db = _get_db()
    filter = []
    for oid in oids:
        if isinstance(oid, str):
            oid = ObjectId(oid)
        filter.append({'_id': oid})
    results = list(db.users.find(
        {
            '$or': filter
        },
        ['given_names', 'surname']
    ))
    for result in results:
        ret[result['_id']] = {
            'first_and_last': '{} {}'.format(
                result.get('given_names', '').split(' ')[0],
                result.get('surname', '')
            ),
            'given_names': result.get('given_names', ''),
            'first_name': result.get('given_names', '').split(' ')[0],
            'surname': result.get('surname', '')
        }
    return ret
    
def usernames_to_oids(usernames=None, add_if_not_exists=True):
    """
        Takes a list of usernames (as strings), searches db.users for them, and either
        returns the oid (object) if found, or optionally adds the user via their username
    """
    ret = []
    if usernames is None or (type(usernames) is list and len(usernames) == 0) or not usernames:
        return ret
    user = User()
    if type(usernames) is str:
        usernames = [usernames]
    for username in usernames:
        oid = user.find_user(username=username, add_if_not_exists=add_if_not_exists)
        if oid:
            ret.append(oid)
        else:
            ret.append(None)
    return ret

def search_users(term):
    """
        Searches db.users for the term. Returns a list of dicts.
    """
    filters = []
    targets = ['email', 'username', 'given_names', 'surname']
    for key in targets:
        filters.append({key: {'$regex': term, '$options': 'i'}})
    db = _get_db()
    results = db.users.find({'$or': filters})
    ret = []
    for result in results:
        ret_one = {}
        for key in targets + ['_id']:
            if key in result.keys():
                ret_one[key] = result[key]
        if 'given_names' in ret_one.keys() and 'surname' in ret_one.keys():
            ret_one['display_name'] = '{} {}'.format(ret_one['given_names'], ret_one['surname'])
        else:
            ret_one['display_name'] = ''
        ret.append(deepcopy(ret_one))
    return ret

def _users_length():
    db = _get_db()
    return db.users.count_documents({})

def get_admins():
    db = _get_db()
    ret = {}
    for k, v in ADMIN_CATEGORIES.items():
        ret[k] = []
        results = db.users.find({'permissions': '_' + k}, ['username', 'email', 'given_names', 'surname', 'permissions'])
        results = natsorted(list(results), key=lambda u: u['username'], alg=ns.IGNORECASE)
        for result in results:
            if 'given_names' in result.keys() and 'surname' in result.keys():
                display_name = '{} {}'.format(result['given_names'], result['surname'])
            else:
                display_name = result['username']
            ret[k].append({
                'username': result['username'],
                'display_name': display_name,
                'email': result['email'],
                'given_names': result.get('given_names', ' '.join(display_name.split(' ')[:-1])),
                'preferred_name': result.get('given_names', display_name).split(' ')[0],
                'surname': result.get('surname', display_name).split(' ')[-1]
            })
    return ret

def get_admins_aggregate():
    admins = get_admins()
    ret = {}
    for category, config in ADMIN_CATEGORIES.items():
        for admin in admins[category]:
            if admin['username'] in ret.keys():
                if category not in ret[admin['username']]['access_levels']:
                    ret[admin['username']]['access_levels'].append(category)
            else:
                ret[admin['username']] = {**admin, **{'access_levels': [category]}}
    return ret

def change_admins(category, add_usernames=[], remove_usernames=[]):
    """
        category (str) e.g. list|filter|super
        add_usernames (list of strings) The usernames that need category of admin added.
        remove_usernames (list of strings) The usernames that need category of admin removed.
    """
    db = _get_db()
    ret = {
        'added': 0,
        'removed': 0
    }
    # add - need to check if users exist first
    add_usernames_oids = []
    for username in add_usernames:
        user = User()
        if user.find_user(username=username, add_if_not_exists=True):
            add_usernames_oids.append(user._id)
    result = db.users.update_many(
        {'_id': {'$in': add_usernames_oids}},
        {'$addToSet': {'permissions': '_{}'.format(category)}}
    )
    ret['added'] = result.matched_count
    # remove
    remove_usernames_oids = []
    for username in remove_usernames:
        user = User()
        if user.find_user(username=username, add_if_not_exists=False):
            remove_usernames_oids.append(user._id)
    result = db.users.update_many(
        {'_id': {'$in': remove_usernames_oids}},
        {'$pull': {'permissions': '_{}'.format(category)}}
    )
    ret['removed'] = result.matched_count
    return ret

def find_email_for_user(term):
    if re.match(r'\b.+@.+\.(.{2,}){1,}\b', term):
        return term
    # attempt to get from db.users
    user = User()
    if user.find_user(username=term, add_if_not_exists=False) and user.config['email']:
        return user.config['email']
    # attempt to get from ldap
    from sres.ldap import find_user_by_username
    res = find_user_by_username(term)
    if res and res[0]['email']:
        return res[0]['email']
    # give up
    return ''

def get_user_by_api_key(key):
    db = _get_db()
    key = utils.clean_alphanumeric(key)
    users = list(db.users.find({'api_keys_list': key}))
    if len(users) == 1:
        user = User()
        if user.find_user(username=users[0].get('username'), add_if_not_exists=False) and user.verify_api_key(key):
            return user
    return None

CONFIGURABLE_FIELDS = [
    'email',
    'username',
    'given_names',
    'surname'
]

class User:
    
    default_config = {
        'email': '',
        'username': '',
        'given_names': '',
        'surname': '',
        'permissions': [],
        'hashed_password': '',
        'salt': '',
        'api_keys': {}, # keyed by key_uuid
        'api_keys_list': [], # just the keys
        'sid': ''
    }
    
    def __init__(self):
        from sres.db import _get_db
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
    
    def find_user(self, sid=None, email=None, username=None, strict_find=True, add_if_not_exists=True, oid=None):
        """
            Searches db.users by specified identifier(s) and returns _id of found user, otherwise None.
        """
        # search
        searcher = []
        if email is not None:
            searcher.append({'email': {'$regex': '^{}$'.format(re.escape(email)), '$options': 'i'}})
        if sid is not None:
            searcher.append({'sid': {'$regex': '^{}$'.format(re.escape(sid)), '$options': 'i'}})
        if username is not None:
            searcher.append({'username': {'$regex': '^{}$'.format(re.escape(username)), '$options': 'i'}})
        if oid is not None:
            if isinstance(oid, str):
                oid = ObjectId(oid)
            searcher.append({'_id': oid})
        
        if len(searcher) > 0:
            db = _get_db()
            users = list(db.users.find({'$and' if strict_find else '$or': searcher}))
        else:
            return None
        
        # return
        if len(users) == 1:
            return self._load(users[0])
        elif len(users) > 1:
            logging.warning('person exists multiple times [{}] [{}] [{}]'.format(sid, email, username))
            # look for exact match
            for user in users:
                if user.get('username') == username: return self._load(user)
                if user.get('sid') == sid: return self._load(user)
                if user.get('email') == email: return self._load(user)
            return None
        else:
            if add_if_not_exists:
                self.db.users.insert_one({
                    'sid': sid,
                    'email': email,
                    'username': username
                })
                logging.info('Added previously unknown user [{}] [{}] [{}]'.format(sid, email, username))
                return self.find_user(sid=sid, username=username, email=email, strict_find=strict_find, add_if_not_exists=False)
            else:
                return None
    
    def _load(self, config):
        for key, value in self.default_config.items():
            self.config[key] = config[key] if key in config.keys() else value
        self._id = config['_id']
        self.config['full_name'] = '{} {}'.format(self.config['given_names'], self.config['surname'])
        return self._id
    
    def update(self):
        result = self.db.users.update_one(
            {'username': self.config['username']},
            {'$set': self.config}
        )
        return result.acknowledged
    
    def make_api_key(self, description=''):
        alphabet = string.ascii_letters + string.digits
        new_key = ''.join(secrets.choice(alphabet) for i in range(64))
        new_key_uuid = utils.create_uuid()
        self.config['api_keys'][new_key_uuid] = {
            'key': new_key,
            'created': datetime.now(),
            'description': description
        }
        self.config['api_keys_list'].append(new_key)
        if self.update():
            return {
                'uuid': new_key_uuid,
                'key': new_key,
                'description': description
            }
        else:
            return None
    
    def delete_api_key(self, key_uuid):
        if key_uuid in self.config['api_keys'].keys():
            key = self.config['api_keys'][key_uuid]['key']
            self.config['api_keys_list'].remove(key)
            self.config['api_keys'].pop(key_uuid, None)
            return self.update()
        else:
            return None
    
    def verify_api_key(self, key):
        for key_uuid, meta in self.config['api_keys'].items():
            if meta['key'] == key:
                return True
        return False
    
    def get_api_keys(self):
        return self.config['api_keys']


