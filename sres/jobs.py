from datetime import datetime
from time import sleep
import random
from os import getpid
import platform
from pymongo.errors import DuplicateKeyError
import logging

from sres.db import _get_db
from sres.config import _get_config

class APSJob:
    def __init__(self, job_id):
        self.job_id = job_id
        self.db = _get_db()
        self.dbc = self.db['sres.apscheduler']
        self.loaded = False
        config = _get_config()
        self.server_number = config.SRES.get('SERVER_NUMBER', 1)
    
    def load(self):
        """Loads the job_id specified on instantiation. Returns True if successful, False otherwise."""
        res = list(self.dbc.find({'_id': self.job_id}))
        if len(res) > 0:
            self.loaded = True
            return True
        else:
            return False
    
    def set_flag(self, flag):
        if self.loaded:
            flags = [{
                'flag': flag,
                'timestamp': datetime.now()
            }]
            # TOOD: extend as opposed to overwrite
            res = self.dbc.update_one({'_id': self.job_id}, {'$set': {'flags': flags}})
            return res.acknowledged
        else:
            return False
    
    def get_flag(self, flag_key=None):
        if self.loaded:
            res = list(self.dbc.find({'_id': self.job_id}))
            if len(res) > 0 and len(res[0].get('flags', [])) > 0:
                if flag_key is None:
                    return res[0]['flags'][0]
                else:
                    for flag in res[0]['flags']:
                        if flag['flag'] == flag_key:
                            return flag
        return {}
    
    def delete_flags(self):
        if self.loaded:
            self.dbc.update_one({'_id': self.job_id}, {'$unset': {'flags': 1}})
    
    def claim_job(self, skip_loading=False):
        """Tries to claim the job for this worker/process. Returns True if OK to proceed otherwise False"""
        logging.debug('node [{}] pid [{}] trying to claim job [{}]'.format(platform.node(), getpid(), self.job_id))
        #logging.debug('claiming...')
        if skip_loading or self.load():
            sleep(random.uniform(0.5, self.server_number) * float(int(str(getpid())[-1])))
            try:
                self.db.job_claims.insert({
                    'job_id': self.job_id,
                    'node': platform.node(),
                    'pid': getpid(),
                    'claimed_on': datetime.now()
                })
                logging.debug('job successfully claimed [{}] [{}]'.format(self.job_id, getpid()))
                return True
            except DuplicateKeyError:
                logging.debug('node [{}] pid [{}] duplicate key job_id [{}]'.format(platform.node(), getpid(), self.job_id))
                claims = self.db.job_claims.find({'job_id': self.job_id})
                claim = list(claims)[0]
                if (datetime.now() - claim.get('claimed_on')).total_seconds() < 7200:
                    logging.debug('job_id appears to be running still [{}]'.format(self.job_id))
                    return False
                else:
                    self.release_claim()
                    return self.claim_job()
                return False
        else:
            logging.debug('Could not load for claim_job job_id [{}] [{}]'.format(self.job_id, getpid()))
        return False
    
    def release_claim(self):
        if self.job_id:
            res = self.db.job_claims.delete_one({'job_id': self.job_id})
            logging.debug('release claim [{}] [{}]'.format(self.job_id, res.acknowledged))
            return res.acknowledged
        return False
    
    def set_now_started(self):
        if self.loaded:
            return self.set_flag('started')
        return False
    
    def has_already_started(self):
        if self.loaded:
            # first wait a random amount of time to allow other processes to catch up,
            # as in, allow the process that has grabbed this job to flag it as started
            sleep(random.uniform(1.0, 3.0) * float(int(str(getpid())[-1])) + (self.server_number * 2.0))
            # then check
            flag = self.get_flag('started')
            if flag and flag.get('timestamp'):
                if (datetime.now() - flag.get('timestamp')).total_seconds() < 7200:
                    return True
                else:
                    # started over 24 hours ago!
                    # assume crashed
                    self.delete_flags()
                    return False
        return False
        
        