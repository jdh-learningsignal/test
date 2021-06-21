from flask import url_for, current_app
from hashlib import sha256
from gridfs import GridFS, GridFSBucket
import logging

from sres.db import _get_db
from sres import utils

def get_file_access_key(filename):
    m = sha256(bytes('{}{}'.format(filename, current_app.config['SECRET_KEY']), 'utf-8'))
    return m.hexdigest().lower()

def get_file_access_url(filename, full_path=False, attachment_filename=None):
    key = get_file_access_key(filename)
    return url_for('file.get_file', _external=full_path, filename=filename, key=key, fn=attachment_filename)

def _migrate_to_gridfs(collection, filename=None):
    """
        collection (string) FILES|TEMP
    """
    import os
    from datetime import datetime
    ret = {
        'number_migrated': 0,
        'migrated': []
    }
    try:
        path = current_app.config['SRES']['PATHS'][collection.upper()]
    except:
        return ret
    if filename is None:
        # look for all files
        files = os.scandir(path)
        full_file_paths = []
        for file in files:
            if file.is_file:
                st = os.stat(file.path)
                ts = datetime.fromtimestamp(st.st_mtime)
                if ts.year in [2017]:
                    full_file_paths.append(file.path)
        files.close()
    else:
        full_file_paths = [os.path.join(path, filename)]
    logging.debug(str(full_file_paths))
    import ntpath
    for full_file_path in full_file_paths:
        try:
            gf = GridFile(collection.lower())
            fn = ntpath.basename(full_file_path)
            if gf.find_and_load(fn):
                logging.info('duplicate file found [{}] [{}]'.format(collection, fn))
                continue
            if gf.save_file(open(full_file_path, 'rb').read(), fn, content_type=utils.guess_mime_type(full_file_path)):
                ret['migrated'].append('{} {}'.format(collection, fn))
                ret['number_migrated'] += 1
                logging.info('migrated [{}] [{}]'.format(collection, fn))
            else:
                logging.error('migrating failed [{}] [{}]'.format(collection, fn))
        except Exception as e:
            logging.error('migrating failed [{}] [{}]'.format(collection, fn))
            logging.exception(e)
    return ret

class GridFile:
    
    def __init__(self, collection='files'):
        """
            collection (string) files|temp
        """
        if collection in ['files', 'temp']:
            self.db = _get_db()
            self.fs = GridFS(self.db, collection)
            self.file_id = None
            self.mime_type = ''
            self.original_filename = ''
            self.bytes = 0
            self.collection = collection
        else:
            raise
    
    def find_and_load(self, filename):
        out = self.fs.find_one({'filename': filename})
        if out:
            self.file_id = out._id
            self.mime_type = out.content_type
            self.bytes = out.length
            try:
                self.original_filename = out.original_filename
            except:
                pass
            return True
        else:
            return False
    
    def open_stream(self):
        fsb = GridFSBucket(self.db, bucket_name=self.collection)
        return fsb.open_download_stream(self.file_id)
    
    def save_file(self, file, filename, content_type=None, original_filename=None):
        res = self.fs.put(
            file, 
            content_type=content_type or file.content_type, 
            filename=filename,
            original_filename=filename if original_filename is None else original_filename
        )
        if res:
            return self.find_and_load(filename)
        else:
            return False
    
    def get_file(self):
        return self.fs.get(self.file_id)
        pass
    
    def delete_file(self):
        if self.file_id:
            self.fs.delete(self.file_id)
            return True
        else:
            return False
    
    def set_metadata(self, metadata):
        if self.file_id:
            res = self.db.file_metadata.update_one(
                {'file_id': self.file_id},
                {'$set': {
                    'file_id': self.file_id,
                    'metadata': metadata
                }},
                upsert=True
            )
            return res.acknowledged
        return False
    
    def get_metadata(self):
        if self.file_id:
            res = list(self.db.file_metadata.find({'file_id': self.file_id}))
            if len(res):
                return list(res)[0].get('metadata', {})
        return {}