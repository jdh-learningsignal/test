from flask import Blueprint, g, send_from_directory, current_app, abort, request
import os
import json
import logging

from sres.files import get_file_access_key, get_file_access_url, GridFile, _migrate_to_gridfs
from sres.auth import login_required, is_user_administrator, get_auth_user
from sres import utils
from sres.columns import Column
from sres.studentdata import StudentData

bp = Blueprint('file', __name__, url_prefix='/files')

@bp.route('/get/<filename>', methods=['GET'])
def get_file(filename, key=None):
    key = request.args.get('key')
    attachment_filename = request.args.get('fn', None)
    if attachment_filename is None:
        attachment_filename = filename
    # check authorisations
    is_authorised = False
    auth_user = get_auth_user()
    if key is None:
        if not auth_user: # require the key if user is not logged in
            abort(401)
        else:
            # a user is logged in - see if they have access despite not supplying a key
            gf = GridFile('files')
            if gf.find_and_load(filename):
                metadata = gf.get_metadata()
                column_uuid = metadata.get('column_uuid')
                if column_uuid:
                    column = Column()
                    if column.load(column_uuid):
                        if column.is_user_authorised(username=auth_user):
                            is_authorised = True
                        else:
                            # provision for student
                            student_data = StudentData(column.table)
                            if student_data.find_student(auth_user):
                                if auth_user == metadata.get('identifier'):
                                    is_authorised = True
                                else:
                                    logging.debug(f'{auth_user} != {metadata.get("identifier")}')
                            else:
                                logging.debug(f'student not found {auth_user}')
                else:
                    # no way to tell
                    is_authorised = False
    else:
        if get_file_access_key(filename).lower() == key.lower():
            is_authorised = True
    # load file, if authorised
    if is_authorised:
        gf = GridFile('files')
        if gf.find_and_load(filename):
            f = gf.get_file()
            original_filename = gf.original_filename
            download_filename = original_filename if original_filename else attachment_filename
            r = current_app.response_class(f, direct_passthrough=True, mimetype='application/octet-stream')
            r.headers.set('Content-Disposition', 'attachment', filename=download_filename)
            return r
        else:
            abort(404)
    else:
        abort(403)

@bp.route('/upload', methods=['POST'])
@login_required
def upload_file():
    file = request.files.get('file')
    new_filename = utils.create_uuid() + '.' + file.filename.split('.')[-1]
    gf = GridFile('files')
    if gf.save_file(file, new_filename):
        return json.dumps({
            'location': get_file_access_url(
                filename=new_filename,
                full_path=True
            )
        })
    else:
        abort(400)
    
@bp.route('/_migrate', methods=['GET'])
@login_required
def migrate_files_to_gridfs():
    if not is_user_administrator('super'):
        abort(403)
    filename = request.args.get('filename')
    collection = request.args.get('collection')
    res = _migrate_to_gridfs(collection, filename)
    return json.dumps(res)