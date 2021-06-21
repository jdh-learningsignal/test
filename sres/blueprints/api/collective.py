from flask import request, abort, Blueprint, jsonify, url_for
import re
import json
import logging
from datetime import datetime
from dateutil import parser

from sres.blueprints.api.auth import check_authentication
from sres.blueprints.api.shared import api_abort
from sres.auth import is_logged_in, get_auth_user
from sres.columns import Column
from sres.tables import Table
from sres.filters import Filter
from sres.collective_assets import CollectiveAsset, list_assets, ALL_ASSET_TYPES, copy_asset_for_user

bp = Blueprint('api_collective', __name__, url_prefix='/api/v1')

@bp.route('/collective/assets', methods=['GET'])
def get_collective_assets():
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    mine_only = True if request.args.get('mine_only') == 'true' else False
    only_asset_types = request.args.getlist('only_asset_types')
    if len(only_asset_types) == 0:
        only_asset_types = list(ALL_ASSET_TYPES.keys())
    
    available_assets = list_assets(only_asset_types=only_asset_types, mine_only=mine_only)
    
    assets = []
    for available_asset in available_assets:
        _asset = {}
        for k in [ 'uuid', 'source_asset_uuid', 'name', 'type', 'display_type', 'visibility', 'description', 'shared_on', 'shared_by', 'workflow_state' ]:
            _asset[k] = available_asset[k]
        assets.append(_asset)
    
    resp = jsonify(assets)
    resp.status_code = 200
    return resp

@bp.route('/collective/assets/<asset_uuid>/copy', methods=['POST'])
def copy_asset(asset_uuid):
    
    ret = {
        'success': False,
        'messages': [],
        'edit_url': '',
        'new_asset_uuid': '',
        'asset_type': ''
    }
    
    auth = check_authentication(request)
    if not auth['authenticated']:
        abort(401)
    
    req = request.get_json(force=True)
    
    table_uuid = req.get('table_uuid')
    if table_uuid is None:
        abort(400)
    
    copy_result = copy_asset_for_user(
        asset_uuid=asset_uuid,
        table_uuid=table_uuid,
        override_user_oid=auth['auth_user_oid'],
        referenced_assets_uuids=None # TODO referenced_assets_uuids
    )
    
    if copy_result['success']:
        ret['success'] = True
        ret['new_asset_uuid'] = copy_result['new_asset_uuid']
        collective_asset = copy_result['collective_asset']
        ret['asset_type'] = collective_asset.asset_type
        if collective_asset.asset_type == 'column' or collective_asset.asset_type == 'aggregatorcolumn':
            ret['edit_url'] = url_for('table.edit_column', table_uuid=table_uuid, column_uuid=copy_result['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid'], _external=True)
        elif collective_asset.asset_type == 'filter':
            filter = Filter()
            if filter.load(copy_result['new_asset_uuid']):
                # make a new tracking counter column in the new target table?
                if req.get('make_new_tracking_column', True) == True:
                    tracking_column = Column()
                    tracking_column.create(table_uuid=table_uuid, override_username=auth['auth_username'])
                    tracking_column.config['name'] = f"Tracking counter for {filter.config['name']}"
                    tracking_column.config['active']['from'] = datetime.now()
                    tracking_column.config['active']['to'] = datetime.now()
                    if tracking_column.update(override_username=auth['auth_username']):
                        filter.config['tracking_record'][0] = {
                            'table_uuid': tracking_column.table.config['uuid'],
                            'column_uuid': tracking_column.config['uuid']
                        }
                        if filter.update(override_username=auth['auth_username']):
                            # keep going
                            pass
                        else:
                            return api_abort(400, 'Error updating filter with new tracking column record.')
                    else:
                        return api_abort(400, 'Error updating tracking column.')
                # re-reference columns?
                if req.get('rereference_columns', []):
                    for mapper in req.get('rereference_columns'):
                        original = mapper.get('original')
                        new = mapper.get('new')
                        if original and new:
                            filter.rereference_columns(original, new)
                    if filter.update(override_username=auth['auth_username']):
                        # keep going
                        pass
                    else:
                        return api_abort(400, 'Error updating filter after re-referencing columns.')
                # return the edit url
                ret['edit_url'] = url_for('filter.edit_filter', filter_uuid=copy_result['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid'], _external=True)
            else:
                return api_abort(400, 'Error loading newly copied filter asset.')
        elif collective_asset.asset_type == 'portal':
            ret['edit_url'] = url_for('portal.edit_portal', portal_uuid=copy_result['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid'], _external=True)
    else:
        logging.warning(copy_result)
        ret['messages'] = copy_result['messages']
    
    resp = jsonify(ret)
    resp.status_code = copy_result['status_code']
    return resp
    
    