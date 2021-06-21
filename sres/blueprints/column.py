from flask import (Blueprint, g, request, abort, url_for)
import re
import json
import logging

from sres.auth import login_required
from sres.columns import Column, parse_multientry_column_reference, parse_string_for_column_references
from sres.studentdata import enumerate_distinct_data_by_column
from sres.collective_assets import CollectiveAsset

bp = Blueprint('column', __name__, url_prefix='/columns')

@bp.route('/details', methods=['GET'])
@login_required
def get_column_details(column_references=''):
    """
        Returns (json) a dict of dicts with metadata of columns specified, 
        keyed by the column reference(s)
    """
    column_references = request.args.get('column_references')
    column_references = re.sub('[^A-Z0-9a-z_\.,]', '', column_references).split(',')
    
    ret = {}
    
    from_collective_asset_uuid = request.args.get('from_collective_asset_uuid')
    collective_asset_uuid = request.args.get('collective_asset_uuid')
    
    if from_collective_asset_uuid or collective_asset_uuid:
        # load from collective asset
        collective_asset = CollectiveAsset()
        if collective_asset.load(from_collective_asset_uuid or collective_asset_uuid):
            select_array = collective_asset.get_select_array_for_referenced_columns()
            for column_reference in column_references:
                parsed_ref = parse_multientry_column_reference(column_reference)
                for item in select_array:
                    if item['value'] == parsed_ref['base_column_reference']:
                        record = {
                            'name': item['display_text'],
                            'description': '',
                            'uoscode': '',
                            'uosname': '',
                            'uosyear': '',
                            'uossemester': '',
                            'columnuuid': parsed_ref['base_column_uuid'],
                            'tableuuid': '',
                            'subfield': parsed_ref.get('field_number', ''),
                            'subfieldlabel': '',
                            'original_columnuuid': column_reference,
                            'escaped_original_column_reference': re.escape(column_reference),
                            'magic_formatter': '',
                            'friendly_display': item['display_text']
                        }
                        #if column.get_magic_formatter_meta():
                        #    record['friendly_display'] += ' ({})'.format(column.get_magic_formatter_meta().get('display'))
                        ret[column_reference] = record
                        break
    # standard load
    for column_reference in column_references:
        if column_reference in ret.keys():
            continue
        column = Column()
        try:
            if column.load(column_reference) and column.is_user_authorised():
                record = {
                    'name': column.config['name'],
                    'description': column.config['description'],
                    'uoscode': column.table.config['code'],
                    'uosname': column.table.config['name'],
                    'uosyear': column.table.config['year'],
                    'uossemester': column.table.config['semester'],
                    'columnuuid': column.config['uuid'],
                    'tableuuid': column.table.config['uuid'],
                    'subfield': column.subfield,
                    'subfieldlabel': column.subfield_label,
                    'original_columnuuid': column_reference,
                    'escaped_original_column_reference': re.escape(column_reference),
                    'magic_formatter': column.magic_formatter,
                    'friendly_display': column.get_friendly_name(show_table_info=True, get_text_only=True)
                }
                if column.get_magic_formatter_meta():
                    record['friendly_display'] += ' ({})'.format(column.get_magic_formatter_meta().get('display'))
                ret[column_reference] = record
        except:
            ret[column_reference] = {}
    return json.dumps(ret)
    
@bp.route('/operand_suggestions', methods=['GET'])
@login_required
def enumerate_operand_suggestions():
    """
        Returns (json) a list of strings of suggested
    """
    column_reference = request.args.get('column_reference')
    term = request.args.get('term')
    column = Column()
    if column.load(column_reference):
        if column.is_user_authorised():
            ret = {
                "success": False,
                "messages": [],
                "search_results": []
            }
            suggestions = enumerate_distinct_data_by_column(column, term)
            if suggestions:
                ret['success'] = True
                ret['search_results'] = suggestions
            return json.dumps(ret)
        else:
            abort(403)
    else:
        abort(400)

@bp.route('/writeability', methods=['POST'])
@login_required
def get_writeability():
    """
        Checks and returns the permissions of columns for student writeability. Accepts an unparsed string.
    """
    column_uuids = parse_string_for_column_references(request.form.get('input_string', ''), uuids_only=True)
    _extra = json.loads(request.form.get('_extra', '{}'))
    ret = []
    for column_uuid in column_uuids:
        meta = {
            'authorised': False,
            'friendly_name': '',
            'edit_link': '',
            'is_active': False,
            'is_writeable_by_students': False,
            'is_self_data_entry_enabled': False,
            'student_editing_allowed': False
        }
        column = Column()
        if column.load(column_uuid) and column.is_user_authorised():
            meta = {
                'authorised': True,
                'friendly_name': column.get_friendly_name(get_text_only=True),
                'edit_link': url_for('table.edit_column', column_uuid=column.config['uuid'], table_uuid=column.table.config['uuid']),
                'is_active': column.is_active(),
                'is_writeable_by_students': column.is_writeable_by_students(),
                'is_self_data_entry_enabled': column.is_self_data_entry_enabled()
            }
            meta['student_editing_allowed'] = meta['is_writeable_by_students'] and meta['is_self_data_entry_enabled']
        ret.append(meta)
    return json.dumps({
        'columns': ret,
        '_extra': _extra
    })
    
    
    

