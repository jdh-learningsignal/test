from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for, abort, jsonify, current_app, send_from_directory, Markup, Response)
import os
import json
import logging
import bleach

from sres.auth import login_required, is_user_administrator, get_auth_user, is_user_asset_administrator_anywhere
from sres.tables import Table
from sres.columns import Column, get_config_from_column_uuids, COLUMN_DATA_TYPES_META
from sres.aggregatorcolumns import AggregatorColumn
from sres.filters import Filter
from sres.portals import Portal
from sres.collective_assets import CollectiveAsset, search_assets, list_assets, ALL_ASSET_TYPES, copy_asset_for_user

bp = Blueprint('collective', __name__, url_prefix='/collective')

@bp.route('/', methods=['GET'])
@login_required
def view_collective():
    vars = {
        'all_asset_types': ALL_ASSET_TYPES
    }
    return render_template('collective-view.html', vars=vars)
    
@bp.route('/search', methods=['GET'])
@login_required
def search_collective():
    term = request.args.get('term', '')
    term = bleach.clean(term)
    limit = request.args.get('limit', None)
    search_asset_types = request.args.getlist('search_asset_types')
    mine_only = True if request.args.get('mine_only', None) is not None else False
    if request.args.get('all'):
        search_asset_types = list(ALL_ASSET_TYPES.keys())
    if is_user_asset_administrator_anywhere() or is_user_administrator('list') or is_user_administrator('filter') or is_user_administrator('super'):
        if len(term):
            return json.dumps(search_assets(term, only_asset_types=search_asset_types, mine_only=mine_only), default=str)
        else:
            return json.dumps(list_assets(limit=limit, only_asset_types=search_asset_types, mine_only=mine_only), default=str)
    else:
        abort(403)

@bp.route('/assets/<asset_uuid>/show', methods=['GET'])
@login_required
def show_asset(asset_uuid):
    collective_vars = {}
    collective_asset = CollectiveAsset()
    if collective_asset.load(asset_uuid):
        # check permissions
        if collective_asset.is_user_authorised_viewer() or collective_asset.is_user_authorised_editor():
            pass
        else:
            flash("Unable to load asset - insufficient permissions.", "warning")
            return render_template('denied.html')
        # load referenced assets
        collective_vars['referenced_assets_configs'] = _get_referenced_asset_configs(collective_asset)
        # render
        return _render_asset_editor(
            asset_type=collective_asset.asset_type,
            original_asset_uuid=None,
            asset_uuid=collective_asset.config['uuid'],
            collective_mode='show',
            source_asset=collective_asset.asset,
            collective_vars=collective_vars
        )
    else:
        flash("Unable to load asset.", "warning")
        return render_template('denied.html')

def _get_referenced_asset_configs(collective_asset):
    referenced_assets = collective_asset.get_referenced_assets()
    referenced_assets_configs = { a['uuid']: a for a in referenced_assets }
    for asset_uuid, asset_config in referenced_assets_configs.items():
        referenced_assets_configs[asset_uuid]['asset_type'] = ALL_ASSET_TYPES[asset_config['type']]['display_1']
        referenced_asset = CollectiveAsset()
        if referenced_asset.load(asset_uuid):
            referenced_assets_configs[asset_uuid]['children'] = _get_referenced_asset_configs(referenced_asset)
    return referenced_assets_configs

@bp.route('/assets/<asset_uuid>/preview', methods=['GET'])
@login_required
def preview_asset(asset_uuid):
    collective_asset = CollectiveAsset()
    if collective_asset.load(asset_uuid):
        if collective_asset.config['type'] == 'column':
            from sres.blueprints.entry import preview_add_value
            return preview_add_value(table_uuid='collective', column_uuid=collective_asset.asset.config['uuid'])
        else:
            # not really built for this...
            flash("No preview is available for this {}.".format(collective_asset.config['type']), "info")
            return render_template('denied.html')
    else:
        flash("Unable to load asset.", "warning")
        return render_template('denied.html')

@bp.route('/assets/<asset_uuid>/delete', methods=['GET'])
@login_required
def delete_asset(asset_uuid):
    collective_asset = CollectiveAsset()
    if collective_asset.load(asset_uuid) and collective_asset.is_user_authorised_editor():
        collective_asset.delete()
        return redirect(url_for('collective.view_collective'))
    else:
        flash("Unable to load or permission denied.", "danger")
        return render_template('denied.html')
    
@bp.route('/assets/<asset_uuid>/copy', methods=['POST'])
@login_required
def copy_asset(asset_uuid):
    table_uuid = request.args.get('table_uuid')
    # process any other assets that also need to be copied
    referenced_assets_uuids = []
    for key in request.form.keys():
        if key.startswith('collective_asset_referenced_column_'):
            referenced_assets_uuids.append(key.replace('collective_asset_referenced_column_', ''))
    # perform the copy
    copy_result = copy_asset_for_user(
        asset_uuid=asset_uuid,
        table_uuid=table_uuid,
        referenced_assets_uuids=referenced_assets_uuids
    )
    # figure out what to do next
    if copy_result['success']:
        # render downstream editors
        collective_asset = copy_result['collective_asset']
        if collective_asset.asset_type == 'column' or collective_asset.asset_type == 'aggregatorcolumn':
            return redirect(url_for('table.edit_column', table_uuid=table_uuid, column_uuid=copy_result['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid']))
        elif collective_asset.asset_type == 'filter':
            return redirect(url_for('filter.edit_filter', filter_uuid=copy_result['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid']))
        elif collective_asset.asset_type == 'portal':
            return redirect(url_for('portal.edit_portal', portal_uuid=copy_result['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid']))
    else:
        for message in copy_result['messages']:
            flash(message[0], message[1])
            return render_template('denied.html')
    
    #if collective_asset.load(asset_uuid):
    #    if collective_asset.is_user_authorised_viewer() or collective_asset.is_user_authorised_editor():
    #        # check admin permissions
    #        if not is_user_administrator('super'):
    #            if collective_asset.asset_type in ['column', 'aggregatorcolumn', 'portal'] and not is_user_administrator('list'):
    #                flash("Sorry, you need to be a list administrator to complete this action.", "warning")
    #                return render_template('denied.html')
    #            if collective_asset.asset_type in ['filter'] and not is_user_administrator('filter'):
    #                flash("Sorry, you need to be a filter administrator to complete this action.", "warning")
    #                return render_template('denied.html')
    #        else:
    #            pass # all good
    #        table = Table()
    #        if table.load(table_uuid) and table.is_user_authorised():
    #            pass # all good
    #        else:
    #            flash("Sorry, you need to be an administrator for the list {} to complete this action.".format(table.get_full_name()), "warning")
    #            return render_template('denied.html')
    #        # see what referenced assets to also bring over
    #        referenced_assets_uuids = []
    #        for key in request.form.keys():
    #            if key.startswith('collective_asset_referenced_column_'):
    #                referenced_assets_uuids.append(key.replace('collective_asset_referenced_column_', ''))
    #        # make the copy
    #        copy_res = collective_asset.make_copy_for_user(table_uuid=table_uuid, referenced_assets_uuids=referenced_assets_uuids)
    #        if copy_res['success']:
    #            flash("Copy succeeded. You are now editing your very own copy of the {}.".format(collective_asset.asset_type), "success")
    #            for message in copy_res['messages']:
    #                flash(message[0], message[1])
    #            # show warning if re-referencing needed
    #            if len(copy_res['referenced_assets_copied']) > 0 and collective_asset.asset_type in ['filter', 'portal']:
    #                flash(
    #                    Markup(
    #                        "<span class=\"fa fa-exclamation-triangle text-danger animated flash infinite slower\"></span> {} related columns were also copied. You must now <a href=\"#\" class=\"btn btn-primary\" data-sres-trigger-click=\".sres-column-referencer-show\" data-sres-target-table-uuid=\"{}\">run the re-referencing wizard</a> so that the column references in this {} point to these columns. <strong>You will only see this message once, after which it will not be possible to perform re-referencing in this way, and any conditional settings may be irreparably broken.</strong>".format(
    #                            len(copy_res['referenced_assets_copied']),
    #                            table_uuid,
    #                            collective_asset.asset_type
    #                        )
    #                    ), "info"
    #                )
    #            if len(copy_res['nested_assets_copied']) > 0:
    #                for nested_asset_copied in copy_res['nested_assets_copied']:
    #                    flash(
    #                        Markup(
    #                            "<span class=\"fa fa-exclamation-triangle text-danger animated flash infinite slower\"></span> A related {display_type} {asset_name} <em>that has related columns of its own</em> was also copied. You must now <a href=\"{edit_url}\" class=\"btn btn-primary\" target=\"_blank\">edit this {display_type} and save it</a> so that its column references are correct. <strong>You will only see this message once, after which it will not be possible to access the editor in this way, and any column references may be irreparably broken.</strong>".format(
    #                                display_type=nested_asset_copied['display_type'],
    #                                asset_name=nested_asset_copied['asset_name'],
    #                                edit_url=url_for('table.edit_column', table_uuid=table_uuid, column_uuid=nested_asset_copied['asset_uuid'], from_collective_asset_uuid=nested_asset_copied['uuid'])
    #                            )
    #                        ), "info"
    #                    )
    #            # render downstream editors
    #            if collective_asset.asset_type == 'column' or collective_asset.asset_type == 'aggregatorcolumn':
    #                return redirect(url_for('table.edit_column', table_uuid=table_uuid, column_uuid=copy_res['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid']))
    #            elif collective_asset.asset_type == 'filter':
    #                return redirect(url_for('filter.edit_filter', filter_uuid=copy_res['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid']))
    #            elif collective_asset.asset_type == 'portal':
    #                return redirect(url_for('portal.edit_portal', portal_uuid=copy_res['new_asset_uuid'], from_collective_asset_uuid=collective_asset.config['uuid']))
    #        else:
    #            logging.debug(str(copy_res))
    #            flash("An unexpected error occured while making a copy of this asset.", "warning")
    #            return render_template('denied.html')
    #    else:
    #        flash("Unauthorised.", "warning")
    #        return render_template('denied.html')
    #else:
    #    flash("Unable to load asset.", "warning")
    #    return render_template('denied.html')

@bp.route('/<asset_type>/<source_asset_uuid>/share', methods=['GET'])
@login_required
def share_asset(asset_type, source_asset_uuid):
    """Renders the first step in sharing - where user specifies what to share and how."""
    collective_vars = {}
    # set asset
    if asset_type == 'column':
        source_asset = Column()
    elif asset_type == 'aggregatorcolumn':
        source_asset = AggregatorColumn()
    elif asset_type == 'filter':
        source_asset = Filter()
    elif asset_type == 'portal':
        source_asset = Portal()
    else:
        flash("Unable to determine source asset.", "warning")
        return render_template('denied.html')
    # load asset
    if source_asset.load(source_asset_uuid) and source_asset.is_user_authorised():
        source_asset_loaded = True
    else:
        flash("Unable to load source asset.", "warning")
        return render_template('denied.html')
    # continue processing share request
    # load up referenced columns
    collective_vars['referenced_column_configs'] = _get_referenced_column_configs(source_asset)
    # render
    return _render_asset_editor(
        asset_type=asset_type,
        original_asset_uuid=source_asset_uuid,
        asset_uuid=None,
        collective_mode='share',
        source_asset=source_asset,
        collective_vars=collective_vars
    )

def _get_referenced_column_configs(asset):
    referenced_column_uuids = asset.get_referenced_column_references(uuids_only=True)
    referenced_column_configs = get_config_from_column_uuids(referenced_column_uuids)
    for referenced_column_uuid, referenced_column_config in referenced_column_configs.items():
        if referenced_column_config['type'] == 'aggregator':
            referenced_column_configs[referenced_column_uuid]['asset_type'] = ALL_ASSET_TYPES['aggregatorcolumn']['display_1']
            # go deeper / nest
            aggregator_column = AggregatorColumn()
            if aggregator_column.load(referenced_column_uuid):
                referenced_column_configs[referenced_column_uuid]['children'] = _get_referenced_column_configs(aggregator_column)
                #referenced_column_configs = {**referenced_column_configs, **_get_referenced_column_configs(aggregator_column)}
        else:
            referenced_column_configs[referenced_column_uuid]['asset_type'] = ALL_ASSET_TYPES['column']['display_1']
    return referenced_column_configs

@bp.route('/assets/<asset_uuid>/likes', methods=['PUT', 'GET'])
@login_required
def like_asset(asset_uuid):
    collective_asset = CollectiveAsset()
    if collective_asset.load(asset_uuid):
        if request.method == 'PUT':
            collective_asset.toggle_likes()
        return json.dumps({
            'count': collective_asset.count_number_of_likes(),
            'liked_by_me': collective_asset.is_liked_by_user()
        })
    else:
        abort(404)

def _render_asset_editor(asset_type, original_asset_uuid, asset_uuid, collective_mode, source_asset=None, collective_vars=None):
    if asset_type == 'column' or asset_type == 'aggregatorcolumn':
        from sres.blueprints.table import edit_column
        return edit_column(table_uuid=source_asset.table.config['uuid'], column_uuid=original_asset_uuid, collective_mode=collective_mode, collective_asset_uuid=asset_uuid, collective_vars=collective_vars)
    elif asset_type == 'filter':
        from sres.blueprints.filter import edit_filter
        return edit_filter(filter_uuid=original_asset_uuid, collective_mode=collective_mode, collective_asset_uuid=asset_uuid, collective_vars=collective_vars)
    elif asset_type == 'portal':
        from sres.blueprints.portal import edit_portal
        return edit_portal(portal_uuid=original_asset_uuid, collective_mode=collective_mode, collective_asset_uuid=asset_uuid, collective_vars=collective_vars)
