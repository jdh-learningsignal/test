from flask import g, session, current_app, url_for
import collections
from copy import deepcopy
from datetime import datetime, time
import logging
import json

from sres.db import _get_db
from sres.auth import get_auth_user_oid, is_user_administrator
from sres.users import oids_to_usernames, usernames_to_oids, oids_to_display_names
from sres import utils
from sres.columns import Column, get_config_from_column_uuids
from sres.aggregatorcolumns import AggregatorColumn
from sres.tables import _get_column_meta_for_select_array, Table

ALL_ASSET_TYPES = {
    'column': {
        'display': "Columns",
        'display_1': "Column",
        'db_collection': 'columns',
        'display_icon': 'columns'
    },
    'aggregatorcolumn': {
        'display': "Aggregator columns",
        'display_1': "Aggregator column",
        'db_collection': 'columns',
        'display_icon': 'cubes'
    },
    'filter': {
        'display': "Filters",
        'display_1': "Filter",
        'db_collection': 'filters',
        'display_icon': 'comments'
    },
    'portal': {
        'display': "Portals",
        'display_1': "Portal",
        'db_collection': 'portals',
        'display_icon': 'window-maximize'
    }
}

def search_assets(term, order_by='shared_on', sort_direction=1, only_asset_types=ALL_ASSET_TYPES.keys(), mine_only=False):
    db = _get_db()
    if len(only_asset_types) == 0:
        return []
    # set up compulsory filters
    base_compulsory_filters = [
        {'workflow_state': 'active',},
        #{'visibility': {'$ne': 'secret'}},
        {'type': {'$in': only_asset_types}}
    ]
    if mine_only:
        base_compulsory_filters.append({
            'shared_by': get_auth_user_oid()
        })
    # perform db search
    if term is not None and len(term):
        # search db.collective_assets
        filters = []
        for key in ['name', 'description']:
            filters.append({
                key: {'$regex': term, '$options': 'i'}
            })
        collective_assets = list(
            db.collective_assets.find(
                {
                    '$or': filters,
                    '$and': base_compulsory_filters
                }
            ).sort(
                [(order_by, sort_direction)]
            )
        )
        # search assets themselves
        for db_collection in [ ALL_ASSET_TYPES[t]['db_collection'] for t in only_asset_types ]:
            filters = []
            for key in ['name', 'description']:
                filters.append({
                    key: {'$regex': term, '$options': 'i'}
                })
            filters = {
                '$or': filters,
                'workflow_state': 'collective'
            }
            assets_shared_to_collective = list(db[db_collection].find(filters, ['uuid']))
            assets_shared_asset_uuids = [ a['uuid'] for a in assets_shared_to_collective ]
            # get the corresponding collective_asset(s)
            collective_assets.extend(
                list(
                    db.collective_assets.find(
                        {
                            'source_asset_uuid': {'$in': assets_shared_asset_uuids},
                            '$and': base_compulsory_filters
                        }
                    ).sort(
                        [(order_by, sort_direction)]
                    )
                )
            )
    else:
        collective_assets = list(db.collective_assets.find({
            '$and': base_compulsory_filters
        }).sort([(order_by, sort_direction)]))
    # build return
    ret = {}
    for collective_asset in collective_assets:
        if collective_asset['visibility'] == 'secret' and get_auth_user_oid() not in collective_asset['shared_by']:
            continue
        if collective_asset['workflow_state'] != 'active':
            continue
        if collective_asset['uuid'] not in ret.keys():
            shared_by_friendly_name_mapping = oids_to_display_names(collective_asset['shared_by'])
            ret[collective_asset['uuid']] = {
                'uuid': collective_asset['uuid'],
                'source_asset_uuid': collective_asset['source_asset_uuid'],
                'name': collective_asset['name'],
                'type': collective_asset['type'],
                'display_type': ALL_ASSET_TYPES[collective_asset['type']]['display_1'],
                'visibility': collective_asset['visibility'],
                'icon': ALL_ASSET_TYPES[collective_asset['type']]['display_icon'],
                'description': collective_asset['description'],
                'shared_on': collective_asset['shared_on'].strftime('%Y-%m-%dT%H:%M:%S'),
                'shared_by': [ shared_by_friendly_name_mapping[o]['first_and_last'] for o in collective_asset['shared_by'] ],
                'workflow_state': collective_asset['workflow_state'],
                'liked_by': collective_asset.get('liked_by', []),
                'liked_by_me': True if get_auth_user_oid() in collective_asset.get('liked_by', []) else False,
                'liked_by_count': collective_asset.get('liked_by_count', len(collective_asset.get('liked_by', [])))
            }
    assets = [ a for uuid, a in ret.items() ]
    return assets

def list_assets(limit=21, order_by='liked_by_count', sort_direction=-1, only_asset_types=ALL_ASSET_TYPES.keys(), mine_only=False):
    if sort_direction not in [1, -1]:
        return []
    if order_by not in ['shared_on', 'liked_by_count', 'name', 'type']:
        return []
    assets = search_assets(term=None, order_by=order_by, sort_direction=sort_direction, only_asset_types=only_asset_types, mine_only=mine_only)
    if limit is None:
        return assets
    else:
        return assets[0:int(limit)]

def copy_asset_for_user(asset_uuid, table_uuid, override_user_oid=None, referenced_assets_uuids=None):
    collective_asset = CollectiveAsset()
    ret = {
        'success': False,
        'status_code': None,
        'collective_asset': None, # CollectiveAsset
        'messages': [], # tuples of text, type,
        'new_asset_uuid': None
    }
    if collective_asset.load(asset_uuid):
        if collective_asset.is_user_authorised_viewer() or collective_asset.is_user_authorised_editor(override_user_oid=override_user_oid):
            # check admin permissions
            if not is_user_administrator('super', user_oid=override_user_oid):
                if collective_asset.asset_type in ['column', 'aggregatorcolumn', 'portal'] and not is_user_administrator('list', user_oid=override_user_oid):
                    ret['status_code'] = 403
                    ret['messages'].append(("Sorry, you need to be a list administrator to complete this action.", "warning"))
                    return ret
                if collective_asset.asset_type in ['filter'] and not is_user_administrator('filter', user_oid=override_user_oid):
                    ret['status_code'] = 403
                    ret['messages'].append(("Sorry, you need to be a filter administrator to complete this action.", "warning"))
                    return ret
            else:
                pass # all good
            table = Table()
            if table.load(table_uuid) and table.is_user_authorised(user_oid=override_user_oid):
                pass # all good
            else:
                ret['messages'].append(("Sorry, you need to be an administrator for the list {} to complete this action.".format(table.get_full_name()), "warning"))
                ret['status_code'] = 403
                return ret
            # make the copy
            if referenced_assets_uuids is None:
                referenced_assets_uuids = []
            copy_res = collective_asset.make_copy_for_user(
                table_uuid=table_uuid,
                referenced_assets_uuids=referenced_assets_uuids,
                override_user_oid=override_user_oid
            )
            if copy_res['success']:
                ret['messages'].append((f"Copy succeeded. You are now editing your very own copy of the {collective_asset.asset_type}.", "success"))
                for message in copy_res['messages']:
                    ret['messages'].append((message[0], message[1]))
                # show warning if re-referencing needed
                if len(copy_res['referenced_assets_copied']) > 0 and collective_asset.asset_type in ['filter', 'portal']:
                    ret['messages'].append((
                        Markup(
                            "<span class=\"fa fa-exclamation-triangle text-danger animated flash infinite slower\"></span> {} related columns were also copied. You must now <a href=\"#\" class=\"btn btn-primary\" data-sres-trigger-click=\".sres-column-referencer-show\" data-sres-target-table-uuid=\"{}\">run the re-referencing wizard</a> so that the column references in this {} point to these columns. <strong>You will only see this message once, after which it will not be possible to perform re-referencing in this way, and any conditional settings may be irreparably broken.</strong>".format(
                                len(copy_res['referenced_assets_copied']),
                                table_uuid,
                                collective_asset.asset_type
                            )
                        ), "info"
                    ))
                if len(copy_res['nested_assets_copied']) > 0:
                    for nested_asset_copied in copy_res['nested_assets_copied']:
                        ret['messages'].append((
                            Markup(
                                "<span class=\"fa fa-exclamation-triangle text-danger animated flash infinite slower\"></span> A related {display_type} {asset_name} <em>that has related columns of its own</em> was also copied. You must now <a href=\"{edit_url}\" class=\"btn btn-primary\" target=\"_blank\">edit this {display_type} and save it</a> so that its column references are correct. <strong>You will only see this message once, after which it will not be possible to access the editor in this way, and any column references may be irreparably broken.</strong>".format(
                                    display_type=nested_asset_copied['display_type'],
                                    asset_name=nested_asset_copied['asset_name'],
                                    edit_url=url_for('table.edit_column', table_uuid=table_uuid, column_uuid=nested_asset_copied['asset_uuid'], from_collective_asset_uuid=nested_asset_copied['uuid'])
                                )
                            ), "info"
                        ))
                # render downstream editors
                ret['success'] = True
                ret['collective_asset'] = collective_asset
                ret['new_asset_uuid'] = copy_res['new_asset_uuid']
                ret['status_code'] = 200
                return ret
            else:
                logging.debug(str(copy_res))
                ret['messages'].append(("An unexpected error occured while making a copy of this asset.", "warning"))
                ret['status_code'] = 500
                return ret
        else:
            ret['messages'].append(("Unauthorised.", "warning"))
            ret['status_code'] = 403
            return ret
    else:
        ret['messages'].append(("Unable to load asset.", "warning"))
        ret['status_code'] = 404
        return ret

class CollectiveAsset:
    
    default_config = {
        'uuid': None,
        'type': None, # column|aggregatorcolumn|filter|portal
        'original_asset_uuid': None, # the uuid of the original asset this was shared from
        'source_asset_uuid': None, # the uuid of the asset in db.<asset_type>s that contains the asset config
        'name': '',
        'description': '',
        'shared_on': None,
        'shared_by': [],
        'workflow_state': 'active', # active|inactive|deleted
        'visibility': 'institution', # institution|(public)-notimplemented|secret
        'referenced_columns_config': {},
        'liked_by': [],
        'liked_by_count': 0,
        'parent_collective_asset_uuid': None
    }
    
    def __init__(self, asset_type=None):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
        self.asset_type = asset_type
        self._init_asset(asset_type)
    
    def _init_asset(self, asset_type):
        self.asset = self._init_an_asset(asset_type)
        
    def _init_an_asset(self, asset_type):
        if asset_type == 'column':
            from sres.columns import Column
            return Column()
        elif asset_type == 'aggregatorcolumn':
            from sres.aggregatorcolumns import AggregatorColumn
            return AggregatorColumn()
        elif asset_type == 'filter':
            from sres.filters import Filter
            return Filter()
        elif asset_type == 'portal':
            from sres.portals import Portal
            return Portal()
        else:
            return None
    
    def load(self, asset_uuid):
        filter = {
            'uuid': asset_uuid
        }
        results = self.db.collective_assets.find(filter)
        results = list(results)
        if len(results) == 1:
            self._id = results[0]['_id']
            for key, value in self.default_config.items():
                try:
                    if isinstance(self.config[key], collections.Mapping):
                        # is dict-type so try and merge
                        self.config[key] = {**value, **results[0][key]}
                    else:
                        self.config[key] = results[0][key]
                except:
                    self.config[key] = value
        else:
            return False
        # try to load the asset
        if self.asset_type is None:
            self.asset_type = self.config['type']
        self._init_asset(self.asset_type)
        self.load_asset()
        # return
        return True
    
    def load_from_source_asset(self, source_asset_type, source_asset_uuid):
        if source_asset_type not in ALL_ASSET_TYPES.keys():
            return False
        res = list(self.db.collective_assets.find({'source_asset_uuid': source_asset_uuid}))
        if len(res) == 1:
            return self.load(res[0]['uuid'])
        else:
            return False
    
    def update(self):
        self.config['liked_by_count'] = len(self.config['liked_by'])
        result = self.db.collective_assets.update_one(
            {'uuid': self.config['uuid']}, 
            {'$set': self.config}
        )
        return result.acknowledged
    
    def create(self):
        self.config['uuid'] = utils.create_uuid()
        result = self.db.collective_assets.insert_one(self.config)
        self._id = result.inserted_id
        if self.load(self.config['uuid']):
            return self.config['uuid']
        else:
            return None
    
    def delete(self):
        self.config['workflow_state'] = 'deleted'
        return self.update()
    
    def get_authorised_usernames(self):
        if self.config['shared_by']:
            return [v for k, v in oids_to_usernames(self.config['shared_by']).items()]
        else:
            return []
    
    def is_user_authorised_editor(self, override_user_oid=None):
        if (override_user_oid or get_auth_user_oid()) in self.config['shared_by'] and self.config['workflow_state'] != 'deleted':
            return True
        if is_user_administrator('super', user_oid=override_user_oid):
            return True
        return False
    
    def is_user_authorised_viewer(self):
        if self.config['workflow_state'] == 'active':
            return True
        return False
    
    def increment_likes(self, increment=1):
        if self.is_liked_by_user() and increment == -1:
            self.config['liked_by'].remove(get_auth_user_oid())
            return self.update()
        elif not self.is_liked_by_user() and increment == 1:
            self.config['liked_by'].append(get_auth_user_oid())
            return self.update()
    
    def toggle_likes(self):
        if self.is_liked_by_user():
            return self.increment_likes(increment=-1)
        else:
            return self.increment_likes()
    
    def is_liked_by_user(self):
        return get_auth_user_oid() in self.config['liked_by']
    
    def count_number_of_likes(self):
        return len(self.config['liked_by'])
    
    def set_new_asset(self, asset):
        """Sets the asset for the current CollectiveAsset using a loaded asset instance.
            First creates a clone of the supplied asset.
        """
        cloned_asset_uuid = asset.clone(add_cloned_notice=False)
        if cloned_asset_uuid:
            self._init_asset(self.asset_type)
            if self.asset.load(cloned_asset_uuid):
                # update any asset-specific configs
                self.asset.config['workflow_state'] = 'collective'
                if self.asset_type in ['column', 'aggregatorcolumn']:
                    self.asset.config['table_uuid'] = 'collective'
                elif self.asset_type == 'filter':
                    self.asset.config['run_history'] = []
                self.asset.update()
                self.asset.is_collective_asset = True
                # update metadata about shared asset
                self.config['type'] = self.asset_type
                self.config['original_asset_uuid'] = asset.config['uuid']
                self.config['source_asset_uuid'] = cloned_asset_uuid
                self.config['shared_on'] = datetime.now()
                self.config['shared_by'] = [get_auth_user_oid()]
                return self.update()
            else:
                return False
        else:
            return False
    
    def load_asset(self):
        if self.asset is None:
            self._init_asset(self.asset_type)
        if self.config['source_asset_uuid']:
            if self.asset.load(self.config['source_asset_uuid']):
                self.asset.is_collective_asset = True
                return True
            else:
                return False
        else:
            return False
    
    def get_referenced_assets(self, uuids_only=False):
        referenced_assets = list(
            self.db.collective_assets.find({
                'parent_collective_asset_uuid': self.config['uuid'],
                'workflow_state': 'active'
            })
        )
        if uuids_only:
            return [ a['uuid'] for a in referenced_assets ]
        else:
            return referenced_assets
        
    def set_metadata_from_form(self, form, save_immediately=True, process_share_referenced_columns=False):
        # metadata
        self.config['name'] = form.get('collective_asset_name', '')
        self.config['description'] = form.get('collective_asset_description', '')
        self.config['workflow_state'] = 'active' if form.get('collective_asset_active') else 'inactive'
        self.config['visibility'] = form.get('collective_asset_visibility', 'secret')
        try:
            self.config['shared_by'] = usernames_to_oids(form.getlist('authorised_collective_administrators')) or self.config['shared_by']
        except:
            pass
        if save_immediately:
            metadata_res = self.update()
        else:
            metadata_res = True
        # process the sharing of referenced columns specified in form?
        if process_share_referenced_columns:
            # parse what's been requested by the user
            requested_references_to_save = []
            for key in form.keys():
                if key.startswith('collective_asset_referenced_column_'):
                    requested_references_to_save.append(key.replace('collective_asset_referenced_column_', ''))
            # get the referenced columns afresh
            referenced_column_uuids = self.asset.get_referenced_column_references(uuids_only=True)
            referenced_column_configs = get_config_from_column_uuids(referenced_column_uuids)
            # iterate through referenced columns
            for referenced_column_uuid, referenced_column_config in referenced_column_configs.items():
                if referenced_column_uuid in requested_references_to_save:
                    column = Column()
                    if column.load(referenced_column_uuid) and column.is_user_authorised():
                        if column.config['type'] == 'aggregator':
                            collective_column = CollectiveAsset('aggregatorcolumn')
                            column = AggregatorColumn()
                            column.load(referenced_column_uuid)
                        else:
                            collective_column = CollectiveAsset('column')
                        if collective_column.create() and collective_column.set_new_asset(column):
                            collective_column.config['parent_collective_asset_uuid'] = self.config['uuid']
                            collective_column.config['shared_by'] = self.config['shared_by']
                            if collective_column.set_metadata_from_form({
                                'collective_asset_name': column.config['name'],
                                'collective_asset_description': '<p>{}</p><p>{}</p>'.format(
                                    column.config['description'],
                                    "Shared together with {} <a href=\"{}\">{}</a>".format(
                                        self.asset_type,
                                        url_for('collective.show_asset', asset_uuid=self.config['uuid'], _external=True),
                                        self.config['name']
                                    )
                                ),
                                'collective_asset_visibility': 'secret',
                                'collective_asset_active': 'active'
                            }):
                                # great!
                                metadata_res = metadata_res and True
                            else:
                                # not so great
                                logging.debug('Error on {}'.format(column.config['uuid']))
                    else:
                        logging.debug('cant load or auth')
                        metadata_res = metadata_res and False
                        continue
                    if column.config['type'] == 'aggregator':
                        # recurse
                        r1 = collective_column.set_metadata_from_form(
                            {
                                **form,
                                **{
                                    'collective_asset_name': collective_column.config['name'],
                                    'collective_asset_description': '<p>{}</p><p>{}</p>'.format(
                                        collective_column.config['description'],
                                        "Shared together with {} <a href=\"{}\">{}</a>".format(
                                            self.asset_type,
                                            url_for('collective.show_asset', asset_uuid=self.config['uuid'], _external=True),
                                            self.config['name']
                                        )
                                    ),
                                    'collective_asset_visibility': 'secret',
                                    'collective_asset_active': 'active'
                                }
                            }, 
                            process_share_referenced_columns=True
                        )
                        r2 = collective_column.update_referenced_columns_config()
                        #logging.debug('recursion {}'.format(collective_column.config['uuid']))
                        #logging.debug(str(r1))
                        #logging.debug(str(r2))
        return metadata_res
    
    def update_referenced_columns_config(self, save_immediately=True):
        referenced_column_uuids = self.asset.get_referenced_column_references(uuids_only=True)
        columns = self.db.columns.find({'uuid': {'$in': referenced_column_uuids}})
        columns = list(columns)
        self.config['referenced_columns_config'] = { c['uuid']: c for c in columns }
        if save_immediately:
            return self.update()
        else:
            return True
    
    def get_select_array_for_referenced_columns(self):
        ret = []
        for column_uuid, column_config in self.config['referenced_columns_config'].items():
            ret = _get_column_meta_for_select_array(
                column={'config':column_config},
                table=None,
                ret=ret,
                show_collapsed_multientry_option=True,
                hide_multientry_subfields=False
            )        
        return ret
    
    def make_copy_for_user(self, table_uuid=None, referenced_assets_uuids=None, override_user_oid=None):
        """Performs the copy process.
            
            table_uuid (str or None)
            referenced_assets_uuids (list of str uuids or None) any related assets to also copy over
        """
        ret = {
            'success': False,
            'new_asset': None,
            'new_asset_uuid': None,
            'messages': [],
            'referenced_assets_copied': [],
            'nested_assets_copied': []
        }
        if self.asset_type in ['filter', 'portal']:
            new_asset_uuid = self.asset.clone(add_cloned_notice=False, set_user_as_sole_administrator=True, user_oid=override_user_oid)
            if new_asset_uuid:
                new_asset = self._init_an_asset(self.asset_type)
                if new_asset.load(new_asset_uuid):
                    # update admins
                    new_asset.config['administrators'] = [get_auth_user_oid()]
                    # update filter-specific things
                    if self.asset_type == 'filter':
                        new_asset.reset_tracking_record_config()
                    # save to db
                    if new_asset.update(override_user_oid=override_user_oid):
                        # so far so good
                        ret['success'] = True
                        ret['new_asset_uuid'] = new_asset_uuid
                        ret['new_asset'] = new_asset
                    else:
                        logging.warning(f'Error updating new_asset type {self.asset_type} uuid {new_asset_uuid}')
                        return ret
                else:
                    logging.warning(f'Error loading newly created cloned asset')
                    return ret
            else:
                logging.warning(f'Error making new asset by cloning')
                return ret
        elif self.asset_type in ['column', 'aggregatorcolumn']:
            new_asset_uuid = self.asset.clone(target_table_uuid=table_uuid, add_cloned_notice=False)
            if new_asset_uuid:
                new_asset = self._init_an_asset(self.asset_type)
                if new_asset.load(new_asset_uuid):
                    new_asset.config['table_uuid'] = table_uuid
                    new_asset.config['workflow_state'] = 'active'
                    new_asset.update()
                    ret['new_asset'] = new_asset
                ret['new_asset_uuid'] = new_asset_uuid
                ret['success'] = True
            else:
                return ret
        
        referenced_assets_mapping = {}
        if referenced_assets_uuids is not None:
            copied_assets_new_uuids = []
            # loop through this asset's actual referenced assets
            for _asset_uuid in self.get_referenced_assets(uuids_only=True):
                collective_asset = CollectiveAsset()
                if _asset_uuid in referenced_assets_uuids:
                    referenced_asset_uuid = _asset_uuid
                    if collective_asset.load(referenced_asset_uuid):
                        #logging.debug('copying {} {} {} {}'.format(
                        #    collective_asset.config['uuid'],
                        #    collective_asset.config['name'],
                        #    collective_asset.config['type'],
                        #    collective_asset.asset_type
                        #))
                        copy_res = collective_asset.make_copy_for_user(table_uuid=table_uuid, referenced_assets_uuids=referenced_assets_uuids, override_user_oid=override_user_oid)
                        if copy_res['success']:
                            # add to original-to-new mapping
                            referenced_assets_mapping[collective_asset.config['original_asset_uuid']] = copy_res['new_asset_uuid']
                            # update messages and records
                            ret['messages'].append(("Successfully made a copy of {} {}.".format(
                                ALL_ASSET_TYPES[collective_asset.asset_type]['display_1'].lower(),
                                collective_asset.config['name']
                            ), "success"))
                            ret['messages'].extend(copy_res['messages'])
                            copied_assets_new_uuids.append(copy_res['new_asset_uuid'])
                            copied_assets_new_uuids.extend(copy_res['referenced_assets_copied'])
                            if collective_asset.asset_type == 'aggregatorcolumn':
                                ret['nested_assets_copied'].append({
                                    'name': collective_asset.config['name'],
                                    'asset_name': copy_res['new_asset'].config['name'] if copy_res['new_asset'] else '??',
                                    'uuid': collective_asset.config['uuid'],
                                    'asset_uuid': copy_res['new_asset'].config['uuid'] if copy_res['new_asset'] else '??',
                                    'type': collective_asset.config['type'],
                                    'display_type': ALL_ASSET_TYPES[collective_asset.config['type']]['display_1'].lower()
                                })
                            ret['nested_assets_copied'].extend(copy_res['nested_assets_copied'])
                        else:
                            logging.error('Error making copy of referenced asset [{}] parent [{}].'.format(referenced_asset_uuid, self.config['uuid']))
                    else:
                        logging.error('Error while making a copy - could not load referenced asset [{}] parent [{}].'.format(referenced_asset_uuid, self.config['uuid']))
            ret['referenced_assets_copied'] = copied_assets_new_uuids
        
        # if aggregator, try and automatically swap/remap
        if self.asset_type == 'aggregatorcolumn':
            #logging.debug('attempting auto swap!!')
            #logging.debug('referenced_assets_mapping')
            #logging.debug(referenced_assets_mapping)
            #logging.debug(str(self.asset.config['aggregation_options']))
            try:
                new_asset = AggregatorColumn()
                if new_asset.load(new_asset_uuid):
                    new_asset.swap_aggregation_configuration_column_references(
                        mapping=referenced_assets_mapping,
                        remove_unmapped_references=True
                    )
                    r = new_asset.update()
                    #logging.debug('RRRR {}'.format(r))
            except Exception as e:
                logging.exception(e)
                #logging.debug('error automatically remapping')
            #logging.debug('just tried to auto swap!!')
            #logging.debug(str(self.asset.config['aggregation_options']))
            pass
        
        return ret
    
    def get_friendly_shared_by_names(self, as_text=True):
        shared_by_friendly_name_mapping = oids_to_display_names(self.config['shared_by'])
        names = [ shared_by_friendly_name_mapping[o]['first_and_last'] for o in self.config['shared_by'] ]
        if as_text:
            return ', '.join(names)
        else:
            return names