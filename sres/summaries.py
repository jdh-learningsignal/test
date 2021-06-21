from flask import g, session, url_for, escape, current_app
from copy import deepcopy
import collections
import re
from datetime import datetime, timedelta
import time
from natsort import natsorted, ns
from bs4 import BeautifulSoup
from flask_mail import Message
import mimetypes
import os
import threading
import logging
import pandas
import numpy
from decimal import Decimal, getcontext, ROUND_HALF_UP
import json
import random

from sres.columns import Column
from sres.db import _get_db
from sres import utils
from sres.auth import is_user_administrator

def _get_natsorted_keys(list_of_keys):
    return { k:i for i,k in enumerate(natsorted(list_of_keys)) }

CALCULATION_MODES = [
    {
        'id': 'distribution',
        'title': 'Distribution',
        'hint': 'Distribution of data',
        'extra_config': [
            {
                'id': 'distribution_scale',
                'type': 'select',
                'options': [
                    {
                        'value': 'raw',
                        'display': 'Raw count'
                    },
                    {
                        'value': 'normalise',
                        'display': 'Normalise to percentage'
                    }
                ],
                'title': 'Scaling',
                'hint': 'How should the counts be scaled?',
                'default': 'raw'
            }
        ]
    },
    {
        'id': 'distribution_cut_grades',
        'title': 'Distribution (by grade boundaries)',
        'hint': 'Distribution of data by grade',
        'extra_config': [
            {
                'id': 'out_of',
                'type': 'input',
                'title': 'Values are out of',
                'hint': 'What is the max value possible?',
                'default': 100
            },
            {
                'id': 'distribution_cut_grades_scale',
                'type': 'select',
                'options': [
                    {
                        'value': 'raw',
                        'display': 'Raw count'
                    },
                    {
                        'value': 'normalise',
                        'display': 'Normalise to percentage'
                    }
                ],
                'title': 'Scaling',
                'hint': 'How should the counts be scaled?',
                'default': 'raw'
            }
        ]
    },
    {
        'id': 'distribution_cut4',
        'title': 'Distribution (4 bins)',
        'hint': 'Distribution of data in 4 bins'
    },
    {
        'id': 'distribution_cut10',
        'title': 'Distribution (10 bins)',
        'hint': 'Distribution of data in 10 bins'
    },
    {
        'id': 'mean',
        'title': 'Mean',
        'hint': 'Mean of data'
    },
    {
        'id': 'unigram',
        'title': 'Word frequency',
        'hint': 'Frequency of single words in a corpus'
    }
]

CALCULATION_MODES_GLOBAL_EXTRA_CONFIG = [
    {
        'id': 'global_extra_config_null_handling',
        'type': 'select',
        'options': [
            {
                'value': 'include',
                'display': 'Include (default)'
            },
            {
                'value': 'ignore',
                'display': 'Ignore'
            },
            {
                'value': 'zero',
                'display': 'Convert to zero'
            }
        ],
        'title': 'Undefined or empty values',
        'hint': 'How are undefined or empty values handled?',
        'default': 'include'
    }
]

PRESENTATION_MODES = [
    {
        'id': 'chart_column',
        'title': 'Column chart',
        'hint': 'Draw a column chart',
        'extra_config': [
            {
                'id': 'x_axis_title_override',
                'type': 'input',
                'title': 'X axis title',
                'hint': 'Override the default x-axis title. Leave blank for default.',
                'default': ''
            },
            {
                'id': 'y_axis_title_override',
                'type': 'input',
                'title': 'Y axis title',
                'hint': 'Override the default y-axis title. Leave blank for default.',
                'default': ''
            }
        ]
    },
    {
        'id': 'chart_pie',
        'title': 'Pie chart',
        'hint': 'Draw a pie chart'
    },
    {
        'id': 'text',
        'title': 'Text only',
        'hint': 'Show just as text'
    },
    {
        'id': 'wordcloud',
        'title': 'Word cloud',
        'hint': 'Draw a word cloud'
    },
    {
        'id': 'bullets',
        'title': 'Bulleted list',
        'hint': 'Make a bulleted list with the data',
        'extra_config': [
            {
                'id': 'list_sort_method',
                'type': 'select',
                'options': [
                    {
                        'value': 'none',
                        'display': 'No sort order (default)'
                    },
                    {
                        'value': 'alphabetical',
                        'display': 'Alphabetical'
                    },
                    {
                        'value': 'random',
                        'display': 'Randomise'
                    }
                ],
                'title': 'Sort text by',
                'hint': 'How to sort the text that is shown.',
                'default': 'alphabetical'
            }
        ]
    }
]

PRESENTATION_MODES_GLOBAL_EXTRA_CONFIG = [
    {
        'id': 'global_extra_config_presentation_magic_formatting',
        'type': 'select',
        'options': [
            {
                'value': 'data',
                'display': 'Use saved data (default)'
            },
            {
                'value': 'display',
                'display': 'Use "What to display on the form" (\'Text to display\') if available'
            }
        ],
        'title': 'Magic formatter',
        'hint': 'Apply magic formatter to the data?',
        'default': 'data'
    }
]

STOP_WORDS = ["i", "my", "you", "your", "the", "a", "an", "as", "be", "etc", "that", "it", "is", "are", "was", "am", "by", "of", "to", "and", "with", "this", "in", "on", "at", "for", "have", "had"]

def list_summaries_for_table(table_uuid, column_uuid=None):
    db = _get_db()
    _filter = {
        'table_uuid': table_uuid,
        'workflow_state':'active'
    }
    if column_uuid is not None:
        _filter['column_uuid'] = column_uuid
    summaries = list(db.summaries.find(_filter))
    ret = []
    for summary in summaries:
        _ret = {}
        for key in ['uuid', 'column_reference', 'column_uuid', 'table_uuid', 'name', 'description', 'workflow_state', 'representation_config']:
            _ret[key] = summary.get(key)
        ret.append(_ret)
    # sort
    ret = list(
        natsorted(ret, key=lambda x: x['name'], alg=ns.IGNORECASE)
    )
    # return
    return ret

def table_uuids_from_summary_uuids(summary_uuids, existing_table_uuids_list=None, order_by_prevalence=True):
    db = _get_db()
    if not summary_uuids or type(summary_uuids) is not list:
        return []
    # parse for actual summary references
    summary_uuids = [ s.replace('SMY_', '', 1) for s in summary_uuids if s.startswith('SMY_') ]
    filter = {
        'uuid': {'$in': summary_uuids}
    }
    results = db.summaries.find(filter, ['uuid', 'table_uuid'])
    results = list(results)
    # parse out just table_uuid, maintaining order
    _map = { c['uuid']: c['table_uuid'] for c in results }
    if existing_table_uuids_list is not None and type(existing_table_uuids_list) is list:
        table_uuids = existing_table_uuids_list
    else:
        table_uuids = []
    for summary_uuid in summary_uuids:
        if summary_uuid in _map.keys():
            table_uuids.append(_map[summary_uuid])
    # order by prevalence
    if order_by_prevalence:
        table_uuids = sorted(table_uuids, key=collections.Counter(table_uuids).get, reverse=True)
    # de-duplicate
    table_uuids = list(dict.fromkeys(table_uuids))
    # return
    return table_uuids

class Representation:
    
    default_config = {
        'calculation': {
            'mode': '',
            'extra_config': {
                'out_of': 100,
                'distribution_scale': 'raw',
                'distribution_cut_grades_scale': 'raw',
                'global_extra_config_null_handling': 'include'
            }
        },
        'presentation': {
            'mode': '', # e.g. chart_column, chart_pie, text
            'decimal_places': 2,
            'extra_config': {
                'global_extra_config_presentation_magic_formatting': 'data',
                'x_axis_title_override': '',
                'y_axis_title_override': '',
                'list_sort_method': 'none'
            }
        },
        'comparison': {
            'mode': 'none'
        },
        'grouping': {
            'mode': 'disabled',
            'column_reference': '',
            'comparison_mode': 'disabled'
        },
        'include_if': [],
        'exclude_if': []
    }
    
    def __init__(self):
        """Initialises the Representation."""
        
        self.db = _get_db()
        self.config = deepcopy(self.default_config)
    
    def calculate(self, column_references, grouping_values=None):
        """Calculates the representation.
        
        column_references (list of str) Can be one or more column references.
        grouping_values (list of str | None) The actual values to match against the grouping column.
        """
        
        ret = {
            'success': False,
            'messages': [],
            'data_array': [],
            'data_text': '',
            'y_axis_label': '',
            'possible_grouping_values': [],
            'grouping_column_reference': '',
            'grouping_column_name': ''
        }
        
        # load up column instances
        column_instances = {}
        column_friendly_names = []
        if type(column_references) is str:
            column_references = [ column_references ]
        for column_reference in column_references:
            column_instances[column_reference] = Column()
            if column_instances[column_reference].load(column_reference):
                column_friendly_names.append(column_instances[column_reference].get_friendly_name(show_table_info=False, get_text_only=True))
            else:
                column_instances.pop(column_reference, None)
        
        # load up grouping values
        possible_grouping_values = []
        grouping_column = Column()
        using_grouping = False
        grouping_compare_to_everyone = False
        if self.config['grouping']['mode'] == 'enabled' and self.config['grouping']['column_reference']:
            grouping_column_reference = self.config['grouping']['column_reference']
            if grouping_column.load(grouping_column_reference):
                possible_grouping_values = grouping_column.get_grouping_column_unique_groups(override_column_reference=grouping_column_reference)
                ret['possible_grouping_values'] = possible_grouping_values
                ret['grouping_column_reference'] = grouping_column_reference
                ret['grouping_column_name'] = grouping_column.get_friendly_name(show_table_info=False, get_text_only=True)
                using_grouping = True
                grouping_compare_to_everyone = True if self.config['grouping'].get('comparison_mode', 'disabled') == 'enabled' else False
        if grouping_values is not None and '$ALL$' in grouping_values:
            using_grouping = False
        
        # load up data
        data_series = pandas.Series()
        data_magic_formatter_mapping = {}
        _data_magic_formatter_mapping = []
        if using_grouping and grouping_compare_to_everyone:
            data_series_ungrouped = pandas.Series()
        else:
            data_series_ungrouped = None
        for column_reference in column_references:
            # continue
            only_column_uuids = [ column_instances[column_reference].config['uuid'] ]
            if using_grouping:
                only_column_uuids.append(grouping_column.config['uuid'])
            _source_data = column_instances[column_reference].table.export_data_to_df(
                only_column_uuids=only_column_uuids,
                do_not_rename_headers=True,
                return_just_df=True
            )
            _df = pandas.DataFrame.from_dict(_source_data['data'])
            # check there actually is data
            if column_reference not in _df.columns:
                # next column_reference please...
                continue
            if using_grouping:
                #_df = _df[_df[grouping_column_reference].isin(grouping_values)] # old way, not flexible
                if grouping_compare_to_everyone:
                    data_series_ungrouped = pandas.concat([data_series_ungrouped, _df[column_reference]], ignore_index=True)
                _df['__SRES_GROUPING_COLUMN__'] = _df[grouping_column_reference].map(utils.force_interpret_str_to_list)
                _df['__SRES_GROUPING_MATCH__'] = _df['__SRES_GROUPING_COLUMN__'].apply(lambda x: any(y in x for y in grouping_values))
                _df = _df[_df['__SRES_GROUPING_MATCH__']]
            # interpret multi-select data if relevant
            _new_data_series = _df[column_reference]
            if column_instances[column_reference].is_multiple_selection_allowed():
                _temp_data, _topology = utils.flatten_list( _new_data_series.to_list() )
                _new_data_series = pandas.Series(_temp_data)
            # put the data together into a pandas series
            data_series = pandas.concat([data_series, _new_data_series], ignore_index=True)
            # gather metadata for magic formatting
            _data_magic_formatter_mapping.extend( column_instances[column_reference].get_select_from_list_elements() )
        for v in _data_magic_formatter_mapping: 
            if v['value'] not in data_magic_formatter_mapping.keys():
                data_magic_formatter_mapping[ v['value'] ] = v['display']
        
        data = {
            'values': {},
            'values_ungrouped': {},
            'series_label': '',
            'series_label_ungrouped': ''
        }
        
        # determine any magic formatting needed to present data
        if self.config['presentation'].get('extra_config', {}).get('global_extra_config_presentation_magic_formatting'):
            magic_formatter = self.config['presentation']['extra_config'].get('global_extra_config_presentation_magic_formatting')
            if magic_formatter == 'data':
                # do nothing
                pass
            elif magic_formatter == 'display':
                for _value, _display in data_magic_formatter_mapping.items():
                    data_series.replace(_value, _display, inplace=True)
        
        # calculation
        if self.config['calculation']['mode'] in ['distribution', 'distribution_cut_grades', 'distribution_cut4', 'distribution_cut10']:
            sorted_keys = None
            normalise_to_percent = False
            undefined_conversion = 'include' # what to do with undefined values
            # load up extra config
            if self.config['calculation']['mode'] == 'distribution' and self.config['calculation']['extra_config'].get('distribution_scale') == 'normalise':
                normalise_to_percent = True
            if self.config['calculation']['mode'] == 'distribution_cut_grades' and self.config['calculation']['extra_config'].get('distribution_cut_grades_scale') == 'normalise':
                normalise_to_percent = True
            if self.config['calculation']['extra_config'].get('global_extra_config_null_handling'):
                undefined_conversion = self.config['calculation']['extra_config'].get('global_extra_config_null_handling')
            # null/undefined conversions?
            if undefined_conversion == 'ignore':
                data_series.replace('', numpy.nan, inplace=True)
                data_series.dropna(inplace=True)
            elif undefined_conversion == 'zero':
                data_series.replace('', 0, inplace=True)
                data_series.fillna(0, inplace=True)
            # calculate
            if self.config['calculation']['mode'] == 'distribution':
                # calculate
                vc = data_series.value_counts()
                if using_grouping and grouping_compare_to_everyone:
                    vc_ungrouped = data_series_ungrouped.value_counts()
                # normalise?
                if normalise_to_percent:
                    vc = ( vc / vc.sum() * 100 ).round(1)
                    if using_grouping and grouping_compare_to_everyone:
                        vc_ungrouped = ( vc_ungrouped / vc_ungrouped.sum() * 100 ).round(1)
            elif self.config['calculation']['mode'] == 'distribution_cut_grades':
                def _distribution_cut_grades(_data_series, out_of=None):
                    bins = pandas.IntervalIndex.from_breaks([0, 50, 65, 75, 85, 100], closed='left')
                    if out_of:
                        return pandas.cut((pandas.to_numeric(_data_series.dropna().values) / out_of) * 100, bins).value_counts()
                    else:
                        return pandas.cut(pandas.to_numeric(_data_series.dropna().values), bins).value_counts()
                # calculate
                if self.config['calculation']['extra_config']['out_of']:
                    out_of = float(self.config['calculation']['extra_config']['out_of'])
                    vc = _distribution_cut_grades(data_series, out_of)
                    if using_grouping and grouping_compare_to_everyone:
                        vc_ungrouped = _distribution_cut_grades(data_series_ungrouped, out_of)
                else:
                    vc = _distribution_cut_grades(data_series)
                    if using_grouping and grouping_compare_to_everyone:
                        vc_ungrouped = _distribution_cut_grades(data_series_ungrouped)
                # normalise?
                if normalise_to_percent:
                    vc = ( vc / vc.sum() * 100 ).round(1)
                    if using_grouping and grouping_compare_to_everyone:
                        vc_ungrouped = ( vc_ungrouped / vc_ungrouped.sum() * 100 ).round(1)
            elif self.config['calculation']['mode'] == 'distribution_cut4':
                vc = pandas.cut(pandas.to_numeric(data_series.dropna().values), 4).value_counts()
                if using_grouping and grouping_compare_to_everyone:
                    vc_ungrouped = pandas.cut(pandas.to_numeric(data_series_ungrouped.dropna().values), 4).value_counts()
            elif self.config['calculation']['mode'] == 'distribution_cut10':
                vc = pandas.cut(pandas.to_numeric(data_series.dropna().values), 10).value_counts()
                if using_grouping and grouping_compare_to_everyone:
                    vc_ungrouped = pandas.cut(pandas.to_numeric(data_series_ungrouped.dropna().values), 10).value_counts()
            # stringify index
            vc.index = vc.index.map(str)
            if using_grouping and grouping_compare_to_everyone:
                vc_ungrouped.index = vc_ungrouped.index.map(str)
            # sort naturally
            if sorted_keys is None:
                sorted_keys = _get_natsorted_keys(vc.index.tolist())
            vc.sort_index(inplace=True, key=lambda x: x.map(sorted_keys))
            if using_grouping and grouping_compare_to_everyone:
                if sorted_keys is None:
                    sorted_keys = _get_natsorted_keys(vc_ungrouped.index.tolist())
                vc_ungrouped.sort_index(inplace=True, key=lambda x: x.map(sorted_keys))
            # store
            scale_label = 'Percent' if normalise_to_percent is True else 'Count'
            data['values'] = vc.to_dict()
            data['series_label'] = scale_label
            ret['y_axis_label'] = scale_label
            data['text'] = vc.to_frame().rename(columns={0: scale_label}).to_html(classes='sres-summary-representation-table table')
            if using_grouping:
                data['series_label'] = 'Group'
                if grouping_compare_to_everyone:
                    data['values_ungrouped'] = vc_ungrouped.to_dict()
                    data['series_label_ungrouped'] = 'Everyone'
        elif self.config['calculation']['mode'] == 'mean':
            # calculate
            series = pandas.to_numeric(data_series, errors='coerce')
            mean = series.mean()
            # rounding
            getcontext().rounding = ROUND_HALF_UP
            dp = Decimal(10) ** -int(self.config['presentation']['decimal_places'])
            mean = Decimal(mean).quantize(dp)
            # store
            data['values'] = {
                'mean': mean
            }
            data['series_label'] = 'Mean'
            ret['y_axis_label'] = 'Mean'
            data['text'] = mean
        elif self.config['calculation']['mode'] == 'unigram':
            data_series.replace(pandas.np.nan, '', inplace=True)
            corpus = list(data_series.values)
            corpus = [ str(x) for x in corpus ]
            corpus = ' '.join(corpus)
            corpus = corpus.lower()
            for punc in ['.', ',', ':', ';', '(', ')', '"', '[', ']']:
                corpus = corpus.replace(punc, ' ')
            unigrams = corpus.split(' ')
            unigrams = [ s for s in unigrams if s and s not in STOP_WORDS ]
            unigrams_count = collections.Counter(unigrams)
            # order
            unigrams_count = dict(natsorted(unigrams_count.items(), key=lambda kv: kv[1], reverse=True))
            # store
            data['values'] = unigrams_count
            data['series_label'] = 'Frequency'
            ret['y_axis_label'] = 'Frequency'
            data['text'] = pandas.DataFrame.from_dict(unigrams_count, orient='index').rename(columns={0:'Frequency'}).to_html(classes='sres-summary-representation-table table')
            
        # determine how to return the data
        if self.config['presentation']['mode'].startswith('chart_'):
            if using_grouping and grouping_compare_to_everyone:
                dist_keys = list(set(list(data['values'].keys()) + list(data['values_ungrouped'].keys())))
                dist_keys = natsorted(dist_keys, alg=ns.IGNORECASE)
                data_array = [[
                    '; '.join(column_friendly_names),
                    data['series_label'],
                    data['series_label_ungrouped']
                ]]
                for k in dist_keys:
                    data_array.append([ k, data['values'].get(k, ''), data['values_ungrouped'].get(k, '') ])
            else:
                data_array = [[
                    '; '.join(column_friendly_names),
                    data['series_label']
                ]]
                for k, v in data['values'].items():
                    data_array.append([k, v])
            ret['data_array'] = data_array
        elif self.config['presentation']['mode'] == 'text':
            ret['data_text'] = utils.bleach_user_input_html( str(data['text']) )
        elif self.config['presentation']['mode'] == 'bullets':
            ret['data_array'] = [ utils.bleach_user_input_html(t) for t in list(data['values'].keys()) ]
            if self.config['presentation'].get('extra_config', {}).get('list_sort_method') == 'alphabetical':
                ret['data_array'] = natsorted( ret['data_array'], alg=ns.IGNORECASE )
            elif self.config['presentation'].get('extra_config', {}).get('list_sort_method') == 'random':
                random.shuffle( ret['data_array'] )
        elif self.config['presentation']['mode'] == 'wordcloud':
            data_array = []
            for k, v in data['values'].items():
                data_array.append({
                    'text': utils.bleach_user_input_html(k),
                    'weight': v
                })
            ret['data_array'] = data_array
        
        # determine overrides
        if self.config['presentation'].get('extra_config', {}).get('x_axis_title_override'):
            ret['x_axis_label'] = self.config['presentation']['extra_config'].get('x_axis_title_override')
        if self.config['presentation'].get('extra_config', {}).get('y_axis_title_override'):
            ret['y_axis_label'] = self.config['presentation']['extra_config'].get('y_axis_title_override')
        
        ret['column_friendly_names'] = '; '.join(column_friendly_names)
        
        return ret

class Summary:
    
    default_config = {
        'uuid': None,
        'table_uuid': None,
        'column_uuid': None,
        'column_reference': None,
        'name': '',
        'description': '',
        'created': None,
        'modified': None,
        'workflow_state': 'active',
        'representation_config': deepcopy(Representation.default_config)
    }
    
    def __init__(self, preloaded_column=None):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
        if preloaded_column is not None and type(preloaded_column) is Column:
            self.column = preloaded_column
        else:
            self.column = Column()
    
    def load(self, summary_uuid=None):
        find_summary = {}
        find_summary['uuid'] = utils.clean_uuid(summary_uuid)
        results = self.db.summaries.find(find_summary)
        results = list(results)
        if len(results) == 1:
            if self._load(results[0]):
                if self.column._id is not None or self.column.load(self.config['column_uuid']):
                    return True
        return False
    
    def _load(self, db_result):
        self._id = db_result['_id']
        for key, value in self.default_config.items():
            try:
                if isinstance(self.config[key], collections.Mapping):
                    # is dict-type so try and merge
                    self.config[key] = {**value, **db_result[key]}
                else:
                    self.config[key] = db_result[key]
            except:
                self.config[key] = value
        return True

    def update(self):
        if self.column.table.is_user_authorised():
            result = self.db.summaries.update_one({'uuid': self.config['uuid']}, {'$set': self.config})
            return result.acknowledged
        else:
            return False
        
    def create(self):
        if is_user_administrator('list') or is_user_administrator('super'):
            pass
        else:
            return False
        self.config['uuid'] = utils.create_uuid()
        result = self.db.summaries.insert_one(self.config)
        if result.acknowledged and self.load(self.config['uuid']):
            return self.config['uuid']
        else:
            return None
    
    def clone(self, add_cloned_notice=True):
        """
            Clones the current summary. Returns the new uuid (string) if successful,
            or None if not.
        """
        summary_clone = Summary()
        if summary_clone.create():
            source_summary_config = deepcopy(self.config)
            # remove keys that should not be cloned
            del source_summary_config['uuid']
            # update some keys
            source_summary_config['created'] = datetime.now()
            if add_cloned_notice:
                source_summary_config['name'] = f"Clone of {source_summary_config['name']}"
                source_summary_config['description'] = f"[Cloned] {source_summary_config['description']}"
            summary_clone.config = {**summary_clone.config, **source_summary_config}
            # save
            if summary_clone.update():
                return summary_clone.config['uuid']
        return None
    
    def delete(self):
        self.config['workflow_state'] = 'deleted'
        return self.update()
    
    def archive(self):
        self.config['workflow_state'] = 'archived'
        return self.update()
    
    def is_grouping_active(self):
        if self.config['representation_config'].get('grouping', {}).get('mode') == 'enabled':
            grouping_column_reference = self.config['representation_config'].get('grouping', {}).get('column_reference')
            if grouping_column_reference:
                return True
        return False
    
    def is_grouping_comparison_mode_active(self):
        if self.is_grouping_active() and self.config['representation_config'].get('grouping', {}).get('comparison_mode') == 'enabled':
            return True
        return False
    
    def get_grouping_column_reference(self):
        return self.config['representation_config'].get('grouping', {}).get('column_reference')
    
    def calculate_representation(self, grouping_values=None):
        representation = Representation()
        representation.config = self.config['representation_config']
        # is there grouping active?
        if self.is_grouping_active():
            # calculate and return
            return representation.calculate(self.config['column_reference'], grouping_values)
        # calculate and return
        return representation.calculate(self.config['column_reference'])
    
    
    
    
    
    