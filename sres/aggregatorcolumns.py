from flask import g, session
from copy import deepcopy
import re
from datetime import datetime, time
import json
import cexprtk
from bs4 import BeautifulSoup
import collections
import logging
import math
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, getcontext

from sres.columns import Column, table_uuids_from_column_references
from sres.db import _get_db
from sres import utils
from sres.studentdata import substitute_text_variables
from sres.conditions import Conditions
from sres import cexprtk_ext
from sres.change_history import get_change_history

SIMPLE_AGGREGATORS = [
    {
        "name": "average",
        "display": "Average",
        "hint": "Calculate the average",
        "parameters": []
    },
    {
        "name": "median",
        "display": "Median",
        "hint": "Calculate the median",
        "parameters": []
    },
    {
        "name": "sum",
        "display": "Sum",
        "hint": "Calculate the sum",
        "parameters": []
    },
    {
        "name": "mode",
        "display": "Mode",
        "hint": "Calculate the mode",
        "parameters": []
    },
    {
        "name": "averageaggressive",
        "display": "Aggressive average",
        "hint": "Calculate the average of all number-like elements anywhere in the data",
        "parameters": []
    },
    {
        "name": "sumaggressive",
        "display": "Aggressive sum",
        "hint": "Calculate the sum of all number-like elements anywhere in the data",
        "parameters": []
    },
    {
        "name": "count",
        "display": "Count numeric",
        "hint": "Count of how many records contain numbers",
        "parameters": []
    },
    {
        "name": "counta",
        "display": "Count non empty",
        "hint": "Count how many records are not empty",
        "parameters": []
    },
    {
        "name": "countblank",
        "display": "Count empty",
        "hint": "Count how many records are empty",
        "parameters": []
    },
    {
        "name": "countif",
        "display": "Count if",
        "hint": "Count how many records are equal to a defined value",
        "parameters": [
            {
                "name": "value",
                "display": "Count if record is equal to"
            }
        ]
    },
    {
        "name": "countifstartswith",
        "display": "Count if starts with",
        "hint": "Count how many records start with a defined value",
        "parameters": [
            {
                "name": "term",
                "display": "Count if record starts with (case insensitive)"
            }
        ]
    },
    {
        "name": "countifcontains",
        "display": "Count if contains",
        "hint": "Count how many records contain a defined value",
        "parameters": [
            {
                "name": "term",
                "display": "Count if record contains (case insensitive)"
            }
        ]
    },
    {
        "name": "countifmatchregex",
        "display": "Count if matches regular expression",
        "hint": "Count how many records match a defined regular expression",
        "parameters": [
            {
                "name": "term",
                "display": "Regular expression"
            }
        ]
    },
    {
        "name": "countallmatchregex",
        "display": "Count all occurrences of regular expression match",
        "hint": "Count all matches a defined regular expression anywhere in the data",
        "parameters": [
            {
                "name": "term",
                "display": "Regular expression"
            }
        ]
    },
    {
        "name": "clone",
        "display": "Clone",
        "hint": "Just copy the value without any calculation or transformation - only one source column should be selected",
        "parameters": []
    },
    {
        "name": "highest_average",
        "display": "Average of highest",
        "hint": "Calculate the average of n highest records",
        "parameters": [
            {
                "name": "n",
                "display": "n"
            }
        ]
    },
    {
        "name": "highest_sum",
        "display": "Sum of highest",
        "hint": "Calculate the sum of n highest records",
        "parameters": [
            {
                "name": "n",
                "display": "n"
            }
        ]
    },
    {
        "name": "lowest_average",
        "display": "Average of lowest",
        "hint": "Calculate the average of n lowest records",
        "parameters": [
            {
                "name": "n",
                "display": "n"
            }
        ]
    },
    {
        "name": "lowest_sum",
        "display": "Sum of lowest",
        "hint": "Calculate the sum of n lowest records",
        "parameters": [
            {
                "name": "n",
                "display": "n"
            }
        ]
    },
    {
        "name": "concatenate",
        "display": "Concatenate",
        "hint": "Join records together, separated by an optional separator",
        "parameters": [
            {
                "name": "separator",
                "display": "The separator to use between each record"
            }
        ]
    }
]

def _comparator_to_operator(comparator):
    if comparator == "=":
        return "equal"
    elif comparator == "<>":
        return "not_equal"
    elif comparator == ">":
        return "greater"
    elif comparator == "<":
        return "less"
    elif comparator == ">=":
        return "greater_or_equal"    
    elif comparator == "<=":
        return "less_or_equal"    
    elif comparator == "LIKE":
        return "contains"    
    elif comparator == "NOT LIKE":
        return "not_contains"    
    elif comparator == "STARTSWITH":
        return "begins_with"
    elif comparator == "ENDSWITH":
        return "ends_with"
    elif comparator == "IN":
        return "in"    
    elif comparator == "NOT IN":
        return "not_in"    
    elif comparator == "BETWEEN":
        return "between"    
    elif comparator == "NOT BETWEEN":
        return "not_between"         
    elif comparator == "IS NULL":
        return "is_empty"    
    elif comparator == "IS NOT NULL":
        return "is_not_empty"   

def _read_legacy_case_builder_cases(cases):
    """
        Converts legacy case builder cases into, amongst other things, queryBuilder 'rules' format.
    
        Returns list of cases [
            {
                'content': 'content of case',
                'rules': 
            }
        ]
    """
    ret = []
    for case in cases:
        case = {k.lower(): v for k, v in case.items()}
        new_case = {}
        new_case['content'] = case['content']
        new_case['rules'] = {
            'condition': case['conditionscombiner'].upper(),
            'rules': []
        }
        new_case['default_case'] = '1' if 'default_case' in case.keys() and case['default_case'] == '1' else '0'
        for condition in case['conditions']:
            condition = {k.lower(): v for k, v in condition.items()}
            new_case['rules']['rules'].append({
                'id': condition['column'],
                'operator': _comparator_to_operator(condition['comparator']),
                'value': str(condition['value'])
            })
        ret.append(new_case)
    return ret
    
def find_aggregators_of_columns(source_column_uuids):
    aggregator_column_uuids = []
    db = _get_db()
    for source_column_uuid in source_column_uuids:
        source_column = db.columns.find({'uuid': source_column_uuid})
        source_column = list(source_column)
        if len(source_column) and source_column[0]['aggregated_by']:
            aggregator_column_uuids.extend(source_column[0]['aggregated_by'])
    # deduplicate
    aggregator_column_uuids = list(dict.fromkeys(aggregator_column_uuids))
    return aggregator_column_uuids

class AggregatorColumn(Column):
    
    def __init__(self):
        super().__init__()
    
    def get_case_builder_cases(self):
        """
            Returns a list of cases (dicts). A bit of overhead to deal with legacy storage schema.
            
            Returns [
                {
                    content (str) Content returned in this case
                    default_case (str) '1' or '0' if this is the default case
                    rules (dict) {
                        queryBuilder configuration
                    }
                }
            ]
        """
        ret = []
        if 'aggregator_type_case_builder_cases' in self.config['aggregation_options'].keys():
            cases = self.config['aggregation_options']['aggregator_type_case_builder_cases']
            if utils.is_json(cases) or isinstance(cases, list):
                try:
                    if isinstance(cases, list):
                        pass
                    else:
                        cases = json.loads(cases)
                    if len(cases):
                        # try to detect if it is legacy or not
                        if 'CONDITIONS' in cases[0].keys() or 'conditions' in cases[0].keys():
                            # probably legacy; new approach has 'rules' key in keeping with queryBuilder nomenclature
                            ret = _read_legacy_case_builder_cases(cases)
                        else:
                            if isinstance(cases, list):
                                ret = cases
                            elif isinstance(cases, str) and utils.is_json(cases):
                                ret = json.loads(cases)
                            else:
                                ret = cases
                except:
                    raise
                    pass
        return ret
    
    def update_other_columns_aggregated_by(self, direction='insert', override_username=None):
        """
            Updates the aggregated_by key for other columns that are source columns for this aggregator column.
        """
        ret = {
            'successful': [],
            'failed': [],
            'messages': []
        }
        # brute force remove references to this aggregator everywhere
        self.db.columns.update_many(
            { },
            {
                '$pull': {'aggregated_by': self.config['uuid']}
            }
        )
        # find columns that will be aggregated by this aggregator
        aggregated_column_references = self._get_aggregated_column_references()
        # load aggregated_by for each of these columns and add this current 
        # aggregator column if not exists in existing aggregated_by array
        for aggregated_column_reference in aggregated_column_references:
            column = Column()
            if column.load(aggregated_column_reference):
                if self.config['aggregation_options']['recalculate_trigger'] != 'manual':
                    # parse to save aggregated_by
                    if self.config['uuid'] not in column.config['aggregated_by'] and direction == 'insert':
                        column.config['aggregated_by'].append(self.config['uuid'])
                    elif self.config['uuid'] in column.config['aggregated_by'] and direction == 'delete':
                        column.config['aggregated_by'] = [c for c in column.config['aggregated_by'] if c != self.config['uuid']]
                    # request db update
                    if column.update(override_username=override_username):
                        ret['successful'].append(column.config['uuid'])
                    else:
                        ret['failed'].append(column.config['uuid'])
                        logging.error('failed updating aggregated_by for [{}]'.format(column.config['uuid']))
                        ret['messages'].append(("Could not update source column {}. You may not have the appropriate permissions".format(column.get_friendly_name()), "warning"))
                else:
                    if self.config['uuid'] not in column.config['aggregated_by']:
                        # indicate success because this aggregator's uuid has already been pulled
                        ret['successful'].append(column.config['uuid'])
                    else:
                        logging.error('aggregated_by for [{}] still contains [{}]'.format(column.config['uuid'], self.config['uuid']))
                        ret['failed'].append(column.config['uuid'])
        ret['successful'] = list(dict.fromkeys(ret['successful']))
        ret['failed'] = list(dict.fromkeys(ret['failed']))
        return ret
    
    def get_referenced_table_uuids(self, order_by_prevalence=True, deduplicate=True, user_must_be_admin=False, get_from_all_methods=True):
        """
            Overrides super. Returns a list of string uuids of tables that correspond to columns referenced
            in the current column.
        """
        all_column_references = self.get_referenced_column_references(
            order_by_prevalence=order_by_prevalence, 
            deduplicate=deduplicate,
            get_from_all_methods=get_from_all_methods
        )
        # figure out tables
        all_table_uuids = table_uuids_from_column_references(all_column_references, user_must_be_admin)
        return all_table_uuids
    
    def get_referenced_column_references(self, order_by_prevalence=True, deduplicate=True, uuids_only=False, get_from_all_methods=True):
        """Overrides super"""
        all_column_references = []
        all_column_references.extend(super().get_referenced_column_references(order_by_prevalence, deduplicate))
        all_column_references.extend(self._get_aggregated_column_references(get_from_all_methods=get_from_all_methods))
        # order by preference
        if order_by_prevalence:
            all_column_references = [r for r, c in collections.Counter(all_column_references).most_common() for r in [r] * c]
        # de-duplicate
        if deduplicate:
            all_column_references = list(dict.fromkeys(all_column_references))
        # if uuids_only
        if uuids_only:
            all_column_uuids = [ r.split('.')[0] for r in all_column_references ]
            all_column_uuids = list(dict.fromkeys(all_column_uuids))
            return all_column_uuids
        else:
            return all_column_references
    
    def _get_aggregated_column_references(self, base_uuid_only=False, get_from_all_methods=False):
        """
            Returns a list of the source columns (string column references) that are aggregated 
            by this aggregator column. Takes into consideration the active aggregation method,
            unless argument get_from_all_methods == True.
        """
        
        def _get_ids_from_rules(rules):
            ids = []
            # get basic column reference from rule
            for rule in rules:
                if 'rules' in rule.keys():
                    ids.extend(_get_ids_from_rules(rule['rules']))
                elif rule.get('id'):
                    ids.append(rule['id'])
                # parse rule value for column references
                if rule.get('value') is not None and '$' in rule.get('value', ''):
                    column_references_in_value = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, rule['value'])
                    if len(column_references_in_value) > 0:
                        ids.extend(utils.clean_delimiter_from_column_references(column_references_in_value))
            return ids
        
        ret = []
        if get_from_all_methods or self.config['aggregation_options']['method'] in 'average,sum,median,mode,averageaggressive,sumaggressive,clone,highest_average,highest_sum,lowest_average,lowest_sum,count,counta,countblank,countif,countifstartswith,countifcontains,countifmatchregex,countallmatchregex,concatenate,mapper'.split(','):
            ret.extend(self.config['aggregation_options']['attributes'])
        if get_from_all_methods or self.config['aggregation_options']['method'] == 'mathematical_operations':
            ret.extend(re.findall(utils.BASE_COLUMN_REFERENCE_PATTERN, self.config['aggregation_options']['aggregator_type_mathematical_operations_formula']))
        if get_from_all_methods or self.config['aggregation_options']['method'] == 'case_builder':
            # parse cases for column references
            for case in self.get_case_builder_cases():
                if not case:
                    continue
                if case['default_case'] != '1' and case.get('rules') is not None:
                    ret.extend(_get_ids_from_rules(case['rules']['rules']))
                # parse data yield values for column references
                if case.get('content') and '$' in case.get('content'):
                    ret.extend(re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, case.get('content')))
        if get_from_all_methods or self.config['aggregation_options']['method'] == 'self_peer_review':
            ret.append(self.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column'])
            ret.append(self.config['aggregation_options']['aggregator_type_self_peer_review_score_column'])
        # remove blanks?!?!
        ret = [c for c in ret if c.strip() != '']
        # if necessary, just return the uuid
        if base_uuid_only:
            ret = [c.split('.')[0] for c in ret]
        # de-duplicate
        ret = list(dict.fromkeys(ret))
        # return
        return ret
    
    def swap_aggregation_configuration_column_references(self, mapping, remove_unmapped_references=False):
        """Swaps column references in aggregation configurations
            
            mapping (dict) keys: original column reference; values: new column reference
        """
        
        # process simple aggregator column selection
        original_attributes = deepcopy(self.config['aggregation_options']['attributes'])
        mapped_attributes = []
        new_attributes = []
        for attribute in original_attributes:
            _attribute_is_mapped = False
            for original_column_uuid, new_column_uuid in mapping.items():
                if original_column_uuid in attribute:
                    _attribute_is_mapped = True
                    break
            if _attribute_is_mapped or not remove_unmapped_references:
                new_attributes.append(attribute.replace(original_column_uuid, new_column_uuid))
        self.config['aggregation_options']['attributes'] = deepcopy(new_attributes)
        
        # process mathematical operations formula
        for original_column_uuid, new_column_uuid in mapping.items():
            self.config['aggregation_options']['aggregator_type_mathematical_operations_formula'] = self.config['aggregation_options']['aggregator_type_mathematical_operations_formula'].replace(
                original_column_uuid,
                new_column_uuid
            )
        
        # process case builder cases
        # define helper
        def _swap_ids_in_rules(rules, mapping):
            # swap column reference from rule 'id'
            for i, rule in enumerate(rules):
                if 'rules' in rule.keys():
                    _swap_ids_in_rules(rule['rules'], mapping)
                elif rule.get('id'):
                    if rule['id'] in mapping.keys():
                        rules[i]['id'] = mapping[rule['id']]
                # swap column references in the rule 'value'
                if rule.get('value') is not None and '$' in rule.get('value', ''):
                    for original_column_uuid, new_column_uuid in mapping.items(): 
                        rules[i]['value'].replace(original_column_uuid, new_column_uuid)
        for i, case in enumerate(self.config['aggregation_options']['aggregator_type_case_builder_cases']):
            _swap_ids_in_rules(self.config['aggregation_options']['aggregator_type_case_builder_cases'][i]['rules']['rules'], mapping)
        
        # process self and peer review configs
        for original_column_uuid, new_column_uuid in mapping.items():
            self.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column'] = self.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column'].replace(
                original_column_uuid,
                new_column_uuid
            )
            self.config['aggregation_options']['aggregator_type_self_peer_review_score_column'] = self.config['aggregation_options']['aggregator_type_self_peer_review_score_column'].replace(
                original_column_uuid,
                new_column_uuid
            )
            
        
    def calculate_aggregation(self, identifiers=[], columns_already_traversed=[], auth_user_override='', forced=False, threaded_aggregation=False, preloaded_columns=None):
        """
            Calculates the aggregation for this column for the specified identifiers.
            
            identifiers (list of strings) Typically SIDs of the students whose data needs to be recalculated.
                If not provided, will default to all students in the table where this aggregator belongs.
            columns_already_traversed (list of strings uuids)
            auth_user_override (string username)
            forced (boolean) True if a manual recalculation is being requested.
            threaded_aggregation (boolean) Whether to use threaded and multiprocessing aggregation.
            preloaded_columns
            
            Returns dict of dicts, keyed by each identifier.
                {
                    success (boolean)
                    errors (list of tuples)
                    aggregated_value (string)
                }
        """
        #logging.debug(f"starting calculate_aggregation for [{self.config['uuid']}] [{self.config['name']}], already traversed [{columns_already_traversed}]")
        #logging.debug(f"workflow state: {self.config['workflow_state']}")
        # if no identifiers specified, then work on everyone
        if len(identifiers) == 0:
            identifiers = self.table.get_all_students_sids()
        # if this aggregator is not active, then don't aggregate!
        if self.config['workflow_state'] != 'active':
            return { i: {'success': False, 'errors': [], 'aggregated_value': '' } for i in identifiers }
        # check if need to actually recalculate
        if not forced and self.config['aggregation_options']['recalculate_trigger'] == 'manual':
            logging.debug(self.config['uuid'] + ' not calculating')
            return { i: {'success': False, 'errors': [], 'aggregated_value': '' } for i in identifiers }
        # proceed
        _ALL_SIMPLE_AGGREGATOR_NAMES = [a['name'] for a in SIMPLE_AGGREGATORS] + ['mapper']
        from sres.studentdata import StudentData
        t0 = datetime.now()
        # set up local variables
        ret = {}
        student_data = StudentData(self.table)
        # set up preloaded_columns helper
        if preloaded_columns is None:
            preloaded_columns = {}
        # protect against circular references
        #logging.debug('AAA columns_already_traversed for [{}]: [{}]'.format(self.config['uuid'], str(columns_already_traversed)))
        if self.config['uuid'] in columns_already_traversed:
            print('CIRCULAR', self.config['uuid'])
            for identifier in identifiers:
                student_data._reset()
                ret[identifier] = {
                    'success': False,
                    'errors': ["Circular reference identified"]
                }
                # clear data
                if student_data.find_student(identifier):
                    student_data.set_data(
                        column_uuid=self.config['uuid'], 
                        data='?!?', 
                        skip_aggregation=True, 
                        ignore_active=True,
                        commit_immediately=True,
                        auth_user_override=auth_user_override
                    )
            raise ArithmeticError('Circular reference detected {} | {}'.format(self.config['uuid'], json.dumps(columns_already_traversed)))
            #return ret
        source_column_uuids = self._get_aggregated_column_references(base_uuid_only=True)
        source_column_references = self.db.columns.find( # direct to db to reduce overhead
            {'uuid': {'$in': source_column_uuids}}, 
            ['type', 'uuid', 'table_uuid']
        )
        source_column_references = list(source_column_references)
        #logging.debug(self.config['uuid'] + ' ' + str(source_column_uuids) + ' ' + str(source_column_references))
        for source_column_reference in source_column_references:
            if source_column_reference['type'] == 'aggregator':
                columns_already_traversed.append(source_column_reference['uuid'])
        #logging.debug('columns_already_traversed for [{}]: [{}]'.format(self.config['uuid'], str(columns_already_traversed)))
        
        # prepare some helper items
        if self.config['aggregation_options']['method'] in _ALL_SIMPLE_AGGREGATOR_NAMES:
            simple_aggregator_column_helpers = {}
            for column_to_aggregate in self.config['aggregation_options']['attributes']:
                if column_to_aggregate:
                    if column_to_aggregate in preloaded_columns.keys():
                        simple_aggregator_column_helpers[column_to_aggregate] = preloaded_columns[column_to_aggregate]
                    else:
                        simple_aggregator_column_helpers[column_to_aggregate] = Column()
                        simple_aggregator_column_helpers[column_to_aggregate].load(column_to_aggregate)
                        preloaded_columns[column_to_aggregate] = simple_aggregator_column_helpers[column_to_aggregate]
        elif self.config['aggregation_options']['method'] == 'mathematical_operations':
            mathematical_operations_aggregator_extension_column_helpers = {}
            expr = self.config['aggregation_options']['aggregator_type_mathematical_operations_formula']
            expr = BeautifulSoup(expr, 'html.parser').get_text()
            expr = utils.clean_exprtk_expression(expr)
            parse_results = cexprtk_ext.parse_ext_functions(expr)
            for column_reference in parse_results['column_references']:
                if column_reference in preloaded_columns.keys():
                    mathematical_operations_aggregator_extension_column_helpers[column_reference] = preloaded_columns[column_reference]
                else:
                    mathematical_operations_aggregator_extension_column_helpers[column_reference] = Column()
                    mathematical_operations_aggregator_extension_column_helpers[column_reference].load(column_reference)
                    preloaded_columns[column_reference] = mathematical_operations_aggregator_extension_column_helpers[column_reference]
        elif self.config['aggregation_options']['method'] == 'case_builder':
            case_builder_column_helpers = {}
            for source_column_uuid in source_column_uuids:
                if source_column_uuid in preloaded_columns.keys():
                    case_builder_column_helpers[source_column_uuid] = preloaded_columns[source_column_uuid]
                else:
                    case_builder_column_helpers[source_column_uuid] = Column()
                    case_builder_column_helpers[source_column_uuid].load(source_column_uuid)
                    preloaded_columns[source_column_uuid] = case_builder_column_helpers[source_column_uuid]
        elif self.config['aggregation_options']['method'] == 'self_peer_review':
            self_peer_review_column_helpers = {}
            for column_reference in [ self.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column'], self.config['aggregation_options']['aggregator_type_self_peer_review_score_column'] ]:
                if column_reference and column_reference in preloaded_columns.keys():
                    self_peer_review_column_helpers[column_reference] = preloaded_columns[column_reference]
                else:
                    self_peer_review_column_helpers[column_reference] = Column()
                    self_peer_review_column_helpers[column_reference].load(column_reference)
                    preloaded_columns[column_reference] = self_peer_review_column_helpers[column_reference]
        #logging.debug(self.config['uuid'] + ' time for setting up calculate_aggregation' + str((datetime.now() - t0).total_seconds()))
        
        # determine the blank replacement
        blank_replacement = 0 if self.config['aggregation_options']['blank_handling'] == 'zero' else ''
        
        # loop through each identifier and perform the aggregation for each
        for identifier in identifiers:
            student_data._reset()
            ret[identifier] = {
                'success': False,
                'errors': [],
                'final_value': ''
            }
            t0 = datetime.now()
            if student_data.find_student(identifier):
                if self.config['aggregation_options']['method'] in _ALL_SIMPLE_AGGREGATOR_NAMES:
                    data_array = []
                    for column_to_aggregate in self.config['aggregation_options']['attributes']:
                        if not column_to_aggregate:
                            continue
                        data_temp_set = []
                        # column_to_aggregate is the source column, so column_helper == the source column
                        if simple_aggregator_column_helpers[column_to_aggregate] and simple_aggregator_column_helpers[column_to_aggregate].config['table_uuid']:
                            # get data
                            if 'r' in self.config['aggregation_options']['axes'] and simple_aggregator_column_helpers[column_to_aggregate].has_multiple_report_mode_enabled():
                                _data = student_data.get_data_for_entry(
                                    column=simple_aggregator_column_helpers[column_to_aggregate],
                                    report_index=-1,
                                    parse_if_json=False
                                )
                                data_temp_set = _data['all_reports_data']
                            elif 't' in self.config['aggregation_options']['axes']:
                                # set up datetime filters if needed
                                only_before = None
                                only_after = None
                                try:
                                    if self.config['aggregation_options']['t_axis_source_limit'] == 'between':
                                        only_after = self.config['aggregation_options']['t_axis_source_limit_from']
                                        only_before = self.config['aggregation_options']['t_axis_source_limit_to']
                                except Exception as e:
                                    logging.exception(e)
                                # get change history
                                ch = student_data.get_change_history(
                                    column_uuids=[simple_aggregator_column_helpers[column_to_aggregate].config['uuid']],
                                    only_before=only_before,
                                    only_after=only_after
                                )
                                if len(ch):
                                    if self.config['aggregation_options']['t_axis_source'] == 'earliest':
                                        data_temp_set.append(ch[-1].get('new_value'))
                                    elif self.config['aggregation_options']['t_axis_source'] == 'latest':
                                        data_temp_set.append(ch[0].get('new_value'))
                                    elif self.config['aggregation_options']['t_axis_source'] in ['user_earliest', 'user_latest']:
                                        users = []
                                        if self.config['aggregation_options']['t_axis_source'] == 'user_earliest':
                                            ch_iter = reversed(ch)
                                        else:
                                            ch_iter = ch
                                        for r in ch_iter:
                                            if r.get('auth_user') not in users:
                                                data_temp_set.append(r.get('new_value'))
                                                users.append(r.get('auth_user'))
                                    else:
                                        data_temp_set.extend([h.get('new_value') for h in ch if h.get('new_value') is not None])
                            else:
                                if simple_aggregator_column_helpers[column_to_aggregate].config['table_uuid'] == self.config['table_uuid']:
                                    data_temp = student_data.get_data(
                                        column_uuid=simple_aggregator_column_helpers[column_to_aggregate].config['uuid'],
                                        preloaded_column=simple_aggregator_column_helpers[column_to_aggregate]
                                    )
                                    if data_temp['success']:
                                        data_temp = data_temp['data']
                                    else:
                                        # uh oh
                                        # print('FAILED to get_data', identifier, simple_aggregator_column_helpers[column_to_aggregate].config['uuid'])
                                        data_temp = blank_replacement
                                        pass
                                else:
                                    # getting crosslist data!!!!
                                    student_data_x = StudentData(simple_aggregator_column_helpers[column_to_aggregate].config['table_uuid'])
                                    if student_data_x.find_student(identifier):
                                        data_temp = student_data_x.get_data(
                                            column_uuid=simple_aggregator_column_helpers[column_to_aggregate].config['uuid'],
                                            preloaded_column=simple_aggregator_column_helpers[column_to_aggregate]
                                        )
                                        if data_temp['success']:
                                            data_temp = data_temp['data']
                                        else:
                                            # uh oh
                                            # print('FAILED to crosslist get_data', identifier, simple_aggregator_column_helpers[column_to_aggregate].config['uuid'])
                                            data_temp = blank_replacement
                                            pass
                                    else:
                                        # cannot find student in the other list!
                                        continue
                                data_temp_set = [data_temp]
                            
                            #logging.debug('dts ' + str(student_data.config['sid']))
                            #logging.debug(str(data_temp_set))
                            
                            for data_temp in data_temp_set:
                                # get subdata if multientry
                                if simple_aggregator_column_helpers[column_to_aggregate].subfield is not None:
                                    try:
                                        if not isinstance(data_temp, list):
                                            try:
                                                data_temp = json.loads(data_temp)
                                            except:
                                                pass
                                        data_temp = data_temp[simple_aggregator_column_helpers[column_to_aggregate].subfield]
                                    except:
                                        # log it
                                        #logging.debug("Problem getting multientry subfield [{}] data [{}] [{}] [{}]".format(
                                        #    simple_aggregator_column_helpers[column_to_aggregate].subfield,
                                        #    str(data_temp),
                                        #    self.config['uuid'],
                                        #    self.table.config['uuid']
                                        #))
                                        data_temp = blank_replacement
                                # check blanks
                                if (data_temp == '' or data_temp is None) and blank_replacement != '':
                                    data_temp = blank_replacement
                                # save data to array
                                if self.config['aggregation_options']['method'] in 'sum,average,median,highest_average,highest_sum,lowest_average,lowest_sum,count'.split(','):
                                    if utils.is_number(data_temp):
                                        data_array.append(float(data_temp))
                                elif self.config['aggregation_options']['method'] in ['sumaggressive', 'averageaggressive']:
                                    numbers = re.findall(
                                        '((\-)?[0-9]+(\.[0-9]+)?)', 
                                        str(data_temp)
                                    )
                                    for number in numbers:
                                        if utils.is_number(number[0]): # necesssary because re.findall returns a tuple, with elements for each regex group
                                            data_array.append(number[0])
                                elif self.config['aggregation_options']['method'] == 'counta':
                                    if data_temp is not None and str(data_temp) != '':
                                        data_array.append(data_temp)
                                elif self.config['aggregation_options']['method'] in ['clone', 'mapper']:
                                    data_array.append(data_temp)
                                    # clone and mapper only take one source column, so ignore the others if >1 specified
                                    break
                                elif self.config['aggregation_options']['method'] == 'countblank':
                                    if data_temp == '' or data_temp is None:
                                        data_array.append(data_temp)
                                elif self.config['aggregation_options']['method'] == 'countif':
                                    if data_temp == self.config['aggregation_options']['aggregator_type_simple_countif_parameter_value']:
                                        data_array.append(data_temp)
                                elif self.config['aggregation_options']['method'] == 'countifstartswith':
                                    if data_temp.startswith(self.config['aggregation_options']['aggregator_type_simple_countifstartswith_parameter_term']):
                                        data_array.append(data_temp)
                                elif self.config['aggregation_options']['method'] == 'countifcontains':
                                    if self.config['aggregation_options']['aggregator_type_simple_countifcontains_parameter_term'].lower() in str(data_temp).lower():
                                        data_array.append(data_temp)
                                elif self.config['aggregation_options']['method'] == 'countifmatchregex':
                                    if re.search(
                                        self.config['aggregation_options']['aggregator_type_simple_countifmatchregex_parameter_term'],
                                        str(data_temp),
                                        flags=re.IGNORECASE
                                    ):
                                        data_array.append(data_temp)
                                elif self.config['aggregation_options']['method'] == 'countallmatchregex':
                                    data_array.extend(
                                        re.findall(
                                            self.config['aggregation_options']['aggregator_type_simple_countallmatchregex_parameter_term'],
                                            str(data_temp),
                                            flags=re.IGNORECASE
                                        )
                                    )
                                elif self.config['aggregation_options']['method'] in ['concatenate', 'mode']:
                                    data_array.append(str(data_temp))
                                else:
                                    # misconfiguration
                                    logging.warning("Aggregation error, could not make data_array. [{}] [{}]".format(
                                        column_to_aggregate,
                                        identifier
                                    ))
                        else:
                            logging.error("Aggregator [{}] [{}] could not be loaded. [{}]".format(
                                column_to_aggregate,
                                simple_aggregator_column_helpers[column_to_aggregate].config['uuid'],
                                str(self.config)
                            ))
                            continue
                    #logging.debug(str(data_array))
                    #logging.debug(self.config['uuid'] + ' time for making data_array ' + str((datetime.now() - t0).total_seconds()))
                    # calculate aggregated value
                    final_value = ''
                    if self.config['aggregation_options']['method'] in 'sum,sumaggressive'.split(','):
                        final_value = utils.list_sum(data_array)
                    elif self.config['aggregation_options']['method'] in ['average', 'averageaggressive', 'mode', 'median']:
                        if len(data_array):
                            if self.config['aggregation_options']['method'] in ['average', 'averageaggressive']:
                                final_value = utils.list_avg(data_array)
                            elif self.config['aggregation_options']['method'] == 'mode':
                                final_value = utils.list_mode(data_array)
                            elif self.config['aggregation_options']['method'] == 'median':
                                final_value = utils.list_median(data_array)
                        else:
                            final_value = ''
                    elif self.config['aggregation_options']['method'] == 'clone':
                        final_value = data_array[0] if data_array else ''
                    elif self.config['aggregation_options']['method'] == 'mapper':
                        try:
                            final_value = self.config['aggregation_options']['aggregator_type_mapper_outputs'][self.config['aggregation_options']['aggregator_type_mapper_inputs'].index(data_array[0])]
                        except:
                            final_value = ''
                    elif self.config['aggregation_options']['method'] in 'count,counta,countblank,countif,countifstartswith,countifcontains,countifmatchregex,countallmatchregex'.split(','):
                        final_value = len(data_array)
                    elif self.config['aggregation_options']['method'] in ['highest_average', 'lowest_average']:
                        _dir = self.config['aggregation_options']['method'].replace('_average', '')
                        n = self.config['aggregation_options']['aggregator_type_simple_{}_average_parameter_n'.format(_dir)]
                        if utils.is_number(n) and len(data_array):
                            n = int(n)
                            data_array = utils.list_nums(sorted(
                                data_array,
                                reverse=True if _dir == 'highest' else False
                            ))
                            final_value = utils.list_avg(data_array[:n])
                        else:
                            final_value = ''
                    elif self.config['aggregation_options']['method'] in ['highest_sum', 'lowest_sum']:
                        _dir = self.config['aggregation_options']['method'].replace('_sum', '')
                        n = self.config['aggregation_options']['aggregator_type_simple_{}_sum_parameter_n'.format(_dir)]
                        if utils.is_number(n):
                            n = int(n)
                            data_array = utils.list_nums(sorted(
                                data_array, 
                                reverse=True if _dir == 'highest' else False
                            ))
                            final_value = utils.list_sum(data_array[:n])
                    elif self.config['aggregation_options']['method'] == 'concatenate':
                        final_value = self.config['aggregation_options']['aggregator_type_simple_concatenate_parameter_separator'].join(data_array)
                    else:
                        # misconfiguration
                        print('aggregation error, could not calculate final_value')
                    #logging.debug(self.config['uuid'] + ' time for calculating final_value ' + str((datetime.now() - t0).total_seconds()))
                elif self.config['aggregation_options']['method'] == 'mathematical_operations':
                    expr = self.config['aggregation_options']['aggregator_type_mathematical_operations_formula']
                    expr = BeautifulSoup(expr, 'html.parser').get_text()
                    expr = utils.clean_exprtk_expression(expr)
                    # if any extension functions are included, call them
                    if mathematical_operations_aggregator_extension_column_helpers:
                        expr = cexprtk_ext.substitute_ext_function_result(
                            expr=expr,
                            identifier=identifier,
                            preloaded_student_data=student_data,
                            preloaded_columns=mathematical_operations_aggregator_extension_column_helpers
                        )
                    # simply use substitute text variables to swap out column references for values!!!
                    final_expr = substitute_text_variables(
                        input=expr,
                        identifier=identifier,
                        default_table_uuid=self.config['table_uuid'],
                        preloaded_student_data=student_data,
                        blank_replacement=blank_replacement,
                        preloaded_columns=mathematical_operations_aggregator_extension_column_helpers
                    )['new_text']
                    # evaluate
                    try:
                        final_value = cexprtk.evaluate_expression(final_expr, {})
                    except:
                        print('err final_expr', final_expr)
                        final_value = ''
                elif self.config['aggregation_options']['method'] == 'case_builder':
                    cases = self.get_case_builder_cases()
                    final_value = ''
                    case_holds_true = False
                    default_case_value = ''
                    for case in cases:
                        if case['rules']:
                            conditions = Conditions(
                                identifier=identifier,
                                conditions=case['rules'],
                                student_data=student_data,
                                preloaded_columns=case_builder_column_helpers
                            )
                            if conditions.evaluate_conditions(blank_replacement=blank_replacement):
                                case_holds_true = True
                                final_value = case['content']
                                break # break upon first truthful case and don't keep processing
                        else:
                            # case['rules'] is blank - either misconfiguration or default case
                            if case['default_case'] == '1':
                                default_case_value = case['content']
                            continue
                    if not case_holds_true:
                        # none of the cases 
                        final_value = default_case_value
                    # check if data fields exist (and therefore substitutions required)
                    if final_value and '$' in final_value:
                        # potential column reference
                        _case_builder_final_value_column_references = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, final_value)
                        if len(_case_builder_final_value_column_references) > 0:
                            # add any extra column helpers as necessary
                            for _case_builder_final_value_column_reference in _case_builder_final_value_column_references:
                                if _case_builder_final_value_column_reference not in case_builder_column_helpers.keys():
                                    _column = Column()
                                    if _column.load(_case_builder_final_value_column_reference):
                                        case_builder_column_helpers[_case_builder_final_value_column_reference] = _column
                            # request the substitution
                            _substituted_final_value = substitute_text_variables(
                                input=final_value,
                                identifier=identifier,
                                default_table_uuid=self.table.config['uuid'],
                                preloaded_student_data=student_data,
                                preloaded_columns=case_builder_column_helpers
                            )['new_text']
                            final_value = _substituted_final_value
                elif self.config['aggregation_options']['method'] == 'self_peer_review':
                    final_value = {
                        'number_of_scores_submitted_for_all_members': 0,
                        'number_of_scores_submitted_for_other_members': 0,
                        'percentage_of_scores_submitted_for_all_members': 0,
                        'percentage_of_scores_submitted_for_other_members': 0,
                        'average_score_by_this_member_for_all_members': '',
                        'average_score_by_this_member_for_other_members': '',
                        'average_score_by_all_members_for_this_member': '',
                        'total_score_by_all_members_for_this_member': '',
                        'average_score_by_other_members_for_this_member': '',
                        'average_score_by_all_members_for_all_members': '',
                        'average_score_by_all_members_for_other_members': '',
                        'average_score_by_other_members_for_other_members': '',
                        'score_by_this_member_for_this_member': '',
                        'sapa': '',
                        'spa_sqrt': '',
                        'spa_linear': '',
                        'spa_knee': '',
                        'paf': '',
                        'number_of_scores_for_this_member_submitted_by_all_members': 0,
                        'number_of_scores_for_this_member_submitted_by_other_members': 0
                    }
                    def _extract_data(data, column, default=''):
                        if column.subfield is not None:
                            if type(data) is not list:
                                try:
                                    data = json.loads(data)
                                except:
                                    return default
                            try:
                                return data[column.subfield]
                            except:
                                return default
                        else:
                            return data
                    # find students in the same group
                    grouping_column_uuid = self.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column']
                    results = list(self.db.data.find({
                        'table': self.table._id,
                        f'{grouping_column_uuid}': student_data.data.get(grouping_column_uuid)
                    }))
                    this_member_sid = student_data.config['sid']
                    this_member_username = student_data.config['username']
                    all_members_sids = [ r['sid'] for r in results ]
                    all_members_usernames = [ r['username'] for r in results ]
                    other_members_sids = [ r['sid'] for r in results if r['sid'] != student_data.config['sid'] ]
                    other_members_usernames = [ r['username'] for r in results if r['username'] != student_data.config['username'] ]
                    sid_to_username = {}
                    username_to_sid = {}
                    for r in results:
                        if r['sid'] not in sid_to_username.keys():
                            sid_to_username[r['sid']] = r['username']
                        if r['username'] not in username_to_sid.keys():
                            username_to_sid[r['username']] = r['sid']
                    # get change history of the score column for students in the same group
                    score_column_reference = self.config['aggregation_options']['aggregator_type_self_peer_review_score_column']
                    if score_column_reference in preloaded_columns.keys():
                        score_column = preloaded_columns[score_column_reference]
                    else:
                        score_column = Column()
                        if not score_column.load(score_column_reference):
                            # uh oh
                            pass # TODO
                    ch = get_change_history( # already sorted by timestamp, most recent first
                        column_uuids=[ score_column.config['uuid'] ],
                        sids=all_members_sids
                    )
                    # calculate how many group members this member has saved data for
                    all_targets_saved_for = []
                    for _ch in ch:
                        if _ch['auth_user'] == this_member_username and _ch['identifier'] not in all_targets_saved_for:
                            all_targets_saved_for.append( _ch['identifier'] )
                    other_targets_saved_for = [ v for v in all_targets_saved_for if v != this_member_sid ]
                    final_value['number_of_scores_submitted_for_all_members'] = len(all_targets_saved_for)
                    final_value['number_of_scores_submitted_for_other_members'] = len(other_targets_saved_for)
                    if len(all_members_sids) > 0:
                        final_value['percentage_of_scores_submitted_for_all_members'] = len(all_targets_saved_for) / len(all_members_sids) * 100
                    if len(other_members_sids) > 0:
                        final_value['percentage_of_scores_submitted_for_other_members'] = len(other_targets_saved_for) / len(other_members_sids) * 100
                    # calculate average score given by this member for all members
                    _data = {}
                    for _ch in ch:
                        if _ch['auth_user'] == this_member_username and _ch['identifier'] in all_members_sids:
                            pair = ( this_member_username , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_this_member_for_all_members'] = utils.list_avg(_data.values())
                    # calculate average score given by this member for other members
                    _data = {}
                    for _ch in ch:
                        if _ch['auth_user'] == this_member_username and _ch['identifier'] in other_members_sids:
                            pair = ( this_member_username , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_this_member_for_other_members'] = utils.list_avg(_data.values())
                    # calculate average score given by all members for this member
                    _data = {}
                    for _ch in ch:
                        if _ch['identifier'] == this_member_sid and _ch['auth_user'] in all_members_usernames:
                            pair = ( _ch['auth_user'] , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_all_members_for_this_member'] = utils.list_avg(_data.values())
                    final_value['total_score_by_all_members_for_this_member'] = utils.list_sum(_data.values())
                    final_value['number_of_scores_for_this_member_submitted_by_all_members'] = len(_data.values())
                    # calculate average score given by other members for this member
                    _data = {}
                    for _ch in ch:
                        if _ch['identifier'] == this_member_sid and _ch['auth_user'] in other_members_usernames:
                            pair = ( _ch['auth_user'] , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_other_members_for_this_member'] = utils.list_avg(_data.values())
                    final_value['number_of_scores_for_this_member_submitted_by_other_members'] = len(_data.values())
                    # calculate average score given by all members for all members
                    _data = {}
                    for _ch in ch:
                        if _ch['identifier'] in all_members_sids and _ch['auth_user'] in all_members_usernames:
                            pair = ( _ch['auth_user'] , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_all_members_for_all_members'] = utils.list_avg(_data.values())
                    # calculate average score given by all members for other members
                    _data = {}
                    for _ch in ch:
                        if _ch['identifier'] in other_members_sids and _ch['auth_user'] in all_members_usernames:
                            pair = ( _ch['auth_user'] , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_all_members_for_other_members'] = utils.list_avg(_data.values())
                    # calculate average score given by other members for other members
                    _data = {}
                    for _ch in ch:
                        if _ch['identifier'] in other_members_sids and _ch['auth_user'] in other_members_usernames:
                            pair = ( _ch['auth_user'] , _ch['identifier'] ) # scorer, scoree
                            if pair not in _data.keys():
                                _data[pair] = _extract_data(_ch['new_value'], score_column)
                    final_value['average_score_by_other_members_for_other_members'] = utils.list_avg(_data.values())
                    # get the score given by this member for this member
                    for _ch in ch:
                        if _ch['identifier'] == this_member_sid and _ch['auth_user'] == this_member_username:
                            try:
                                final_value['score_by_this_member_for_this_member'] = float(_extract_data(_ch['new_value'], score_column))
                                break
                            except:
                                final_value['score_by_this_member_for_this_member'] = ''
                                pass
                    try:
                        final_value['sapa'] = math.sqrt(final_value['score_by_this_member_for_this_member'] / final_value['average_score_by_other_members_for_this_member'])
                        #final_value['spa_linear'] = final_value['average_score_by_all_members_for_this_member'] / final_value['average_score_by_all_members_for_all_members']
                        #final_value['spa_sqrt'] = math.sqrt(final_value['spa_linear'])
                        #if final_value['spa_linear'] <= 1:
                        #    final_value['spa_knee'] = final_value['spa_linear']
                        #elif final_value['spa_linear'] > 1:
                        #    final_value['spa_knee'] = final_value['spa_sqrt']
                    except:
                        final_value['sapa'] = ''
                        logging.error(f"Error calculating SAPA for {this_member_sid} {self.config['uuid']}")
                    # calculate PAF, SAPA, and SPA factors
                    try:
                        # fetch the matrix of raw scores
                        paf_matrix = {}
                        recorded_pairs = [] #(scorer_sid, scoree_sid)
                        recorded_scorers = [] # sids
                        for scorer_sid in all_members_sids:
                            for scoree_sid in all_members_sids:
                                paf_matrix[ (scorer_sid, scoree_sid) ] = 0 # (scorer, scoree)
                        for _ch in ch:
                            scorer_sid = username_to_sid[_ch['auth_user']]
                            pair = ( scorer_sid, _ch['identifier'] ) # scorer, scoree
                            if pair not in recorded_pairs:
                                _score = _extract_data(_ch['new_value'], score_column)
                                if utils.is_number(_score):
                                    paf_matrix[pair] = float(_score)
                                recorded_pairs.append(pair)
                            if scorer_sid not in recorded_scorers:
                                recorded_scorers.append(scorer_sid)
                            if len(recorded_pairs) == len(all_members_sids) ** 2:
                                # all done
                                break
                        # calculate the total scores given by each scorer
                        total_scores_allocated_by_students = {}
                        for scorer_sid in all_members_sids:
                            score_allocated_by_student = 0
                            for scoree_sid in all_members_sids:
                                score_allocated_by_student += paf_matrix[ (scorer_sid, scoree_sid) ]
                            total_scores_allocated_by_students[scorer_sid] = score_allocated_by_student
                        # calculate the total scores received by each scoree
                        total_scores_received_by_students = {}
                        for scoree_sid in all_members_sids:
                            score_received_by_student = 0
                            for scorer_sid in all_members_sids:
                                score_received_by_student += paf_matrix[ (scorer_sid, scoree_sid) ]
                            total_scores_received_by_students[scoree_sid] = score_received_by_student
                        average_total_score_received_by_all_students = utils.list_avg(total_scores_received_by_students.values())
                        # calculate SPA
                        final_value['spa_linear'] = final_value['total_score_by_all_members_for_this_member'] / average_total_score_received_by_all_students
                        final_value['spa_sqrt'] = math.sqrt(final_value['spa_linear'])
                        if final_value['spa_linear'] <= 1:
                            final_value['spa_knee'] = final_value['spa_linear']
                        elif final_value['spa_linear'] > 1:
                            final_value['spa_knee'] = final_value['spa_sqrt']
                        # normalise the given scores by the total that each scorer gave
                        normalised_scores_matrix = {}
                        for scorer_sid in all_members_sids:
                            for scoree_sid in all_members_sids:
                                normalised_scores_matrix[ (scorer_sid, scoree_sid) ] = paf_matrix[ (scorer_sid, scoree_sid) ] / total_scores_allocated_by_students[scorer_sid]
                        # calculate multiplication factor
                        multiplication_factor = len(all_members_sids) / len(recorded_scorers)
                        # calculate paf
                        pa_scores = {}
                        for scoree_sid in all_members_sids:
                            student_pa_score = 0
                            for scorer_sid in all_members_sids:
                                student_pa_score += normalised_scores_matrix[ (scorer_sid, scoree_sid) ]
                            pa_scores[scoree_sid] = student_pa_score * multiplication_factor
                        if len(all_members_sids) * 0.95 <= utils.list_sum(pa_scores.values()) <= len(all_members_sids) * 1.05:
                            # calculation check OK
                            final_value['paf'] = pa_scores[this_member_sid]
                        else:
                            logging.error(f"Error calculating pa_scores for {this_member_sid} {self.config['uuid']}: {pa_scores}")
                    except Exception as err:
                        logging.error(f"Error calculating PAF or SPA for {this_member_sid} {self.config['uuid']}")
                        logging.error(err)
                    # put it all together into an array
                    final_value_array = [
                        final_value['number_of_scores_submitted_for_all_members'],
                        final_value['number_of_scores_submitted_for_other_members'],
                        utils.round_number(final_value['percentage_of_scores_submitted_for_all_members'], 1),
                        utils.round_number(final_value['percentage_of_scores_submitted_for_other_members'], 1),
                        utils.round_number(final_value['average_score_by_this_member_for_all_members'], 4),
                        utils.round_number(final_value['average_score_by_this_member_for_other_members'], 4),
                        utils.round_number(final_value['average_score_by_all_members_for_this_member'], 4),
                        utils.round_number(final_value['average_score_by_other_members_for_this_member'], 4),
                        utils.round_number(final_value['average_score_by_all_members_for_all_members'], 4),
                        utils.round_number(final_value['average_score_by_all_members_for_other_members'], 4),
                        utils.round_number(final_value['average_score_by_other_members_for_other_members'], 4),
                        final_value.get('score_by_this_member_for_this_member', ''),
                        utils.round_number(final_value.get('sapa', ''), 4),
                        utils.round_number(final_value['spa_sqrt'], 4),
                        #utils.round_number(final_value['spa_linear'], 4),
                        utils.round_number(final_value['spa_knee'], 4),
                        utils.round_number(final_value['paf'], 4),
                        final_value['number_of_scores_for_this_member_submitted_by_all_members'],
                        final_value['number_of_scores_for_this_member_submitted_by_other_members'],
                    ]
                    final_value = json.dumps(final_value_array, default=str)
                
                ###################################################################
                # final_value has been determined; perform any post-processing now
                ###################################################################
                
                # post-aggregation arithmetic, if needed
                if self.config['aggregation_options']['post_aggregation_arithmetic_operator'] in ['+', '-', '*', '/'] and utils.is_number(self.config['aggregation_options']['post_aggregation_arithmetic_value']) and utils.is_number(final_value):
                    _paao = self.config['aggregation_options']['post_aggregation_arithmetic_operator']
                    _paav = float(self.config['aggregation_options']['post_aggregation_arithmetic_value'])
                    final_value = float(final_value)
                    if _paao == '+':
                        final_value = final_value + _paav
                    elif _paao == '-':
                        final_value = final_value - _paav
                    elif _paao == '*':
                        final_value = final_value * _paav
                    elif _paao == '/' and _paav != 0:
                        final_value = final_value / _paav
                # rounding if needed
                if self.config['aggregation_options']['rounding'] != '' and utils.is_number(final_value):
                    try:
                        getcontext().rounding = ROUND_HALF_UP
                        _dp = Decimal(10) ** -int(self.config['aggregation_options']['rounding'])
                        _final_value = final_value
                        if self.config['aggregation_options']['rounding_direction'] == 'nearest':
                            final_value = Decimal(final_value).quantize(_dp)
                        else:
                            final_value = Decimal(final_value).quantize(
                                _dp,
                                rounding=ROUND_CEILING if self.config['aggregation_options']['rounding_direction'] == 'ceiling' else ROUND_FLOOR
                            )
                        if final_value.is_nan() or final_value.is_infinite():
                            final_value = ''
                        else:
                            final_value = str(final_value)
                    except:
                        final_value = _final_value
                # substitutions if needed
                if self.config['aggregation_options']['regex_replace_pattern'] != '':
                    if self.config['aggregation_options']['regex_replace_mode'] == 'text':
                        final_value = final_value.replace(
                            self.config['aggregation_options']['regex_replace_pattern'],
                            self.config['aggregation_options']['regex_replace_replacement']
                        )
                    elif self.config['aggregation_options']['regex_replace_mode'] == 'regex':
                        final_value = re.sub(
                            self.config['aggregation_options']['regex_replace_pattern'],
                            self.config['aggregation_options']['regex_replace_replacement'],
                            final_value
                        )
                # check for inf and nan
                if utils.is_number(final_value):
                    try:
                        if final_value == math.inf or final_value == math.nan or math.isnan(final_value):
                            final_value = ''
                    except:
                        # just yield the final_value...
                        pass
                # save
                ret[identifier]['aggregated_value'] = final_value
                ret[identifier]['success'] = True
                t0 = datetime.now()
                student_data.set_data(
                    column_uuid=self.config['uuid'],
                    data=final_value,
                    auth_user_override=auth_user_override,
                    columns_already_traversed=columns_already_traversed[:],
                    ignore_active=True,
                    commit_immediately=True,
                    preloaded_column=self,
                    skip_auth_checks=True,
                    threaded_aggregation=threaded_aggregation,
                    preloaded_columns=preloaded_columns
                )
                
                # modify this column's settings if needed
                if self.config['aggregation_options']['method'] == 'self_peer_review':
                    self.config['multientry_data_format'] = True
                    self.config['multi_entry']['options'] = [
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Number of scores submitted by this member for all group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Number of scores submitted by this member for other group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Percentage of scores submitted by this member for all group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Percentage of scores submitted by this member for other group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by this member for all group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by this member for other group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by all group members for this member'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by other group members for this member'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by all group members for all group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by all group members for other group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Average score by other group members for other group members'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Score by this group member for themselves'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'SAPA'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'SPA (square root)'
                        },
                        #{
                        #    'type': 'regex',
                        #    'regex': '.*',
                        #    'label': 'SPA (linear)'
                        #},
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'SPA (knee)'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'PAF'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Number of scores submitted by all group members for this member'
                        },
                        {
                            'type': 'regex',
                            'regex': '.*',
                            'label': 'Number of scores submitted by other group members for this member'
                        },
                    ]
                    self.update()
                
                #print('time for save data', (datetime.now() - t0).total_seconds())
            else:
                ret[identifier]['errors'].append(("Could not find student {}".format(identifier), "warning"))
        return ret
    
