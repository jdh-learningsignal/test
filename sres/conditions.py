from flask import g
import datetime
from dateutil import parser
import collections
import json
import re
import cexprtk
import logging

from sres import utils
from sres.db import _get_db
from sres.columns import Column, table_uuids_from_column_references
from sres.studentdata import StudentData, substitute_text_variables, NAME_FIELDS, IDENTIFIER_FIELDS, NON_DATA_FIELDS
from sres import cexprtk_ext
from sres.tables import Table
from sres.auth import is_user_administrator

ALL_COMPARATORS = {
    "equal": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "not_equal": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "in": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "not_in": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "less": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "less_or_equal": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "greater": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "greater_or_equal": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "between": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "not_between": {
        'numeric': True,
        'datetime': True,
        'string': True
    },
    "begins_with": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "not_begins_with": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "contains": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "not_contains": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "ends_with": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "not_ends_with": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "is_empty": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "is_not_empty": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "matches_regex": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "not_matches_regex": {
        'numeric': False,
        'datetime': False,
        'string': True
    },
    "is_null": { # is null should be cast to is empty
        'numeric': False,
        'datetime': False,
        'string': False
    },
    "is_not_null": { # is not null should be cast to is not empty
        'numeric': False,
        'datetime': False,
        'string': False
    }
}

NUMERIC_COMPARATORS = [k for k, v in ALL_COMPARATORS.items() if v['numeric']]
DATETIME_COMPARATORS = [k for k, v in ALL_COMPARATORS.items() if v['datetime']]
STRING_COMPARATORS = [k for k, v in ALL_COMPARATORS.items() if v['string']]

def _build_query_from_conditions(conditions):
    """
        Builds a mongo find query/filter for db.data based on conditions (dict) per queryBuilder config.
    """
    if not isinstance(conditions, collections.Mapping):
        return None
    filters = []
    combiner = '$and' if conditions['condition'] == 'AND' else '$or'
    for rule in conditions['rules']:
        if 'rules' in rule.keys():
            # the current 'rule' is actually a group, so, need to recurse
            filters.append(build_query_from_conditions(rule))
        else:
            pass
    return {
        combiner: filters
    }

class AdvancedConditions:
    
    def __init__(self, conditions):
        """
            conditions (str) cexprtk expression
        """
        self.db = _get_db()
        self.conditions = conditions
    
    def run_conditions(self):
        """
            Runs self.conditions against db.data.
            
            Returns a dict:
                {
                    all_column_references (list of strings) column references
                    all_column_uuids (list of string uuids) column uuids of the column references
                    data (dict of dicts) keyed by sid. Every dict in the dict of dicts corresponds to a
                        student who matches the conditions, and contains the relevant fields 
                        (keyed by column_uuid strings) requested.
                }
        """
        
        # first, pre-process all the cexprtk extension functions
        extension_column_helpers = {}
        expr = self.conditions
        parse_results = cexprtk_ext.parse_ext_functions(expr)
        for column_reference in parse_results['column_references']:
            extension_column_helpers[column_reference] = Column()
            extension_column_helpers[column_reference].load(column_reference)
        print('xxx', extension_column_helpers)
        
        # grab relevant columns
        all_column_references = re.findall(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, self.conditions)
        clean_column_references = utils.clean_delimiter_from_column_references(all_column_references)
        all_column_uuids = [r.split('.')[0] for r in clean_column_references]
        all_table_uuids = table_uuids_from_column_references(all_column_references)
        # extract relevant data from db into memory
        #exists = []
        #for column_uuid in all_column_uuids:
        #    exists.append({'{}'.format(column_uuid): {'$exists': True}})
        fields = all_column_uuids + ['table', 'table_uuid', 'status'] + NAME_FIELDS + IDENTIFIER_FIELDS
        results = self.db.data.find(
            {
                '$and': [
                    {'status': 'active'},
                    {'table_uuid': {'$in': all_table_uuids}}
                ]
            }, 
            fields
        )
        results = list(results)
        # populate a dict with all fields from the results dict
        student_data = {}
        for result in results:
            idx = result['sid']
            if not idx in student_data.keys():
                student_data[idx] = {
                    k: '' for k in NON_DATA_FIELDS
                }
            for id_key in NON_DATA_FIELDS:
                if not student_data[idx][id_key] and id_key in result.keys() and result[id_key]:
                    student_data[idx][id_key] = result[id_key]
            for column_uuid in all_column_uuids:
                if column_uuid in result.keys():
                    if result[column_uuid] is None:
                        student_data[idx][column_uuid] = ''
                    elif utils.is_json(result[column_uuid]) and not utils.is_number(result[column_uuid]):
                        student_data[idx][column_uuid] = json.loads(result[column_uuid])
                    else:
                        student_data[idx][column_uuid] = result[column_uuid]
                else:
                    student_data[idx][column_uuid] = ''
        # get metadata for each column reference
        column_references_info = {}
        for raw_column_reference in all_column_references:
            if raw_column_reference not in column_references_info.keys():
                column_references_info[raw_column_reference] = Column()
                if column_references_info[raw_column_reference].load(utils.clean_delimiter_from_column_references(raw_column_reference)):
                    pass
                else:
                    del column_references_info[raw_column_reference]
        # evaluate
        student_outcomes = {}
        for sid, data in student_data.items():
            expr = self.conditions
            # if any extension functions are included, call them first
            if extension_column_helpers:
                print('expr before', expr)
                expr = cexprtk_ext.substitute_ext_function_result(
                    expr=expr,
                    identifier=sid,
                    #preloaded_student_data=student_data,
                    preloaded_columns=extension_column_helpers
                )
                print('expr after', expr)
            # substitute values
            for raw_column_reference, column in column_references_info.items():
                # get data
                if column.subfield is not None:
                    try:
                        data_temp = data[column.config['uuid']][column.subfield]
                    except:
                        data_temp = ''
                else:
                    data_temp = data[column.config['uuid']]
                # substitute
                expr = expr.replace(raw_column_reference, data_temp)
            # evaluate
            try:
                student_outcomes[sid] = cexprtk.evaluate_expression(expr, {})
            except:
                student_outcomes[sid] = False
        # parse for return
        for k, v in student_outcomes.items():
            if v == False:
                student_data.pop(k)
        return {
            'data': student_data,
            'all_column_references': clean_column_references,
            'all_column_uuids': all_column_uuids
        }
        
        

class OpenConditions:
    
    """Class for filters' primary conditions"""
    
    def __init__(self, conditions):
        """
            conditions (dict) QueryBulder rules object
        """
        self.db = _get_db()
        self.conditions = conditions
    
    def run_conditions(self, check_table_permissions=False, user_oid=None):
        """
            Runs self.conditions against db.data.
            
            check_table_permissions (boolean) Whether to perform an additional check of table permissions.
            user_oid (ObjectId) ObjectId of the user to check permissions for. Must be supplied
                if check_table_permissions is True.
            
            Returns a dict:
                {
                    all_column_references (list of strings) column references
                    all_column_uuids (list of string uuids) column uuids of the column references
                    data (dict of dicts) keyed by sid. Every dict in the dict of dicts corresponds to a
                        student who matches the conditions, and contains the relevant fields 
                        (keyed by column_uuid strings) requested.
                }
        """
        ret = {
            'data': {},
            'all_column_references': [],
            'all_column_uuids': []
        }
        # grab relevant columns
        conditions_helper = Conditions(identifier=None, conditions=self.conditions, student_data=None)
        all_column_references = conditions_helper.extract_all_column_references()
        all_column_uuids = [r.split('.')[0] for r in all_column_references]
        all_table_uuids = table_uuids_from_column_references(all_column_references)
        # check table conditions if needed
        if check_table_permissions and user_oid is not None:
            permissions_check = self.db.tables.find(
                {
                    'uuid': {'$in': all_table_uuids},
                    '$or': [
                        {'staff.administrators': user_oid},
                        {'staff.auditors': user_oid}
                    ]
                }
            )
            if len(list(permissions_check)) != len(all_table_uuids):
                if not is_user_administrator('super', user_oid=user_oid):
                    # no good! Not authorised for all the needed tables.
                    logging.warning(f'Not authorised for all needed tables to run conditions [{all_table_uuids}]')
                    return ret
        # extract relevant data from db into memory
        #exists = []
        #for column_uuid in all_column_uuids:
        #    exists.append({'{}'.format(column_uuid): {'$exists': True}})
        fields = all_column_uuids + ['table', 'table_uuid', 'status'] + NAME_FIELDS + IDENTIFIER_FIELDS
        results = self.db.data.find(
            {
                '$and': [
                    {'status': 'active'},
                    {'table_uuid': {'$in': all_table_uuids}}
                ]
            }, 
            fields
        )
        results = list(results)
        # populate a dict with all fields from the results dict
        student_data = {}
        for result in results:
            idx = result['sid']
            if not idx in student_data.keys():
                student_data[idx] = {
                    k: '' for k in NON_DATA_FIELDS
                }
            for id_key in NON_DATA_FIELDS:
                if not student_data[idx][id_key] and id_key in result.keys() and result[id_key]:
                    student_data[idx][id_key] = result[id_key]
            for column_uuid in all_column_uuids:
                if column_uuid in result.keys():
                    if result[column_uuid] is None:
                        student_data[idx][column_uuid] = ''
                    elif utils.is_json(result[column_uuid]) and not utils.is_number(result[column_uuid]):
                        student_data[idx][column_uuid] = json.loads(result[column_uuid])
                    else:
                        student_data[idx][column_uuid] = result[column_uuid]
                else:
                    student_data[idx][column_uuid] = ''
        # cache up all Column instances
        preloaded_columns = {}
        for column_reference in all_column_references:
            preloaded_columns[column_reference] = Column()
            preloaded_columns[column_reference].load(column_reference)
        # use Conditions.evaluate_conditions to evaluate whether the conditions apply to each student
        student_outcomes = {}
        default_table = Table()
        default_table.load(all_table_uuids[0])
        for sid, data in student_data.items():
            #logging.debug('evaluating conditions ' + str(sid))
            conditions_helper = Conditions(
                identifier=sid, 
                conditions=self.conditions, 
                student_data=data, 
                default_table=default_table,
                preloaded_columns=preloaded_columns
            )
            student_outcomes[sid] = conditions_helper.evaluate_conditions()
        # parse for return
        for k, v in student_outcomes.items():
            if v == False:
                student_data.pop(k)
        # return
        ret['data'] = student_data
        ret['all_column_references'] = all_column_references
        ret['all_column_uuids'] = all_column_uuids
        return ret
        
class Conditions:
    
    """Generic class for processing QueryBuilder conditions"""
    
    def __init__(self, identifier, conditions, student_data, default_table=None, preloaded_columns=None):
        """
            identifier (string) unique student identifier
            conditions (dict) QueryBulder rules object
            student_data (StudentData or dict) loaded class instance of StudentData,
                or loaded dict of dicts, keyed by sid and then field names
            default_table (Table instance)
            preloaded_columns (dict of Column instances, keyed by column_uuid)
        """
        self.identifier = identifier
        self.conditions = conditions
        self.student_data = student_data
        self.default_table = default_table
        self.preloaded_columns = preloaded_columns
        if isinstance(student_data, StudentData):
            self.student_data_type = 'class'
        elif isinstance(student_data, collections.Mapping):
            self.student_data_type = 'dict'
        else:
            self.student_data_type = None
    
    def evaluate_conditions(self, blank_replacement=''):
        """
            Evaluates self.conditions.
            
            Returns simply a True or False if rules and combiner ('condition' in queryBuilder parlance) hold true.
        """
        combiner = self.conditions['condition']
        rules = self.conditions['rules']
        _not = 'not' in self.conditions.keys() and self.conditions['not']
        rule_outcomes = [False for r in rules]
        r = 0
        for rule in rules:
            if 'rules' in rule.keys():
                # the current 'rule' is actually a group, so, need to recurse
                conditions = Conditions(
                    identifier=self.identifier,
                    conditions=rule,
                    student_data=self.student_data,
                    preloaded_columns=self.preloaded_columns
                )
                rule_outcomes[r] = conditions.evaluate_conditions(blank_replacement=blank_replacement)
            else:
                column_reference = rule['id']
                expr_right = rule['value'] # the value, or 'operator', or right hand side of the expression
                if expr_right and '$' in str(expr_right):
                    # potential column reference
                    if re.match(utils.DELIMITED_COLUMN_REFERENCE_PATTERN, expr_right):
                        _substituted_expr_right = substitute_text_variables(
                            expr_right,
                            self.identifier,
                            self.student_data.table.config['uuid'],
                            preloaded_student_data=self.student_data,
                            preloaded_columns=self.preloaded_columns
                        )['new_text']
                        expr_right = _substituted_expr_right
                comparator = rule['operator']
                # get data (expr_left)
                column = None
                if self.preloaded_columns is not None:
                    # use Column instance cache please
                    if column_reference in self.preloaded_columns.keys():
                        column = self.preloaded_columns[column_reference]
                    else:
                        # need to load...
                        pass
                if column is None:
                    column = Column(preloaded_table=self.default_table)
                    if not column.load(column_reference):
                        rule_outcomes[r] = False
                        continue
                if self.student_data_type == 'class':
                    data_temp = self.student_data.get_data(
                        column_uuid=column.config['uuid'], 
                        preloaded_column=column, 
                        default_value=blank_replacement # default to blank
                    )
                elif self.student_data_type == 'dict':
                    data_temp = {
                        'data': blank_replacement
                    }
                    if column.config['uuid'] in self.student_data.keys():
                        data_temp['data'] = self.student_data[column.config['uuid']]
                    else:
                        data_temp['data'] = blank_replacement # default to blank
                    data_temp['success'] = True
                else:
                    # something misconfigured
                    data_temp = {
                        'success': False,
                        'data': blank_replacement
                    }
                #logging.debug(str(self.identifier) + ' ' + str(data_temp))
                if not data_temp['success']:
                    rule_outcomes[r] = False
                    continue
                # determine expr_left
                _expr_left_do_not_stringify = False
                if column.subfield is not None:
                    try:
                        if isinstance(data_temp['data'], list) and column.subfield < len(data_temp['data']):
                            expr_left = data_temp['data'][column.subfield]
                            _expr_left_do_not_stringify = True
                        else:
                            expr_left = blank_replacement
                    except:
                        rule_outcomes[r] = False
                        continue
                else:
                    expr_left = data_temp['data']
                # convert if necessary for blank_replacement
                if blank_replacement != '' and (expr_left == '' or expr_left is None):
                    expr_left = blank_replacement
                # determine how to perform the comparison
                mode = 'string'
                if (comparator == 'between' or comparator == 'not_between') and isinstance(expr_right, list):
                    if utils.is_number(expr_right[0]) and utils.is_number(expr_right[1]) and utils.is_number(expr_left) and comparator in NUMERIC_COMPARATORS:
                        expr_left = float(expr_left)
                        expr_right = [float(expr_right[0]), float(expr_right[1])]
                        mode = 'numeric'
                    elif utils.is_datetime(expr_right[0]) and utils.is_datetime(expr_right[1]) and utils.is_datetime(expr_left) and comparator in DATETIME_COMPARATORS:
                        expr_left = parser.parse(expr_left)
                        expr_right = [parser.parse(expr_right[0]), parser.parse(expr_right[1])]
                        mode = 'datetime'
                elif (comparator in ['contains', 'not_contains', 'in', 'not_in', 'begins_with', 'not_begins_with', 'ends_with', 'not_ends_with', 'matches_regex', 'not_matches_regex']):
                    if type(expr_left) is list:
                        expr_left = json.dumps(expr_left, default=str)
                    else:
                        expr_left = str(expr_left)
                    expr_right = str(expr_right)
                else:
                    if utils.is_number(expr_right) and utils.is_number(expr_left) and comparator in NUMERIC_COMPARATORS:
                        expr_left = float(expr_left)
                        expr_right = float(expr_right)
                        mode = 'numeric'
                    elif utils.is_datetime(expr_right) and utils.is_datetime(expr_left) and comparator in DATETIME_COMPARATORS:
                        expr_left = parser.parse(expr_left)
                        expr_right = parser.parse(expr_right)
                        mode = 'datetime'
                    else:
                        # operate as strings...
                        if comparator == 'is_null': comparator = 'is_empty'
                        if comparator == 'is_not_null': comparator = 'is_not_empty'
                        if not _expr_left_do_not_stringify:
                            if type(expr_left) is list:
                                expr_left = json.dumps(expr_left, default=str)
                            else:
                                expr_left = str(expr_left)
                        expr_right = str(expr_right)
                # perform the comparison
                if comparator == 'equal':
                    if mode == 'string':
                        rule_outcomes[r] = expr_left.lower() == expr_right.lower()
                    else:
                        rule_outcomes[r] = expr_left == expr_right
                elif comparator == 'not_equal':
                    if mode == 'string':
                        rule_outcomes[r] = expr_left.lower() != expr_right.lower()
                    else:
                        rule_outcomes[r] = expr_left != expr_right
                elif comparator == 'in':
                    rule_outcomes[r] = expr_left.lower() in expr_right.lower().split(',')
                elif comparator == 'not_in':
                    rule_outcomes[r] = expr_left.lower() not in expr_right.lower().split(',')
                elif comparator == 'less':
                    rule_outcomes[r] = expr_left < expr_right
                elif comparator == 'less_or_equal':
                    rule_outcomes[r] = expr_left <= expr_right
                elif comparator == 'greater':
                    rule_outcomes[r] = expr_left > expr_right
                elif comparator == 'greater_or_equal':
                    rule_outcomes[r] = expr_left >= expr_right
                elif comparator == 'between' or comparator == 'not_between':
                    if isinstance(expr_right, list):
                        # OK
                        if comparator == 'between':
                            rule_outcomes[r] = expr_right[0] < expr_left < expr_right[1]
                        elif comparator == 'not_between':
                            rule_outcomes[r] = not (expr_right[0] < expr_left < expr_right[1])
                    else:
                        # misconfiguration
                        rule_outcomes[r] = False
                elif comparator == 'begins_with':
                    rule_outcomes[r] = expr_left.lower().startswith(expr_right.lower())
                elif comparator == 'not_begins_with':
                    rule_outcomes[r] = not expr_left.lower().startswith(expr_right.lower())
                elif comparator == 'contains':
                    rule_outcomes[r] = expr_right.lower() in expr_left.lower()
                elif comparator == 'not_contains':
                    rule_outcomes[r] = not (expr_right.lower() in expr_left.lower())
                elif comparator == 'ends_with':
                    rule_outcomes[r] = expr_left.lower().endswith(expr_right.lower())
                elif comparator == 'not_ends_with':
                    rule_outcomes[r] = not(expr_left.lower().endswith(expr_right.lower()))
                elif comparator == 'is_empty':
                    try:
                        rule_outcomes[r] = len(expr_left) == 0
                    except:
                        rule_outcomes[r] = expr_left == ''
                elif comparator == 'is_not_empty':
                    try:
                        rule_outcomes[r] = len(expr_left) != 0
                    except:
                        rule_outcomes[r] = expr_left != ''
                elif comparator == 'matches_regex':
                    rule_outcomes[r] = True if re.search(expr_right, expr_left) is not None else False
                elif comparator == 'not_matches_regex':
                    rule_outcomes[r] = False if re.search(expr_right, expr_left) is not None else True
                else:
                    # problem, unrecognised comparator/operator!
                    rule_outcomes[r] = False
            r += 1
        # finished looping through rules. decide on interim final outcome
        interim_outcome = None
        if combiner == 'AND':
            if False in rule_outcomes:
                interim_outcome = False
            else:
                interim_outcome = True
        if combiner == 'OR':
            if True in rule_outcomes:
                interim_outcome = True
            else:
                interim_outcome = False
        if interim_outcome is not None:
            if _not:
                return not interim_outcome
            else:
                return interim_outcome
        else:
            # something went wrong...
            return False
    
    def extract_all_column_references(self, remove_duplicates=True):
        """
            Recurses through the rules and picks up all the ids (i.e. column references)
        """
        column_references = []
        if not self.conditions:
            return column_references
        for rule in self.conditions['rules']:
            if 'rules' in rule.keys():
                # the current 'rule' is actually a group, so, need to recurse
                conditions = Conditions(
                    identifier=self.identifier,
                    conditions=rule,
                    student_data=self.student_data
                )
                column_references.extend(conditions.extract_all_column_references())
            else:
                column_references.append(rule['id'])
        if remove_duplicates:
            column_references = list(dict.fromkeys(column_references))
        return column_references
