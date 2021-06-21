from datetime import datetime
from dateutil import parser
import re

from sres import utils
from sres.studentdata import substitute_text_variables

PATTERN_DATEDIFF_NONCAPTURING = r"(?<=_DATEDIFF\()(?:\s*)(\$COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*\$)(?:\s*,\s*)(\$COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*\$)(?:\s*,\s*)([a-z]+)(?:\s*)(?=\))"
PATTERN_DATEDIFF_CAPTURE_ALL = r"_DATEDIFF\(\s*\$COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*\$\s*,\s*\$COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*\$\s*,\s*[a-z]+\s*\)"

def parse_ext_functions(expr):
    """
        Parses the cexprtk expressions and looks for extension function calls.
        Returns a dict...
    """
    ret = {
        'column_references': []
    }
    if '_DATEDIFF' in expr:
        matches = re.findall(PATTERN_DATEDIFF_NONCAPTURING, expr)
        if len(matches):
            for match in matches:
                if len(match) == 3:
                    ref1 = utils.clean_delimiter_from_column_references(match[0])
                    ref2 = utils.clean_delimiter_from_column_references(match[1])
                    interval = match[2]
                    ret['column_references'].append(ref1)
                    ret['column_references'].append(ref2)
    # elif '_OTHERFUNCTIONSIGNATURE':
    # remove duplicates
    ret['column_references'] = list(dict.fromkeys(ret['column_references']))
    # return
    return ret

def substitute_ext_function_result(expr, identifier, preloaded_columns, preloaded_student_data=None):
    """
        Looks for calls to extension functions in cexprtk expressions, calculates them out, 
        and substitutes in the (numerical) result.
        
        expr (str)
        identifier (str)
        preloaded_columns (dict of Columns keyed by column reference)
        preloaded_student_data (StudentData)
        
        Returns string of expr with appropriate substitutions
    """
    if '_DATEDIFF' in expr:
        full_matches = re.findall(PATTERN_DATEDIFF_CAPTURE_ALL, expr)
        if len(full_matches):
            for full_match in full_matches:
                matches = re.findall(PATTERN_DATEDIFF_NONCAPTURING, full_match)
                if len(matches):
                    for match in matches:
                        if len(match) == 3:
                            # get ref 1
                            raw_ref1 = match[0]
                            clean_ref1 = utils.clean_delimiter_from_column_references(raw_ref1)
                            data_ref1 = substitute_text_variables(
                                input=raw_ref1,
                                identifier=identifier,
                                default_table_uuid=preloaded_columns[clean_ref1].table.config['uuid'],
                                preloaded_student_data=preloaded_student_data
                            )['new_text']
                            # get ref 2
                            raw_ref2 = match[1]
                            clean_ref2 = utils.clean_delimiter_from_column_references(raw_ref2)
                            data_ref2 = substitute_text_variables(
                                input=raw_ref2,
                                identifier=identifier,
                                default_table_uuid=preloaded_columns[clean_ref2].table.config['uuid'],
                                preloaded_student_data=preloaded_student_data
                            )['new_text']
                            # get interval
                            interval = match[2]
                            # calculate
                            calculation_result = _datediff(data_ref1, data_ref2, interval)
                            # replace
                            expr = expr.replace(full_match, str(calculation_result))
    return expr

def _datediff(date1, date2, interval):
    if not isinstance(date1, str) or not isinstance(date2, str):
        return ''
    if not date1 or not date2:
        return ''
    d1 = parser.parse(date1)
    d2 = parser.parse(date2)
    diff = d1 - d2
    if interval == 'seconds':
        return diff.total_seconds()
    elif interval == 'minutes':
        return diff.total_seconds() / 60
    elif interval == 'hours':
        return diff.total_seconds() / (60 * 60)
    elif interval == 'days':
        return diff.total_seconds() / (60 * 60 * 24)
    elif interval == 'weeks':
        return diff.total_seconds() / (60 * 60 * 24 * 7)
    else:
        return diff.total_seconds()
    
    