from flask import g, session, url_for, escape, current_app
from copy import deepcopy
import re
import collections
from datetime import datetime, timedelta
from dateutil import parser
from natsort import natsorted, ns
from scipy import stats
import pandas
import numpy
from flask_mail import Message
import logging
import json
import os
#import concurrent.futures

from sres import utils
from sres.db import _get_db
from sres.config import _get_config
from sres.auth import get_auth_user_oid, is_user_administrator, get_auth_user
from sres.users import User, oids_to_usernames, find_email_for_user
from sres.columns import table_uuids_from_column_references, Column
from sres.conditions import Conditions
from sres.studentdata import substitute_text_variables, StudentData
from sres.jobs import APSJob

_FORM_OPTIONS = {
    'trigger_config_quartiles_range': [
        {'value': '1', 'display': "bottom"},
        {'value': '2', 'display': "bottom two"},
        {'value': '3', 'display': "top two"},
        {'value': '4', 'display': "top"}
    ],
    'trigger_config_quartiles_combiner': [
        {'value': 'any', 'display': "any"},
        {'value': 'all', 'display': "all"}
    ],
    'trigger_config_matching_count_comparator': [
        {'value': 'eq', 'display': "exactly"},
        {'value': 'lt', 'display': "fewer than"},
        {'value': 'gt', 'display': "more than"}
    ],
    'trigger_config_matching_method': [
        {'value': 'eq', 'display': "is/are equal to"},
        {'value': 'neq', 'display': "is/are not equal to"},
        {'value': 'like', 'display': "contain(s)"},
        {'value': 'notlike', 'display': "do/does not contain(s)"},
        {'value': 'lt', 'display': "is/are less than the number"},
        {'value': 'lte', 'display': "is/are less than or equal to the number"},
        {'value': 'gt', 'display': "is/are greater than the number"},
        {'value': 'gte', 'display': "is/are greater than or equal to the number"},
        {'value': 'isnull', 'display': "is/are empty"},
        {'value': 'isnotnull', 'display': "is/are not empty"},
        {'value': 'regex', 'display': "fit(s) the regular expression"}
    ],
    'trigger_config_trending_direction': [
        {'value': 'down', 'display': "downwards"},
        {'value': 'up', 'display': "upwards"}
    ],
    'trigger_config_trending_data_conversion': [
        {'value': 'none', 'display': "No conversion, leave as is"},
        {'value': 'blankzero', 'display': "Convert blanks to 0 (zero)"},
        {'value': 'binarise', 'display': "Blanks as 0 (zero), non-blanks as 1 (one)"}
    ],
    'trigger_config_distance_direction': [
        {'value': 'lt', 'display': "lower than"},
        {'value': 'gt', 'display': "higher than"},
        {'value': 'ltgt', 'display': "lower than or higher than"}
    ],
    'trigger_config_distance_combiner': [
        {'value': 'any', 'display': 'any'},
        {'value': 'all', 'display': 'all'}
    ],
    'trigger_config_ml_outliers_data_conversion_non_numerical': [
        {'value': 'binarise', 'display': "Convert blanks to 0 (zero), convert non-blanks to 1 (one)"},
        {'value': 'extractnum', 'display': "Convert blanks to 0 (zero), extract any numbers from non-blanks otherwise treat as 1 (one)"}
    ],
    'trigger_config_columns_ignore_state': [
        {'value': 'disabled', 'display': "(Disabled - all columns will be used)"},
        {'value': 'untilactivefrom', 'display': "they have not yet become active (i.e. the 'active from' date is in the future)"},
        {'value': 'untilactiveto', 'display': "they have not yet finished being active (i.e. the 'active to' date is in the future)"}
    ]
}

def _get_form_option_display_by_value(key, value):
    for option in _FORM_OPTIONS[key]:
        if option['value'] == value:
            return option['display']
    return ''

def list_authorised_insights(auth_user=None, show_deleted=False, only_fields=None):
    """
        Gets all the insights that the specified or current user is authorised to view.
        
        auth_user (string) username
        show_deleted(boolean)
        only_fields (None or list) The db keys to return, as a list of strings.
            If [], requests all fields. If None, returns basic fields only.
        
        Returns a list of dicts, straight from db.filters
    """
    db = _get_db()
    filter = {}
    # administrators
    if not is_user_administrator('super'):
        filter['administrators'] = get_auth_user_oid() if auth_user is None else usernames_to_oids([auth_user], add_if_not_exists=False)[0]
    # archived or not
    if show_deleted:
        #filter['workflow_state'] = {'$in': ['archived', '', 'active']}
        pass
    else:
        filter['workflow_state'] = {'$in': ['', None, 'active']}
    # fields
    if only_fields is None:
        return_fields = ['uuid', 'name', 'description', 'created', 'modified', 'workflow_state', 'alert_startfrom', 'alert_endby']
    else:
        return_fields = only_fields
    # find!
    return list(db.insights.find(filter, return_fields).sort([('created', -1), ('name', 1)]))

def run_insight(insight_uuid, send_email=True, return_html=False):
    """
        Runs the insight and variably sends alerts off and/or returns the HTML content of the insight alert.
    """
    ret = {
        'success': False,
        'alerts': []
    }
    db = _get_db()
    insight = Insight()
    if insight.load(insight_uuid):
        # check if already running
        job = APSJob(insight.get_job_id())
        if not job.claim_job(skip_loading=True):
            logging.debug('insight claimed elsewhere, quitting [{}] pid [{}]'.format(insight_uuid, os.getpid()))
            return ret
        # check workflow_state
        if insight.config['workflow_state'] != 'active':
            insight.schedule(action='remove')
            return ret
        # load
        column_references = insight.config['trigger_config']['trigger_config_select_columns'].split(',')
        teacher_allocation_columnuuid = insight.config['alert_config']['teacher_allocation_columnuuid']
        grouping_columnuuid = insight.config['alert_config']['grouping_columnuuid']
        table_uuids = insight.get_referenced_table_uuids()
        columns_info = {}
        ignored_columns_info = {}
        single_table_only = len(table_uuids) == 1
        # set up other columns
        for column_reference in column_references:
            column = Column()
            if column.load(column_reference):
                include_column = True
                # determine if any date conditions need to be imposed on columns
                if insight.config['trigger_config']['trigger_config_columns_ignore_state'] == 'untilactivefrom':
                    if datetime.now() < column.config['active']['from']:
                        include_column = False
                elif insight.config['trigger_config']['trigger_config_columns_ignore_state'] == 'untilactiveto':
                    if datetime.now() < column.config['active']['to']:
                        include_column = False
                if include_column:
                    columns_info[column_reference] = {
                        'column_uuid': column.config['uuid'],
                        'table_uuid': column.table.config['uuid'],
                        'subfield': column.subfield,
                        'friendly_display_column': column.get_friendly_name(),
                        'friendly_display_table': column.table.get_full_name(),
                        'friendly_display': column.get_friendly_name(show_table_info= not single_table_only)
                    }
                else:
                    table_uuids.remove(column.table.config['uuid'])
                    ignored_columns_info[column_reference] = {
                        'column_uuid': column.config['uuid'],
                        'table_uuid': column.table.config['uuid'],
                        'subfield': column.subfield,
                        'friendly_display_column': column.get_friendly_name(),
                        'friendly_display_table': column.table.get_full_name(),
                        'friendly_display': column.get_friendly_name(show_table_info= not single_table_only)
                    }
        # get relevant students and data
        filter = {
            'status': 'active'
        }
        filter['table_uuid'] = {'$in': table_uuids}
        fields = ['sid', 'email']
        fields.extend([c['column_uuid'] for r, c in columns_info.items()])
        if teacher_allocation_columnuuid:
            fields.append(teacher_allocation_columnuuid.split('.')[0])
        if grouping_columnuuid:
            fields.append(grouping_columnuuid.split('.')[0])
        results = list(db.data.find(filter, fields))
        #logging.debug('fields ' + str(fields))
        #logging.debug('results ' + str(results[:5]))
        # parse data
        data = {}
        teacher_allocation_teachers = []
        grouping_allocation_groups = []
        teachers_to_groups = {}
        students_by_groups = {}
        collected_column_references = [c for c in column_references if c in columns_info.keys()] # the column references that there is actually data for
        column_references = collected_column_references
        #logging.debug('column_references ' + str(column_references))
        #logging.debug('columns_info.keys() ' + str(columns_info.keys()))
        for result in results:
            row = {
                'identifier': result['sid']
            }
            # collect column data
            for column_reference in column_references:
                if column_reference in columns_info.keys():
                    column_uuid = columns_info[column_reference]['column_uuid']
                    if column_uuid in result.keys():
                        if columns_info[column_reference]['subfield'] is None:
                            row[column_reference] = result[column_uuid]
                        else:
                            try:
                                row[column_reference] = json.loads(result[column_uuid])[columns_info[column_reference]['subfield']]
                            except:
                                row[column_reference] = ''
                        if not row[column_reference]:
                            row[column_reference] = None
                    else:
                        row[column_reference] = ''
            # collect teacher allocations
            try:
                if teacher_allocation_columnuuid and teacher_allocation_columnuuid in result.keys():
                    if utils.is_json(result[teacher_allocation_columnuuid]):
                        row['teacher_allocation'] = json.loads(result[teacher_allocation_columnuuid])
                    else:
                        row['teacher_allocation'] = result[teacher_allocation_columnuuid].split(',')
                    # only record allocation if not blank
                    if len(row['teacher_allocation']) == 1 and row['teacher_allocation'][0] == '':
                        pass
                    else:
                        teacher_allocation_teachers.extend(row['teacher_allocation'])
                else:
                    row['teacher_allocation'] = []
            except:
                row['teacher_allocation'] = []
            teacher_allocation_teachers = list(dict.fromkeys(teacher_allocation_teachers)) # deduplicate
            # collect group allocations
            try:
                if grouping_columnuuid and grouping_columnuuid in result.keys() and result[grouping_columnuuid]:
                    # parse group allocations
                    if utils.is_json(result[grouping_columnuuid]) and not utils.is_number(result[grouping_columnuuid]):
                        row['grouping_allocation'] = json.loads(result[grouping_columnuuid])
                    elif len(result[grouping_columnuuid]) == 1:
                        row['grouping_allocation'] = [result[grouping_columnuuid]]
                    else:
                        row['grouping_allocation'] = result[grouping_columnuuid].split(',')
                    grouping_allocation_groups.extend(row['grouping_allocation'])
                    # grow struct of teachers to groups
                    for teacher in row['teacher_allocation']:
                        if teacher in teachers_to_groups.keys():
                            teachers_to_groups[teacher].extend(row['grouping_allocation'])
                        else:
                            if teacher:
                                teachers_to_groups[teacher] = row['grouping_allocation']
                    # grow struct of students by groups
                    for group in row['grouping_allocation']:
                        if group in students_by_groups.keys():
                            students_by_groups[group].append(row['identifier'])
                        else:
                            students_by_groups[group] = [row['identifier']]
                elif grouping_columnuuid and grouping_columnuuid in result.keys() and not result[grouping_columnuuid]:
                    row['grouping_allocation'] = ['__none__'] # special system designation for empty group
                    if '__none__' in students_by_groups.keys():
                        students_by_groups['__none__'].append(row['identifier'])
                    else:
                        students_by_groups['__none__'] = [row['identifier']]
                else:
                    row['grouping_allocation'] = []
            except:
                row['grouping_allocation'] = []
            # add row to dataset
            data[row['identifier']] = deepcopy(row)
        df_data = pandas.DataFrame.from_dict(data, orient='index')
        #print('data', df_data)
        #logging.debug('df_data ' + str(list(df_data)))
        # deduplicate
        for teacher in teachers_to_groups:
            teachers_to_groups[teacher] = list(dict.fromkeys(teachers_to_groups[teacher]))
        for group in students_by_groups:
            students_by_groups[group] = list(dict.fromkeys(students_by_groups[group]))
        # sort groups in order of name, and try interpret mon/tue/etc
        # TODO for weekday..??
        students_by_groups = dict(natsorted(students_by_groups.items(), key=lambda kv: kv[0], alg=ns.IGNORECASE))
        
        targeted_identifiers = []
        
        if insight.config['trigger_config']['trigger_type'] == 'quartiles':
            # calculate the quartiles for each column requested
            for column_reference in column_references:
                df_data[column_reference] = pandas.to_numeric(df_data[column_reference], errors='coerce')
                df_data['quartile_{}'.format(column_reference)] = pandas.qcut(
                    df_data[column_reference],
                    4,
                    labels=False,
                    duplicates='drop'
                )
            # perform comparison for each student
            df_data['match_count'] = 0
            for column_reference in column_references:
                quartiles_range = int(insight.config['trigger_config']['trigger_config_quartiles_range'])
                if quartiles_range in [1,2]:
                    df_data['match_count'] = df_data['match_count'] + df_data['quartile_{}'.format(column_reference)].apply(
                        lambda x: 1 if x <= quartiles_range - 1 else 0
                    )
                elif quartiles_range in [3,4]:
                    df_data['match_count'] = df_data['match_count'] + df_data['quartile_{}'.format(column_reference)].apply(
                        lambda x: 1 if x >= quartiles_range - 1 else 0
                    )
            #print('quartile df_data', df_data)
            if insight.config['trigger_config']['trigger_config_quartiles_combiner'] == 'any':
                df_filtered = df_data.loc[df_data['match_count'] >= 1]
            else: # all
                df_filtered = df_data.loc[df_data['match_count'] == len(column_references)]
            #print('quartile df_filtered', df_filtered)
            # collect
            targeted_identifiers = df_filtered['identifier'].tolist()
        elif insight.config['trigger_config']['trigger_type'] == 'matching':
            # perform comparison for each student for each column
            df_data['match_count'] = 0
            matching_value = insight.config['trigger_config']['trigger_config_matching_value']
            if insight.config['trigger_config']['trigger_config_matching_method'] == 'eq':
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: 1 if str(x) == str(matching_value) else 0
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] == 'neq':
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: 1 if str(x) != str(matching_value) else 0
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] == 'like':
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: 1 if str(matching_value).lower() in str(x).lower() else 0
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] == 'notlike':
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: 1 if str(matching_value).lower() not in str(x).lower() else 0
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] in ['lt', 'lte', 'gt', 'gte']:
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: _compare_values(
                            x,
                            insight.config['trigger_config']['trigger_config_matching_method'],
                            matching_value
                        )
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] == 'isnull':
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: 1 if x == '' or x is None else 0
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] == 'isnotnull':
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: 1 if x != '' and x is not None else 0
                    )
            elif insight.config['trigger_config']['trigger_config_matching_method'] == 'regex':
                def _regex_matcher(matching_value, x):
                    try:
                        if len(re.findall(matching_value, x)):
                            logging.debug('matched [{}] ... [{}] in [{}]'.format(matching_value, x, insight.config['uuid']))
                            return 1
                        return 0
                    except:
                        #logging.debug('error matching [{}] ... [{}] in [{}]'.format(matching_value, x, insight.config['uuid']))
                        return 0
                for column_reference in column_references:
                    df_data['match_count'] = df_data['match_count'] + df_data[column_reference].apply(
                        lambda x: _regex_matcher(matching_value, x)
                    )
            # check if met the threshold
            matching_count = int(insight.config['trigger_config']['trigger_config_matching_count'])
            matching_count_comparator = insight.config['trigger_config']['trigger_config_matching_count_comparator']
            if matching_count_comparator == 'eq':
                df_filtered = df_data.loc[df_data['match_count'] == matching_count]
            elif matching_count_comparator == 'lt':
                df_filtered = df_data.loc[df_data['match_count'] < matching_count]
            elif matching_count_comparator == 'gt':
                df_filtered = df_data.loc[df_data['match_count'] > matching_count]
            # collect
            targeted_identifiers = df_filtered['identifier'].tolist()
        elif insight.config['trigger_config']['trigger_type'] == 'trending':
            # slope calculator method
            def _slope(x_values, y_values):
                mask = ~numpy.isnan(x_values) & ~numpy.isnan(y_values)
                x_values, y_values = numpy.array(x_values), numpy.array(y_values)
                if len(x_values[mask]):
                    reg = stats.linregress(x_values[mask], y_values[mask])
                    return reg[0]
                else:
                    return numpy.nan
            # any data conversions specified?
            data_conversion = insight.config['trigger_config']['trigger_config_trending_data_conversion']
            if data_conversion == 'binarise':
                # presence of data as 1, absence as 0
                for column_reference in column_references:
                    df_data[column_reference] = df_data[column_reference].apply(
                        lambda x: 0 if str(x) == '' or x is None or x is numpy.nan else 1
                    )
            elif data_conversion == 'blankzero' or data_conversion == 'none':
                if data_conversion == 'blankzero':
                    # convert blanks to zero
                    for column_reference in column_references:
                        df_data[column_reference] = df_data[column_reference].apply(
                            lambda x: 0 if str(x) == '' or x is None or x is numpy.nan else x
                        )
                elif data_conversion == 'none':
                    pass
                # cast to numeric
                for column_reference in column_references:
                    df_data[column_reference] = pandas.to_numeric(df_data[column_reference], errors='coerce')
                df_data.fillna(value=numpy.nan, inplace=True)
            # calculate slopes
            df_data['slope'] = df_data.apply(
                lambda x: _slope(
                    list(range(0, len(column_references))),
                    [x[column_reference] for column_reference in column_references]
                )
            , axis=1)
            # check if met the threshold
            trending_direction = insight.config['trigger_config']['trigger_config_trending_direction']
            if trending_direction == 'down':
                df_filtered = df_data.loc[df_data['slope'] < 0]
            elif trending_direction == 'up':
                df_filtered = df_data.loc[df_data['slope'] > 0]
            # collect
            targeted_identifiers = df_filtered['identifier'].tolist()
        elif insight.config['trigger_config']['trigger_type'] == 'distance_from_average':
            # calculate the mean for each column and the distance from
            for column_reference in column_references:
                df_data[column_reference] = pandas.to_numeric(df_data[column_reference], errors='coerce')
                mean = df_data[column_reference].mean()
                df_data['distance_{}'.format(column_reference)] = df_data[column_reference].apply(
                    lambda x: x - mean
                )
            # perform comparison for each student
            df_data['match_count'] = 0
            distance_value = float(insight.config['trigger_config']['trigger_config_distance_value'])
            distance_direction = insight.config['trigger_config']['trigger_config_distance_direction']
            for column_reference in column_references:
                if distance_direction == 'lt':
                    df_data['match_count'] = df_data['match_count'] + df_data['distance_{}'.format(column_reference)].apply(
                        lambda x: 1 if x < distance_value else 0
                    )
                elif distance_direction == 'gt':
                    df_data['match_count'] = df_data['match_count'] + df_data['distance_{}'.format(column_reference)].apply(
                        lambda x: 1 if x > distance_value else 0
                    )
                if distance_direction == 'ltgt':
                    df_data['match_count'] = df_data['match_count'] + df_data['distance_{}'.format(column_reference)].apply(
                        lambda x: 1 if x < distance_value or x > distance_value else 0
                    )
            if insight.config['trigger_config']['trigger_config_distance_combiner'] == 'any':
                df_filtered = df_data.loc[df_data['match_count'] >= 1]
            else: # all
                df_filtered = df_data.loc[df_data['match_count'] == len(column_references)]
            # collect
            targeted_identifiers = df_filtered['identifier'].tolist()
        elif insight.config['trigger_config']['trigger_type'] == 'ml_outliers':
            # TODO
            pass
        
        # set up app context
        if not current_app:
            from sres import create_app
            app = create_app()
        else:
            app = current_app
        
        # prepare the data summaries per identified student
        # first preload columns
        column_helpers = {}
        column_references_in_summary = insight._get_referenced_column_references_from_content_per_student()
        for column_reference_in_summary in column_references_in_summary:
            column_helper = Column()
            if column_helper.load(column_reference_in_summary):
                if column_reference_in_summary not in column_helpers.keys():
                    column_helpers[column_reference_in_summary] = column_helper
        # then iterate
        student_summaries = {}
        student_data = StudentData(table_uuids[0])
        for targeted_identifier in targeted_identifiers:
            student_data._reset()
            student_data.find_student(targeted_identifier)
            student_summaries[targeted_identifier] = {}
            student_summaries[targeted_identifier]['base_text'] = substitute_text_variables(
                input=insight.config['content_per_student'],
                identifier=targeted_identifier,
                default_table_uuid=table_uuids[0],
                preloaded_columns=column_helpers,
                preloaded_student_data=student_data
            )['new_text']
        
        for teacher_n in range(0, len(teacher_allocation_teachers) + 1):
            # first collect the people who need to be informed and who they need to be informed about 
            current_teacher_identifier = ''
            current_targeted_identifiers = []
            alert_skipped = False
            if teacher_n == 0:
                # this is the overall coordinator teacher who should get everything
                current_targeted_identifiers = deepcopy(targeted_identifiers)
            elif teacher_n <= len(teacher_allocation_teachers):
                # if individual teacher allocations are specified, send a subset to the pertinent teachers
                current_teacher_identifier = teacher_allocation_teachers[teacher_n - 1]
                # gather all student identifiers belonging to this teacher
                students_of_this_teacher = []
                if grouping_columnuuid:
                    for group in teachers_to_groups[current_teacher_identifier]:
                        students_of_this_teacher.extend(students_by_groups[group])
                else:
                    for idx, student in df_data[['identifier', 'teacher_allocation']].iterrows():
                        if current_teacher_identifier in student['teacher_allocation']:
                            students_of_this_teacher.append(student['identifier'])
                # gather targeted student identifiers belonging to this teacher
                for targeted_identifier in targeted_identifiers:
                    if targeted_identifier in students_of_this_teacher:
                        current_targeted_identifiers.append(targeted_identifier)
            else:
                break
            # check if skipping if no students identified
            if insight.config['alert_config']['when_no_students_identified'] == 'nosend' and len(current_targeted_identifiers) == 0:
                # skip
                alert_skipped = True
            
            # second prepare the insight alert messages 
            # alert subject
            alert_subject = insight.config['content_email_subject']
            # first bit of body
            alert_body = insight.config['content_email_first']
            # data summaries
            if insight.config['content_config']['content_config_summary_logic']:
                pass
                alert_body += "<p>" + insight.get_trigger_logic(columns_info, ignored_columns_info)['text'] + "</p>"
            if insight.config['content_config']['content_config_summary_totals']:
                alert_body += "<p>{} students have been identified in this Teacher Insight.</p>".format(len(current_targeted_identifiers))
            # if for a teacher allocation teacher, then inject a message accordingly
            if current_teacher_identifier:
                alert_body += "<p>This is being generated based on students allocated to {}.</p>".format(current_teacher_identifier)
            # loop through the teachers and their groups
            if len(current_targeted_identifiers):
                if teachers_to_groups and current_teacher_identifier in teachers_to_groups.keys() and teachers_to_groups[current_teacher_identifier]:
                    # for the current teacher, show data by groups if grouping has been set
                    for group in teachers_to_groups[current_teacher_identifier]:
                        if group == '__none__':
                            alert_body += "<h2>No group designation</h2>"
                        else:
                            alert_body += "<h2>Group {}</h2>".format(group)
                        group_alert_body = []
                        for student in students_by_groups[group]:
                            if student in current_targeted_identifiers:
                                group_alert_body.append(student_summaries[student]['base_text'])
                        alert_body += "<p>{} of {} students in this group were identified.</p>".format(
                            len(group_alert_body),
                            len(students_by_groups[group])
                        )
                        alert_body += ''.join(group_alert_body)
                elif current_teacher_identifier == '' and students_by_groups:
                    for group, students in students_by_groups.items():
                        if group == '__none__':
                            alert_body += "<h2>No group designation</h2>"
                        else:
                            alert_body += "<h2>Group {}</h2>".format(group)
                        group_alert_body = []
                        for student in students_by_groups[group]:
                            if student in current_targeted_identifiers:
                                group_alert_body.append(student_summaries[student]['base_text'])
                        alert_body += "<p>{} of {} students in this group were identified.</p>".format(
                            len(group_alert_body),
                            len(students_by_groups[group])
                        )
                        alert_body += ''.join(group_alert_body)
                else:
                    for student in current_targeted_identifiers:
                        alert_body += student_summaries[student]['base_text']
            # append last bit of body
            alert_body += insight.config['content_email_last']
            # build footer
            alert_footer = []
            with app.app_context():
                alert_footer.append(
                    '<a href="{}">Edit this insight</a>'.format(
                        url_for('insight.edit_insight', insight_uuid=insight.config['uuid'], _external=True)
                    )
                )
            # check if this is the last time the insight alert will be sent
            if datetime.now() > insight.config['alert_endby']:
                alert_footer.append("This is the last scheduled day that this insight will run.")
            # append footer
            alert_body += '<p style="font-size:smaller;">' + ' | '.join(alert_footer) + "</p>"
            # third send the messages
            if teacher_n == 0:
                email_to = insight.config['alert_config']['recipient_emails']
                email_cc = ''
            else:
                email_to = find_email_for_user(current_teacher_identifier)
                email_cc = insight.config['alert_config']['recipient_emails']
            if send_email and email_to and not alert_skipped:
                # do the send
                with app.app_context():
                    msg = Message(
                        subject=alert_subject,
                        recipients=[email_to],
                        cc=re.findall("[^\s,;]+", email_cc),
                        html=alert_body,
                        charset='utf-8',
                        sender=(
                            insight.config['content_email_from_name'],
                            insight.config['content_email_from_address']
                        )
                    )
                    with app.mail.record_messages() as outbox:
                        send_result = app.mail.send(msg)
                        #print(outbox)
                        #print(outbox[0].html)
            # save to ret
            ret['alerts'].append({
                'body': alert_body,
                'subject': alert_subject,
                'to': email_to,
                'cc': email_cc,
                'skipped': alert_skipped
            })
        del app
        ret['success'] = True
        job.release_claim()
        return ret
        
def _compare_values(matching_value, operator, x):
    
    matching_value = str(matching_value) if matching_value is not None else ''
    x = str(x) if x is not None else ''
    
    def _operate(left_value, operator, right_value):
        if operator == 'lt':
            return 1 if left_value < right_value else 0
        elif operator == 'lte':
            return 1 if left_value <= right_value else 0
        elif operator == 'gt':
            return 1 if left_value > right_value else 0
        elif operator == 'gte':
            return 1 if left_value >= right_value else 0
    
    if utils.is_number(matching_value) and utils.is_number(x):
        return _operate(float(matching_value), operator, float(x))
    elif utils.is_datetime(matching_value) and utils.is_datetime(x):
        return _operate(parser.parse(matching_value), operator, parser.parse(x))
    else:
        return _operate(matching_value, operator, x)

class Insight:
    
    default_config = {
        '_referenced_column_references': [],
        'uuid': None,
        'default_table_uuid': '',
        'name': '',
        'description': '',
        'administrators': [],
        'created': None,
        'modified': None,
        'workflow_state': 'active',
        'alert_frequency': 24,
        'alert_startfrom': datetime.now(),
        'alert_endby': datetime.now() + timedelta(days=7),
        'alert_config': {
            'recipient_emails': '',
            'teacher_allocation_columnuuid': '',
            'when_no_students_identified': 'send',
            'grouping_columnuuid': '',
            'alert_starttime': '02:00',
            'alert_interval': 'hours'
        },
        'trigger_config': {
            'trigger_type': '',
            'trigger_config_quartiles_range': 1,
            'trigger_config_quartiles_combiner': 'any',
            'trigger_config_matching_count_comparator': 'eq',
            'trigger_config_matching_count': '',
            'trigger_config_matching_method': 'matches',
            'trigger_config_matching_value': '',
            'trigger_config_trending_direction': 'down',
            'trigger_config_trending_data_conversion': 'none',
            'trigger_config_distance_value': '',
            'trigger_config_distance_direction': 'lt',
            'trigger_config_distance_combiner': 'any',
            'trigger_config_ml_outliers_data_conversion_non_numerical': 'binarise',
            'trigger_config_select_columns': '', # a comma-separated list of column references
            'trigger_config_columns_ignore_state': 'disabled'
        },
        'content_email_from_name': 'Student Relationship Engagement System',
        'content_email_from_address': _get_config().SRES['NOREPLY_EMAIL'],
        'content_email_subject': "Insight from the SRES",
        'content_email_first': "<p>Hi there,</p><p>This is an automated Teacher Insight about your students.</p>",
        'content_per_student': "$PREFERREDNAME$ $SURNAME$",
        'content_email_last': "<hr>This message was generated by the Student Relationship Engagement System.",
        'content_config': {
            'content_config_summary_totals': True,
            'content_config_summary_logic': True
        },
        'extra_data': {},
        'extra_config': {}
    }
    
    def __init__(self):
        self.db = _get_db()
        self._id = None
        self.config = deepcopy(self.default_config)
    
    def load(self, insight_uuid):
        filter = {}
        filter['uuid'] = utils.clean_uuid(insight_uuid)
        results = self.db.insights.find(filter)
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
            return True
        else:
            return False
    
    def update(self, override_username=None):
        if self.is_user_authorised(override_username):
            self.config['_referenced_column_references'] = self.get_referenced_column_references()
            result = self.db.insights.update_one({'uuid': self.config['uuid']}, {'$set': self.config})
            return result.acknowledged
        else:
            return False
        
    def create(self):
        if is_user_administrator('filter') or is_user_administrator('super'):
            pass
        else:
            return False
        self.config['uuid'] = utils.create_uuid()
        result = self.db.insights.insert_one(self.config)
        if result.acknowledged and self.load(self.config['uuid']):
            return self.config['uuid']
        else:
            return None
    
    def clone(self):
        """
            Clones the current insight. Returns the new uuid (string) if successful,
            or None if not.
        """
        insight_clone = Insight()
        if insight_clone.create():
            source_insight_config = deepcopy(self.config)
            # remove keys that should not be cloned
            del source_insight_config['uuid']
            # update some keys
            source_insight_config['created'] = datetime.now()
            source_insight_config['name'] = 'Clone of {}'.format(source_insight_config['name'])
            source_insight_config['description'] = '[Cloned insight] {}'.format(source_insight_config['description'])
            insight_clone.config = {**insight_clone.config, **source_insight_config}
            # save
            if insight_clone.update():
                return insight_clone.config['uuid']
        return None
    
    def delete(self):
        self.config['workflow_state'] = 'deleted'
        self.schedule(action='remove')
        return self.update()
    
    def is_user_authorised(self, username=None):
        if is_user_administrator('super'):
            return True
        if username is None:
            username = get_auth_user()
        user = User()
        if user.find_user(username=username):
            if user._id in self.config['administrators']:
                return True
        return False
    
    def get_referenced_column_references(self, order_by_prevalence=True, deduplicate=True):
        """
            Returns a list of string references of columns that are referenced in the current insight.
        """
        all_column_references = []
        # trigger_config_select_columns
        all_column_references.extend(self.config['trigger_config']['trigger_config_select_columns'].split(','))
        # grouping_columnuuid
        if self.config['alert_config']['grouping_columnuuid']:
            all_column_references.append(self.config['alert_config']['grouping_columnuuid'])
        # teacher_allocation_columnuuid
        if self.config['alert_config']['teacher_allocation_columnuuid']:
            all_column_references.append(self.config['alert_config']['teacher_allocation_columnuuid'])
        # email content
        all_column_references.extend(self._get_referenced_column_references_from_content_per_student())
        #print('all_column_references', all_column_references)
        # clean delimiters
        all_column_references = utils.clean_delimiter_from_column_references(all_column_references)
        # order by preference
        if order_by_prevalence:
            all_column_references = [r for r, c in collections.Counter(all_column_references).most_common() for r in [r] * c]
        # de-duplicate
        if deduplicate:
            all_column_references = list(dict.fromkeys(all_column_references))
        return all_column_references
    
    def _get_referenced_column_references_from_content_per_student(self):
        return re.findall(
            utils.DELIMITED_COLUMN_REFERENCE_PATTERN, 
            self.config['content_per_student']
        )
    
    def get_referenced_table_uuids(self, order_by_prevalence=True, deduplicate=True):
        """
            Returns a list of string uuids of tables that correspond to columns referenced
            in the current insight.
        """
        all_column_references = self.get_referenced_column_references(order_by_prevalence, deduplicate)
        # figure out tables
        all_table_uuids = table_uuids_from_column_references(all_column_references)
        #print('all_table_uuids', all_table_uuids)
        return all_table_uuids
    
    def get_authorised_usernames(self):
        if self.config['administrators']:
            return [v for k, v in oids_to_usernames(self.config['administrators']).items()]
        else:
            return []
    
    def get_job_id(self):
        return 'sres_insight_alert_a{}'.format(self.config['uuid'])
    
    def schedule(self, action='save'):
        """Updates the scheduler for this insight.
            
            action (str) save|remove
        """
        ret = {
            'success': False,
            'messages': []
        }
        job_id = self.get_job_id()
        # delete any set schedule
        if current_app.scheduler.get_job(job_id):
            current_app.scheduler.remove_job(job_id)
            if action == 'remove':
                ret['success'] = True
                return ret
        # set up the schedule
        if self.config['alert_endby'].date() >= datetime.now().date():
            start_date = self.config['alert_startfrom'].strftime('%Y-%m-%d') + ' ' + self.config['alert_config']['alert_starttime'] + ':00'
            end_date = self.config['alert_endby']
            if self.config['alert_config']['alert_interval'] == 'hours':
                current_app.scheduler.add_job(
                    run_insight,
                    args=(self.config['uuid'],),
                    trigger='interval',
                    start_date=start_date,
                    end_date=end_date,
                    hours=self.config['alert_frequency'],
                    coalesce=True,
                    id=job_id,
                    max_instances=1,
                    misfire_grace_time=1800
                )
                ret['messages'].append(("Scheduled.", "success"))
            elif self.config['alert_config']['alert_interval'] == 'week':
                current_app.scheduler.add_job(
                    run_insight,
                    args=(self.config['uuid'],),
                    trigger='interval',
                    start_date=start_date,
                    end_date=end_date,
                    weeks=1,
                    coalesce=True,
                    id=job_id,
                    max_instances=1,
                    misfire_grace_time=1800
                )
                ret['messages'].append(("Scheduled.", "success"))
        return ret
    
    def get_trigger_logic(self, columns_info, ignored_columns_info):
        
        ret = {
            'text': ''
        }
        
        text = "This Teacher Insight alert "
        column_list_friendly = []
        for column_reference, column_info in columns_info.items():
            column_list_friendly.append(column_info['friendly_display'])
        column_list_friendly = ', '.join(column_list_friendly)
        
        if self.config['trigger_config']['trigger_type'] == 'quartiles':
            text += "finds students who are in the "
            text += _get_form_option_display_by_value('trigger_config_quartiles_range', self.config['trigger_config']['trigger_config_quartiles_range'])
            text += " quartile(s) for <strong>"
            text += _get_form_option_display_by_value('trigger_config_quartiles_combiner', self.config['trigger_config']['trigger_config_quartiles_combiner'])
            text += "</strong> of " + column_list_friendly + "."
        elif self.config['trigger_config']['trigger_type'] == 'matching':
            text += "finds students where "
            text += _get_form_option_display_by_value('trigger_config_matching_count_comparator', self.config['trigger_config']['trigger_config_matching_count_comparator'])
            text += " " + self.config['trigger_config']['trigger_config_matching_count']
            text += " of " + column_list_friendly
            text += " " + _get_form_option_display_by_value('trigger_config_matching_method', self.config['trigger_config']['trigger_config_matching_method'])
            text += " " + self.config['trigger_config']['trigger_config_matching_value'] + "."        
        elif self.config['trigger_config']['trigger_type'] == 'trending':
            text += "finds students where "
            text += column_list_friendly
            text += " trends "
            text += _get_form_option_display_by_value('trigger_config_trending_direction', self.config['trigger_config']['trigger_config_trending_direction'])
            text += "."
        elif self.config['trigger_config']['trigger_type'] == 'distance_from_average':
            text += "finds students where "
            text += _get_form_option_display_by_value('trigger_config_distance_combiner', self.config['trigger_config']['trigger_config_distance_combiner'])
            text += " of "
            text += column_list_friendly
            text += " is/are "
            text += _get_form_option_display_by_value('trigger_config_distance_direction', self.config['trigger_config']['trigger_config_distance_direction'])
            text += " the average"
            text += "." if self.config['trigger_config']['trigger_config_distance_value'] == '' else " by " + self.config['trigger_config']['trigger_config_distance_value'] + "."
        elif self.config['trigger_config']['trigger_type'] == 'ml_outliers':
            # TODO
            pass
        
        if ignored_columns_info:
            ignored_column_list_friendly = []
            for column_reference, column_info in ignored_columns_info.items():
                ignored_column_list_friendly.append(column_info['friendly_display'])
            ignored_column_list_friendly = ', '.join(ignored_column_list_friendly)
            text += " (Information in {} was ignored".format(ignored_column_list_friendly)
            if self.config['trigger_config']['trigger_config_columns_ignore_state'] == 'untilactivefrom':
                text += " because they have not yet become active"
            elif self.config['trigger_config']['trigger_config_columns_ignore_state'] == 'untilactiveto':
                text += " because they have not yet concluded"
            text += ".)"
            
        ret['text'] = text
        return ret
