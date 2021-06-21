from flask import current_app, g
import json
import requests
import pandas as pd
from copy import deepcopy
from datetime import datetime, timedelta
from dateutil import parser, tz
import re
import bleach
from natsort import natsorted, ns
from queue import Queue
from threading import Thread
from bs4 import BeautifulSoup
import base64
import os, sys
import logging
from urllib import parse
from hashlib import sha1
import pickle

from sres.db import DbCookie, _get_db
from sres.auth import get_auth_user, is_user_administrator
from sres import utils
from sres.studentdata import StudentData, run_aggregation_bulk
from sres.tables import Table, USER_ROLES
from sres.columns import find_column_by_name, Column, ColumnReferences
from sres.aggregatorcolumns import find_aggregators_of_columns, AggregatorColumn
from sres.config import _get_proxies, _get_config
from sres.users import usernames_to_oids

def _encrypt_auth_token(unencrypted_token):
    return utils.encrypt_to_hex(unencrypted_token)

def _decrypt_auth_token(encrypted_token):
    return utils.decrypt_from_hex(encrypted_token)

def _make_job_id(connection_type, table_uuid, identifiers=None):
    if identifiers is None or (isinstance(identifiers, list) and len(identifiers) == 0):
        return 'sres_connector_canvas_{}_t{}'.format(
            connection_type,
            table_uuid
        )
    else:
        return 'sres_connector_canvas_{}_t{}_i{}'.format(
            connection_type,
            table_uuid,
            sha1(str(identifiers).encode()).hexdigest()
        )

CONNECTION_META = {
    'student_enrollment': {
        'type': 'student_enrollment',
        'requires_course_connection': True,
        'frequency': 6,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Import student enrollments <em>from</em> Canvas",
        'description': "Enrollments will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_enrollments_student',
        'display': 'Student enrollments',
        'additional_form_elements': [
            {
                'name': 'canvas_connect_enrollments_student_overwrite',
                'key': 'overwrite',
                'label': "Update details of existing students",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'overwrite',
                        'display': "Yes"
                    },
                    {
                        'value': 'nooverwrite',
                        'display': "No"
                    }
                ]
            },
            {
                'name': 'canvas_connect_enrollments_student_import_avatar_urls',
                'key': 'import_avatar_urls',
                'label': "Import avatars",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': '0',
                        'display': "No"
                    },
                    {
                        'value': 'import',
                        'display': "Yes"
                    }
                ]
            },
            {
                'name': 'canvas_connect_enrollments_student_import_pronouns',
                'key': 'import_pronouns',
                'label': "Import pronouns (if specified)",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': '0',
                        'display': "No"
                    },
                    {
                        'value': 'import',
                        'display': "Yes"
                    }
                ]
            },
            {
                'name': 'canvas_connect_enrollments_student_inactivate_students_not_in_import',
                'key': 'inactivate_students_not_in_import',
                'label': "Remove student enrollments from existing list if they are not found in the Canvas course(s)",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'inactivate',
                        'display': "Yes"
                    },
                    {
                        'value': 'keep',
                        'display': "No - keep all students"
                    }
                ]
            }
        ]
    },
    'teacher_enrollment': {
        'type': 'teacher_enrollment',
        'requires_course_connection': True,
        'frequency': 6,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Import teacher enrollments <em>from</em> Canvas",
        'description': "Teacher enrollments will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_enrollments_teacher',
        'display': 'Teacher enrollments',
        'additional_form_elements': [
            {
                'name': 'canvas_connect_enrollments_teacher_overwrite',
                'key': 'overwrite',
                'label': "Overwrite existing authorised staff based on Canvas records",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'no',
                        'display': "No"
                    },
                    {
                        'value': 'yes',
                        'display': "Yes"
                    }
                ]
            },
            {
                'name': 'canvas_connect_enrollments_teacher_type_mapping_TeacherEnrollment',
                'key': 'type_mapping_TeacherEnrollment',
                'label': "Import staff with 'teacher' roles",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'administrator',
                        'display': "Yes, as SRES list administrators"
                    },
                    {
                        'value': 'user',
                        'display': "Yes, as SRES list users"
                    },
                    {
                        'value': 'auditor',
                        'display': "Yes, as SRES list auditors"
                    },
                    {
                        'value': 'no',
                        'display': "No"
                    },
                ]
            },
            {
                'name': 'canvas_connect_enrollments_teacher_type_mapping_TaEnrollment',
                'key': 'type_mapping_TaEnrollment',
                'label': "Import staff with 'teaching assistant' roles",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'user',
                        'display': "Yes, as SRES list users"
                    },
                    {
                        'value': 'auditor',
                        'display': "Yes, as SRES list auditors"
                    },
                    {
                        'value': 'administrator',
                        'display': "Yes, as SRES list administrators"
                    },
                    {
                        'value': 'no',
                        'display': "No"
                    },
                ]
            },
            {
                'name': 'canvas_connect_enrollments_teacher_type_mapping_DesignerEnrollment',
                'key': 'type_mapping_DesignerEnrollment',
                'label': "Import staff with 'designer' roles",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'no',
                        'display': "No"
                    },
                    {
                        'value': 'administrator',
                        'display': "Yes, as SRES list administrators"
                    },
                    {
                        'value': 'user',
                        'display': "Yes, as SRES list users"
                    },
                    {
                        'value': 'auditor',
                        'display': "Yes, as SRES list auditors"
                    },
                ]
            },
            {
                'name': 'canvas_connect_enrollments_teacher_type_mapping_ObserverEnrollment',
                'key': 'type_mapping_ObserverEnrollment',
                'label': "Import staff with 'observer' roles",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'auditor',
                        'display': "Yes, as SRES list auditors"
                    },
                    {
                        'value': 'user',
                        'display': "Yes, as SRES list users"
                    },
                    {
                        'value': 'administrator',
                        'display': "Yes, as SRES list administrators"
                    },
                    {
                        'value': 'no',
                        'display': "No"
                    },
                ]
            },
        ]
    },
    'user_enrollments': {
        'type': 'user_enrollments',
        'admin_only': True,
        'system_level_connection': True,
        'requires_course_connection': False,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Sync user enrollments <em>from</em> Canvas",
        'description': "User enrollments will be requested from Canvas every {frequency} hours. This includes the course codes of active, completed, and inactive enrollments. This can only be modified by an administrator.",
        'form_element': 'canvas_connect_user_enrollments',
        'display': 'User enrollments',
        'additional_form_elements': [
            {
                'name': 'canvas_connect_user_enrollments_update_when_student_added',
                'key': 'triggerable',
                'label': "Also request sync when student is added to list",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'no',
                        'display': "No"
                    },
                    {
                        'value': 'yes',
                        'display': "Yes"
                    }
                ]
            }
        ]
    },
    'gradebook': {
        'type': 'gradebook',
        'requires_course_connection': True,
        'frequency': 3,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import Gradebook scores <em>from</em> Canvas",
        'description': "Gradebook will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_gradebook',
        'display': 'Gradebook',
        'additional_form_elements': [
            {
                'name': 'gradebook_assignment_ids',
                'key': 'gradebook_assignment_ids',
                'type': 'gradebook_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose gradebook columns (assignments)",
                'hint': "By default, data for <em>all</em> assignments (gradebook columns) will be imported. Recall that each Canvas gradebook column must correspond to a Canvas assignment. You can limit which assignments (gradebook columns) are imported by specifying them below. If none are specified, the default is to load all assignments (gradebook columns)."
            }
        ]
    },
    'gradebook_custom_columns': {
        'type': 'gradebook_custom_columns',
        'requires_course_connection': True,
        'frequency': 6,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import Gradebook custom column data <em>from</em> Canvas",
        'description': "Custom column data from gradebook will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_gradebook_custom_columns',
        'display': 'Gradebook custom columns'
    },
    'sections': {
        'type': 'sections',
        'requires_course_connection': True,
        'frequency': 12,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import section memberships <em>from</em> Canvas",
        'description': "Section memberships will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_sections',
        'display': 'Sections'
    },
    'groups': {
        'type': 'groups',
        'requires_course_connection': True,
        'frequency': 12,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import group memberships <em>from</em> Canvas",
        'description': "Group memberships will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_groups',
        'display': 'Groups'
    },
    'module_completion': {
        'type': 'module_completion',
        'requires_course_connection': True,
        'frequency': 2,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import module completion states <em>from</em> Canvas",
        'description': "If modules are used and completion requirements are set up, module completion states will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_module_completion',
        'display': 'Modules and module items completion states'
    },
    'recent_students': {
        'type': 'recent_students',
        'requires_course_connection': True,
        'frequency': 12,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import student activity times <em>from</em> Canvas",
        'description': "Students' latest activity timestamp to your course, and total activity time in minutes, will be requested from Canvas every {frequency} hours. <a href=\"https://community.canvaslms.com/docs/DOC-10026\" target=\"_blank\">More information</a> about last activity and total activity time. Note that Canvas mobile app usage is not currently collected by Canvas and so will not be counted here.",
        'form_element': 'canvas_connect_recent_students',
        'display': 'Recent student logins'
    },
    'analytics_student_summaries': {
        'type': 'analytics_student_summaries',
        'requires_course_connection': True,
        'frequency': 12,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import analytics student summaries <em>from</em> Canvas",
        'description': "Analytics student summaries will be requested from Canvas every {frequency} hours. Page views and participations will be imported. <a href=\"https://community.canvaslms.com/docs/DOC-10299\" target=\"_blank\">More information</a>. Note that Canvas mobile app usage is not currently collected by Canvas and so will not be counted here.",
        'form_element': 'canvas_connect_analytics_student_summaries',
        'display': 'Analytics student summaries'
    },
    'discussion_engagement_overall': {
        'type': 'discussion_engagement_overall',
        'requires_course_connection': True,
        'frequency': 12,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import overall discussion engagement <em>from</em> Canvas",
        'description': "Overall engagement with discussions (posts and replies) will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_discussion_engagement_overall',
        'display': 'Overall discussion engagement',
        'additional_form_elements': [
            {
                'name': 'canvas_connect_discussion_engagement_overall_import_posts_text',
                'key': 'import_posts_text',
                'label': "Also import text of posts",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'disabled',
                        'display': "No"
                    },
                    {
                        'value': 'enabled',
                        'display': "Yes"
                    }
                ]
            },
            {
                'name': 'canvas_connect_discussion_engagement_overall_import_replies_text',
                'key': 'import_replies_text',
                'label': "Also import text of replies",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'disabled',
                        'display': "No"
                    },
                    {
                        'value': 'enabled',
                        'display': "Yes"
                    }
                ]
            }
        ]
    },
    'discussion_engagement_by_topic': {
        'type': 'discussion_engagement_by_topic',
        'requires_course_connection': True,
        'frequency': 12,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import discussion engagement by topic <em>from</em> Canvas",
        'description': "Topic-by-topic engagement with discussions (posts and replies) will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_discussion_engagement_by_topic',
        'display': 'Discussion engagement by topic',
        'additional_form_elements': [
            {
                'name': 'canvas_connect_discussion_engagement_by_topic_import_posts_text',
                'key': 'import_posts_text',
                'label': "Also import text of posts",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'disabled',
                        'display': "No"
                    },
                    {
                        'value': 'enabled',
                        'display': "Yes"
                    }
                ]
            },
            {
                'name': 'canvas_connect_discussion_engagement_by_topic_import_replies_text',
                'key': 'import_replies_text',
                'label': "Also import text of replies",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'disabled',
                        'display': "No"
                    },
                    {
                        'value': 'enabled',
                        'display': "Yes"
                    }
                ]
            }
        ]
    },
    'quiz_submissions_question_text': {
        'type': 'quiz_submissions_question_text',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import question text for quiz submissions <em>from</em> Canvas",
        'description': "Question text (the 'stem') for quiz submissions will be requested from Canvas every {frequency} hours. These will be sorted into questions answered correctly and questions answered incorrectly.",
        'form_element': 'canvas_connect_quiz_submissions_question_text',
        'display': 'Quiz submissions question text',
        'additional_form_elements': [
            {
                'name': 'quiz_ids',
                'key': 'quiz_ids',
                'type': 'quiz_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose quiz(zes)",
                'hint': "If left empty, defaults to <em>no</em> quizzes in selected courses"
            }
        ]
    },
    'assignments_submission_comments': {
        'type': 'assignments_submission_comments',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import comments for assignment submissions <em>from</em> Canvas",
        'description': "Comments (feedback) on submissions made for assignments via SpeedGrader will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_assignments_submission_comments',
        'display': 'Comments on assignment submissions',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            }
        ]
    },
    'assignments_rubric_outcomes': {
        'type': 'assignments_rubric_outcomes',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import rubric outcomes for assignment submissions <em>from</em> Canvas",
        'description': "Rubric outcomes (rubric assessments) on submissions made for assignments via SpeedGrader will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_assignments_rubric_outcomes',
        'display': 'Rubric outcomes on assignment submissions',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            },
            {
                'name': 'canvas_connect_assignments_rubric_outcomes_import_comments',
                'key': 'import_comments',
                'label': "Import rubric comments",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'disabled',
                        'display': "No"
                    },
                    {
                        'value': 'enabled',
                        'display': "Yes"
                    }
                ]
            }
        ]
    },
    'assignments_peer_review_scores': {
        'type': 'assignments_peer_review_scores',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import peer review statuses and peer scores and comments for assignment submissions <em>from</em> Canvas",
        'description': "Peer review statuses (number of completed and assigned), scores (according to the rubric, and total), and peer comments for submissions made for assignments via SpeedGrader will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_assignments_peer_review_scores',
        'display': 'Peer review outcomes for assignment submissions',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            },
            {
                'name': 'canvas_connect_assignments_peer_review_scores_include_comments',
                'key': 'include_comments',
                'label': "Import peer comments",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'no',
                        'display': "No"
                    },
                    {
                        'value': 'import',
                        'display': "Yes"
                    }
                ]
            },
            {
                'name': 'canvas_connect_assignments_peer_review_scores_criterion_aggregation_method',
                'key': 'criterion_aggregation_method',
                'label': "Operation to perform on per-criterion peer scores",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'mean',
                        'display': "Calculate mean"
                    },
                    {
                        'value': 'median',
                        'display': "Calculate median"
                    },
                    {
                        'value': 'sum',
                        'display': "Calculate sum"
                    }
                ]
            },
            {
                'name': 'canvas_connect_assignments_peer_review_scores_overall_aggregation_method',
                'key': 'overall_aggregation_method',
                'label': "Operation to perform on overall peer scores",
                'hint': "",
                'type': 'select',
                'options': [
                    {
                        'value': 'mean',
                        'display': "Calculate mean"
                    },
                    {
                        'value': 'median',
                        'display': "Calculate median"
                    },
                    {
                        'value': 'sum',
                        'display': "Calculate sum"
                    }
                ]
            }
        ]
    },
    'assignments_submission_attachments': {
        'type': 'assignments_submission_attachments',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import the download links of the files for assignment submissions <em>from</em> Canvas",
        'description': "The links to the file(s) submitted by students for assignment submission(s) will be requested from Canvas every {frequency} hours.",
        'form_element': 'canvas_connect_assignments_submission_attachments',
        'display': 'Files of assignment submissions',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            }
        ]
    },
    'tii_assignments_review_status': {
        'type': 'tii_assignments_review_status',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import review status of Turnitin assignments <em>from</em> Canvas",
        'description': "Review status of Turnitin assignments (whether and when students view their Grademark feedback) will be requested from Canvas and Turnitin every {frequency} hours.",
        'form_element': 'canvas_connect_tii_assignments_review_status',
        'display': 'Turnitin assignments\' review status',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            }
        ]
    },
    'tii_assignments_quickmarks_usage': {
        'type': 'tii_assignments_quickmarks_usage',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import Quickmark usage in Turnitin assignments <em>from</em> Canvas",
        'description': "Quickmarks used in Turnitin assignments will be requested from Canvas and Turnitin every {frequency} hours.",
        'form_element': 'canvas_connect_tii_assignments_quickmarks_usage',
        'display': 'Turnitin assignments\' Quickmarks usage',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            }
        ]
    },
    'tii_assignments_grademark_rubric': {
        'type': 'tii_assignments_grademark_rubric',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import Grademark rubric outcomes in Turnitin assignments <em>from</em> Canvas",
        'description': "Grademark rubric outcomes (achievement as graded via Turnitin rubrics) in Turnitin assignments will be requested from Canvas and Turnitin every {frequency} hours.",
        'form_element': 'canvas_connect_tii_assignments_grademark_rubric',
        'display': 'Turnitin assignments\' Grademark rubric outcomes',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            }
        ]
    },
    'tii_assignments_summary_comments': {
        'type': 'tii_assignments_summary_comments',
        'requires_course_connection': True,
        'frequency': 24,
        'custom_config_card': False,
        'one_way_notice': True,
        'label': "Bulk import summary feedback comments from Turnitin assignments <em>from</em> Canvas",
        'description': "The overall text-based feedback comment left for students in Turnitin assignments will be requested from Canvas and Turnitin every {frequency} hours.",
        'form_element': 'canvas_connect_tii_assignments_summary_comments',
        'display': 'Turnitin assignments\' summary comments',
        'additional_form_elements': [
            {
                'name': 'tii_assignment_ids',
                'key': 'only_assignment_ids',
                'type': 'assignment_chooser',
                '_list': True,
                '_type': 'int',
                'label': "Choose assignments",
                'hint': "If left empty, defaults to all assignments in selected courses"
            }
        ]
    }
}

def is_canvas_connection_enabled():
    _config = _get_config()
    if _config.SRES.get('LMS', {}).get('canvas', {}).get('enabled') == True:
        return True
    return False

def import_handler(connection_type, table_uuid, canvas_course_ids, override_username, once_off=False, system_level_connection=False, identifiers=[]):
    time_start = datetime.now()
    res = {
        'success': False,
        'messages': []
    }
    try:
        # check if already running somewhere
        from sres.jobs import APSJob
        job_id = _make_job_id(connection_type, table_uuid, identifiers)
        job = APSJob(job_id)
        if not job.claim_job(skip_loading=True if once_off or system_level_connection else False):
            res['messages'].append(("Job already running.", "warning"))
            logging.info('Job already running, not starting again [{}]'.format(job_id))
            return res
    except Exception as e:
        print(e)
    # continue
    try:
        connector = CanvasConnector(_override_username=override_username)
        # grab connection details
        connection_details = connector.get_connection(table_uuid, connection_type, system_level_connection=system_level_connection)
        # see if enabled
        if not connection_details['enabled']:
            logging.debug('not running because not enabled [{}] [{}]'.format(connection_type, table_uuid))
            connector.schedule_task(action='delete', table_uuid=table_uuid, connection_type=connection_type)
            job.release_claim()
            return res
        # continue
        print('IMPORT_HANDLER', connection_type, table_uuid, canvas_course_ids, override_username, connection_details)
        logging.info("connector_canvas.import_handler [{}] [{}] [{}] [{}] [{}]".format(
            connection_type,
            table_uuid,
            str(canvas_course_ids),
            override_username,
            str(connection_details)
        ))
        if not canvas_course_ids:
            canvas_course_ids = connection_details['canvas_course_ids']
        if 'username' in connection_details.keys():
            connector.override_username = connection_details['username']
        connector._import_worker(connection_type, table_uuid, canvas_course_ids, connection_details, res, identifiers=identifiers)
        job.release_claim()
        return res
    except Exception as e:
        logging.error('Exception running job [{}]'.format(job_id))
        logging.exception(e)
        print(e)
        job.release_claim()

def run_triggerable_connections(table_uuid, identifiers=[]):
    for con_id, con in CONNECTION_META.items():
        if con.get('system_level_connection') == True:
            connector = CanvasConnector(_override_username='__system__')
            connection = connector.get_connection(table_uuid, con_id, system_level_connection=True)
            if connection.get('enabled') and connection.get('triggerable') == 'yes':
                import_handler(
                    con_id,
                    table_uuid,
                    [],
                    connection.get('username', '__system__'),
                    system_level_connection=True,
                    identifiers=identifiers
                )
    
class CanvasConnector:
    
    def __init__(self, is_cron=False, _override_auth_token=None, _override_username=None):
        self.is_cron = False
        self.override_username = None
        self.config = {}
        self._override_auth_token = _override_auth_token
        
        self.connections = {}
        self.connected_course_ids = []
        
        # import config for db directly from instance - this is needed because 
        # this method could be called from outside of an active request
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from instance.config import SRES as SRES_CONFIG
        if 'canvas' in SRES_CONFIG['LMS'].keys():
            self.config = deepcopy(SRES_CONFIG['LMS']['canvas']['config'])
            self.config['api_url'] = self.config['base_url'] + self.config['api_path']
        
        # if is_cron and get_auth_user()[0:6] == '_cron-':
        #     self.is_cron = True
        #     self.override_username = get_auth_user()[6:]
        if _override_username is not None and is_user_administrator('super'):
            self.override_username = _override_username
        else:
            try:
                self.override_username = get_auth_user()
            except:
                self.override_username = _override_username
        
        self.db_cookie = DbCookie(self.override_username or get_auth_user())
        self.data_logger = logging.getLogger('sres.db.studentdata')
    
    def check_token_validity(self):
        """Basic token checker by requesting user's own details."""
        result = self._send_request(
            method='GET', 
            url='{api_url}users/self'.format(api_url=self.config['api_url'])
        )
        if result is not None and result['status_code'] == 200:
            return True
        return False
    
    def set_auth_token(self, token):
        return self.set_token(token, 'auth')
    
    def set_token(self, token, token_type, expires=None):
        if not expires:
            expires = datetime.now() + timedelta(days=365)
        if token_type in ['auth', 'refresh']:
            return self.db_cookie.set(
                key='sres.connector.canvas.{}_token'.format(token_type), 
                value=_encrypt_auth_token(token), 
                expires=None, 
                use_key_as_is=False)
    
    def get_auth_token(self):
        if 'override_auth_token' in self.config.keys() and self.config['override_auth_token']:
            return self.config['override_auth_token']
        if not self._override_auth_token:
            return self.get_token('auth')
        else:
            return self._override_auth_token
    
    def get_token(self, token_type):
        if token_type in ['auth', 'refresh']:
            token = self.db_cookie.get(key='sres.connector.canvas.{}_token'.format(token_type))
            if token:
                return _decrypt_auth_token(token)
        return ''
    
    def get_connecting_users_by_table(self, table_uuid):
        ret = []
        # TODO this is used for act_as functionality
    
    def set_connection(self, table_uuid, canvas_course_ids, connection_type, additional_data):
        ret = {
            'success': False,
            'messages': []
        }
        auth_token = self.get_auth_token()
        if not auth_token:
            ret['messages'].append(("Authorisation token unavailable.", "danger"))
        else:
            settings = {
                'enabled': True,
                'table_uuid': table_uuid,
                'canvas_course_ids': canvas_course_ids,
                'username': self.override_username if self.override_username else get_auth_user(),
                'frequency': CONNECTION_META[connection_type]['frequency']
            }
            settings = {**settings, **additional_data}
            ret['success'] = self.db_cookie.set(
                key='sres.connector.canvas.{}.t{}'.format(connection_type, table_uuid), 
				value=json.dumps(settings)
            )
            if ret['success']:
                ret['messages'].append(("Successfully updated: {}".format(CONNECTION_META[connection_type]['display']), "success"))
            else:
                ret['messages'].append(("Could not save settings.", "warning"))
        return ret
    
    def get_connection(self, table_uuid, connection_type, system_level_connection=False):
        if system_level_connection:
            cookie = self.db_cookie.get_like(
                key_pattern='sres.connector.canvas.{}.t{}'.format(connection_type, table_uuid),
                ignore_username=True,
                get_latest_only=True,
                default=''
            )
            cookie = cookie.get('value', None)
        else:
            cookie = self.db_cookie.get(
                key='sres.connector.canvas.{}.t{}'.format(connection_type, table_uuid)
            )
        ret = {
            'enabled': False,
            'table_uuid': table_uuid,
            'canvas_course_ids': [],
            'frequency': 24
        }
        if cookie:
            connection_data = json.loads(cookie)
            if 'additional_form_elements' in CONNECTION_META[connection_type]:
                # to accommodate for legacy, need to check if some connection settings should be lists not strings
                for additional_form_element in CONNECTION_META[connection_type]['additional_form_elements']:
                    if additional_form_element.get('_list') == True:
                        if additional_form_element['key'] in connection_data.keys():
                            if not isinstance(connection_data[additional_form_element['key']], list):
                                setting = str(connection_data[additional_form_element['key']])
                                if len(setting) > 0:
                                    setting = setting.split(',')
                                    if additional_form_element.get('_type') == 'int':
                                        setting = [int(s) for s in setting]
                                else:
                                    setting = []
                                connection_data[additional_form_element['key']] = setting
            # convert legacy storage method which was array of strings
            canvas_course_ids = connection_data['canvas_course_ids']
            if len(canvas_course_ids) > 0:
                canvas_course_ids = [int(c) for c in canvas_course_ids]
                connection_data['canvas_course_ids'] = canvas_course_ids
            # return
            return {**ret, **connection_data}
        else:
            return ret
    
    def unset_connection(self, table_uuid, connection_type):
        return self.db_cookie.delete(key='sres.connector.canvas.{}.t{}'.format(connection_type, table_uuid))
    
    def disable_connection(self, table_uuid, connection_type):
        connection = self.get_connection(table_uuid=table_uuid, connection_type=connection_type)
        connection['enabled'] = False
        return self.db_cookie.set(
            key='sres.connector.canvas.{}.t{}'.format(connection_type, table_uuid), 
            value=json.dumps(connection)
        )
    
    def load_connections(self, table_uuid):
        for con_id, con in CONNECTION_META.items():
            self.connections[con_id] = self.get_connection(table_uuid=table_uuid, connection_type=con_id)
    
    def load_connected_course_ids(self):
        for con_id, con in self.connections.items():
            self.connected_course_ids.extend(con['canvas_course_ids'])
        # remove duplicates
        self.connected_course_ids = list(dict.fromkeys(self.connected_course_ids))
    
    def schedule_task(self, action, table_uuid, connection_type, canvas_course_ids=[], run_now=False):
        ret = {
            'success': False,
            'messages': []
        }
        job_id = _make_job_id(connection_type, table_uuid)
        print(action.upper(), 'job_id', job_id, run_now)
        if action == 'update':
            current_app.scheduler.add_job(
                import_handler,
                args=(connection_type, table_uuid, canvas_course_ids, self.override_username),
                trigger='interval',
                minutes=CONNECTION_META[connection_type]['frequency'] * 60, # convert hours to minutes,
                max_instances=1,
                coalesce=True,
                id=job_id,
                replace_existing=True,
                misfire_grace_time=600
            )
        elif action == 'delete':
            try:
                if not current_app:
                    from sres import create_app
                    app = create_app()
                    with app.app_context():
                        current_app.scheduler.remove_job(job_id)
                else:
                    current_app.scheduler.remove_job(job_id)
            except Exception as e:
                logging.error('schedule_task remove_job error for job_id [{}]'.format(job_id))
                logging.exception(e)
        if run_now and action in ['update', 'run']:
            try:
                current_app.scheduler.add_job(
                    import_handler,
                    args=(connection_type, table_uuid, canvas_course_ids, self.override_username, True),
                    trigger='date',
                    #run_date=datetime.now() + timedelta(seconds=10),
                    max_instances=1,
                    coalesce=True,
                    id=job_id + '_oncenow',
                    replace_existing=True,
                    misfire_grace_time=60
                )
            except Exception as e:
                logging.error('schedule_task error for job_id [{}]'.format(job_id))
                logging.exception(e)
        return ret
    
    def _import_worker(self, connection_type, table_uuid, canvas_course_ids, connection_details, res, identifiers=[]):
        t0 = datetime.now()
        # check to see if all courses are concluded already - if so, run this import but unschedule
        if self.are_all_courses_concluded(canvas_course_ids):
            # delete schedule
            self.schedule_task(
                action='delete',
                table_uuid=table_uuid,
                connection_type=connection_type,
                canvas_course_ids=canvas_course_ids
            )
            # disable connection
            self.disable_connection(table_uuid, connection_type)
        # run import
        if connection_type == 'student_enrollment':
            _res = self.import_student_enrollments(table_uuid, canvas_course_ids)
        elif connection_type == 'teacher_enrollment':
            _res = self.import_teacher_enrollments(table_uuid, canvas_course_ids)
        elif connection_type == 'user_enrollments':
            table = Table()
            if table.load(table_uuid):
                _identifiers = identifiers if len(identifiers) > 0 else table.get_all_students_sids()
                df_data = self.get_user_enrollments_friendly_as_df(identifiers=_identifiers)
                _res = self._import_data(
                    table_uuid=table_uuid,
                    df_data=df_data['df'],
                    data_type=connection_type,
                    mapped_multi_entry_options=df_data['mapping'],
                )
        elif connection_type == 'gradebook':
            only_assignment_ids = connection_details['gradebook_assignment_ids'] if 'gradebook_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_gradebook = self.get_gradebook_friendly_as_df(canvas_course_ids, only_assignment_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_gradebook,
                data_type=connection_type,
                multi_entry_options=[
                    {'label':'Score', 'type':'regex', 'regex':'.*'},
                    {'label':'Workflow state', 'type':'select', 'select':'unsubmitted,submitted,graded,pending_review', 'selectmode':'single', 'regex':'.*'},
                    {'label':'Late', 'type':'select', 'select':'True,False', 'selectmode':'single', 'regex':'.*'},
                    {'label':'Submitted at', 'type':'regex', 'regex':'.*'},
                    {'label':'Grade', 'type':'regex', 'regex':'.*'}
                ]
            )
        elif connection_type == 'gradebook_custom_columns':
            df_data = self.get_gradebook_custom_columns_friendly_as_df(canvas_course_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data,
                data_type=connection_type
            )
        elif connection_type == 'sections':
            df_data = self._get_sections_by_user_for_courses(canvas_course_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data,
                data_type=connection_type
            )
        elif connection_type == 'groups':
            df_data = self.get_group_memberships_by_user_for_courses_as_df(canvas_course_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data,
                data_type=connection_type
            )
        elif connection_type == 'module_completion':
            df_data = self.get_module_completion_for_courses_as_df(canvas_course_ids)
            if df_data['success']:
                _res = self._import_data(
                    table_uuid=table_uuid,
                    df_data=df_data['df'],
                    data_type=connection_type,
                    mapped_multi_entry_options=df_data['mapping'],
                    perform_subfield_shifts=True
                )
        elif connection_type == 'recent_students':
            df_data = self.get_recent_students_as_df(canvas_course_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data,
                data_type=connection_type
            )
        elif connection_type == 'analytics_student_summaries':
            df_student_summaries = self.get_analytics_student_summaries_as_df(canvas_course_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_student_summaries,
                data_type=connection_type
            )
        elif connection_type == 'assignments_submission_comments':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_data = self.get_assignments_submission_comments_as_df(canvas_course_ids, only_assignment_ids)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data,
                data_type=connection_type
            )
        elif connection_type == 'assignments_rubric_outcomes':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            import_comments = True if connection_details.get('import_comments', '') == 'enabled' else False
            df_data = self.get_assignments_rubric_outcomes_as_df(canvas_course_ids, only_assignment_ids, import_comments)
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['data'],
                data_type=connection_type,
                mapped_multi_entry_options=df_data['rubric_mapping']
            )
        elif connection_type == 'assignments_peer_review_scores':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            include_comments = True if connection_details.get('include_comments', '') == 'import' else False
            criterion_aggregation_method = connection_details.get('criterion_aggregation_method', 'mean')
            overall_aggregation_method = connection_details.get('overall_aggregation_method', 'mean')
            df_data = self.get_assignments_peer_review_scores_as_df(
                canvas_course_ids, 
                only_assignment_ids, 
                include_comments=include_comments,
                criterion_aggregation_method=criterion_aggregation_method,
                overall_aggregation_method=overall_aggregation_method
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['data'],
                data_type=connection_type,
                mapped_multi_entry_options=df_data['mapped_multi_entry_options']
            )
        elif connection_type == 'assignments_submission_attachments':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_data = self.get_assignments_submission_attachments_as_df(
                canvas_course_ids, 
                only_assignment_ids
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['data'],
                data_type=connection_type
            )
        elif connection_type == 'tii_assignments_quickmarks_usage':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_data = self.get_tii_assignments_data_as_df(
                canvas_course_ids=canvas_course_ids, 
                data_types=["quickmarks_usage"],
                only_assignment_ids=only_assignment_ids
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['df'],
                data_type=connection_type,
                mapped_multi_entry_options=df_data['quickmarks_mapping']
            )
        elif connection_type == 'tii_assignments_review_status':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_data = self.get_tii_assignments_data_as_df(
                canvas_course_ids=canvas_course_ids, 
                data_types=["review_status"],
                only_assignment_ids=only_assignment_ids
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['df'],
                data_type=connection_type
            )
        elif connection_type == 'tii_assignments_grademark_rubric':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_data = self.get_tii_assignments_grademark_rubric_as_df(
                canvas_course_ids=canvas_course_ids, 
                only_assignment_ids=only_assignment_ids
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['df'],
                data_type=connection_type,
                mapped_multi_entry_options=df_data['rubrics_as_multientry_mapping']
            )
        elif connection_type == 'tii_assignments_summary_comments':
            only_assignment_ids = connection_details['only_assignment_ids'] if 'only_assignment_ids' in connection_details.keys() else []
            only_assignment_ids = list(set(only_assignment_ids))
            df_data = self.get_tii_assignments_data_as_df(
                canvas_course_ids=canvas_course_ids, 
                data_types=["summary_comments"],
                only_assignment_ids=only_assignment_ids
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['df'],
                data_type=connection_type
            )
        elif connection_type == 'discussion_engagement_overall':
            df_data = self.get_discussion_engagement_as_df(
                canvas_course_ids,
                mode='overall',
                fetch_posts_text=True if connection_details.get('import_posts_text') == 'enabled' else False,
                fetch_replies_text=True if connection_details.get('import_replies_text') == 'enabled' else False
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['df'],
                data_type=connection_type,
                mapped_multi_entry_options=df_data['mapping']
            )
        elif connection_type == 'discussion_engagement_by_topic':
            df_data = self.get_discussion_engagement_as_df(
                canvas_course_ids,
                mode='by_topic',
                fetch_posts_text=True if connection_details.get('import_posts_text') == 'enabled' else False,
                fetch_replies_text=True if connection_details.get('import_replies_text') == 'enabled' else False
            )
            _res = self._import_data(
                table_uuid=table_uuid,
                df_data=df_data['df'],
                data_type=connection_type,
                mapped_multi_entry_options=df_data['mapping']
            )
        elif connection_type == 'quiz_submissions_question_text':
            # special case because the import method is direct-writing
            quiz_ids = connection_details['quiz_ids'] if 'quiz_ids' in connection_details.keys() else []
            quiz_ids = list(set(quiz_ids))
            _res = self.do_import_quiz_submissions_question_text(
                canvas_course_ids=canvas_course_ids,
                target_table_uuid=table_uuid,
                quiz_ids=quiz_ids
            )
            pass
        # update res
        res = {**res, **_res}
        # no return value
        pass
        logging.info("Canvas connector _import_worker completed [{}] [{}] [{}] [{}]".format(
            connection_type,
            (datetime.now() - t0).total_seconds(),
            table_uuid,
            str(canvas_course_ids)
        ))
    
    def _import_data(self, table_uuid, df_data, data_type, multi_entry_options=[], mapped_multi_entry_options={}, perform_subfield_shifts=False):
        ret = {
            'success': False,
            'messages': []
        }
        destination_column_uuids = []
        destination_identifiers = []
        column_mappings = {} # keyed by column_uuid - maps column_uuids to column references
        preloaded_columns = {} # keyed by column_uuid
        expected_identifiers = ['id', 'canvas_id', 'sid', 'email']
        expected_identifier_in_use = ''
        # create column if not exists, otherwise update
        for column_header in list(df_data):
            if column_header in expected_identifiers + ['given_names', 'family_name', 'avatar_url', 'login_id', 'preferred_name']:
                # don't import data from these columns!
                pass
            else:
                existing_columns = find_column_by_name(
                    term=column_header,
                    table_uuid=table_uuid, 
                    exact=True,
                    return_loaded_instances=True,
                    term_is_system_name='either'
                )
                if not existing_columns:
                    # need to create new column
                    column = Column()
                    column.create(
                        table_uuid=table_uuid,
                        override_username=self.override_username
                    )
                    column.config['name'] = column_header
                    column.config['system_name'] = column_header
                    column.config['type'] = 'mark'
                    column.config['simple_input']['allow_free'] = 'true'
                    if data_type == 'gradebook' or multi_entry_options:
                        column.config['type'] = 'multiEntry'
                        column.config['multi_entry']['options'] = multi_entry_options
                    elif data_type == 'assignments_submission_attachments':
                        column.config['type'] = 'file'
                    elif data_type == 'tii_assignments_quickmarks_usage' or mapped_multi_entry_options:
                        column.config['type'] = 'multiEntry'
                        column.config['multi_entry']['options'] = mapped_multi_entry_options[column_header]['multi_entry_options']
                    column.update(override_username=self.override_username)
                else:
                    column = existing_columns[0]
                    old_multientryoptions = deepcopy(column.config['multi_entry']['options'])
                    # set the system_name again
                    column.config['system_name'] = column_header
                    # update some metadata for existing column
                    if multi_entry_options:
                        column.config['type'] = 'multiEntry'
                        column.config['multi_entry']['options'] = multi_entry_options
                        column.update(override_username=self.override_username)
                    else:
                        column.config['type'] = 'mark'
                        column.config['simple_input']['allow_free'] = 'true'
                    if mapped_multi_entry_options and column_header in mapped_multi_entry_options.keys():
                        column.config['type'] = 'multiEntry'
                        column.config['multi_entry']['options'] = mapped_multi_entry_options[column_header]['multi_entry_options']
                        column.update(override_username=self.override_username)
                    # force update aggregator references # hopefully not needed...
                    # column.table.update_all_aggregator_links_by_source_column(
                    #     source_columnuuid=column.config['uuid'],
                    #     override_username=self.override_username
                    # )
                    # perform subfield shifts if needed
                    if perform_subfield_shifts and (multi_entry_options or mapped_multi_entry_options):
                        column_references = ColumnReferences(override_username=self.override_username)
                        subfield_shifts = column_references.parse_subfield_shift(old_multientryoptions, column.config['multi_entry']['options'])
                        if subfield_shifts['shift_needed']:
                            column_references.perform_subfield_shifts(
                                column_uuid=column.config['uuid'],
                                old_to_new_mapping=subfield_shifts['old_to_new_mapping'],
                                override_username=self.override_username
                            )
                destination_column_uuids.append(column.config['uuid'])
                column_mappings[column_header] = column.config['uuid']
                preloaded_columns[column.config['uuid']] = Column()
                preloaded_columns[column.config['uuid']].load(column.config['uuid'])
        # turn nans into blank strings
        df_data.fillna('', inplace=True)
        # save data
        success_count = 0
        student_data = StudentData(table_uuid)
        for index, row in df_data.iterrows():
            student_data._reset()
            student_found = False
            if expected_identifier_in_use != '':
                student_found = student_data.find_student({
                    expected_identifier_in_use: row[expected_identifier_in_use]
                })
            if not student_found or expected_identifier_in_use == '':
                for expected_identifier in expected_identifiers:
                    if expected_identifier in row.keys():
                        student_found = student_data.find_student({
                            expected_identifier: row[expected_identifier]
                        })
                        if student_found:
                            expected_identifier_in_use = expected_identifier
                            break
            if student_found:
                destination_identifiers.append(student_data.config['sid'])
                for column_header in column_mappings:
                    current_data = row[column_header]
                    # Canvas returns null values as 'null' string
                    if current_data == None or current_data == 'null': current_data = ''
                    # set
                    student_data.set_data(
                        column_uuid=column_mappings[column_header],
                        data=current_data,
                        auth_user_override=self.override_username,
                        skip_auth_checks=True,
                        commit_immediately=False,
                        ignore_active=True,
                        only_save_history_if_delta=True, # Important so changelog isn't flooded
                        skip_aggregation=True, # Important to save resources until final aggregation,
                        preloaded_column=preloaded_columns[column_mappings[column_header]]
                    )
                # commit to db
                if student_data.save():
                    self.data_logger.info("Data committed in _import_data for {} {}".format(student_data.config['sid'], str(student_data._id)))
                    success_count += 1
            elif expected_identifier_in_use == '':
                # problem
                print('Could not find student AAA')
                pass
            else:
                # cannot find student
                print('Could not find student BBB')
                ret['messages'].append(("Could not find student.", "warning"))
                logging.warning("Could not find student, in _import_data")
            pass
        # run aggregators if necessary
        bulk_aggregation_results = run_aggregation_bulk(
            source_column_uuids=destination_column_uuids,
            target_identifiers=destination_identifiers,
            override_username=self.override_username
        )
        # update column description to be last updated date, if necessary/appropriate
        for column_header, column_uuid in column_mappings.items():
            if column_uuid in preloaded_columns.keys():
                column = preloaded_columns[column_uuid]
            else:
                column = Column()
                if not column.load(column_mappings[column_header]):
                    logging.error("Could not load column {} while running Canvas connect for table {}".format(column_mappings[column_header], table_uuid))
                    continue
            column.config['description'] = 'Last updated {}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            column.config['active']['from'] = datetime.now()
            column.config['active']['to'] = datetime.now()
            column.config['datasource']['mode'] = 'sync'
            column.config['datasource']['type'] = 'lms'
            column.config['datasource']['name'] = 'canvas'
            column.update(override_username=self.override_username)
        # return
        ret['messages'].append(("Successfully saved {} {} records.".format(str(success_count), data_type), "success"))
        logging.info("Successfully saved {} {} records to {}.".format(str(success_count), data_type, table_uuid))
        return ret
    
    def import_student_enrollments(self, table_uuid, canvas_course_ids):
        ret = {
            'success': False,
            'messages': []
        }
        connection_details = self.get_connection(table_uuid, 'student_enrollment')
        no_overwrite = 'overwrite' in connection_details.keys() and connection_details['overwrite'] == 'nooverwrite'
        import_avatar_urls = 'import_avatar_urls' in connection_details.keys() and connection_details['import_avatar_urls'] == 'import'
        import_pronouns = 'import_pronouns' in connection_details.keys() and connection_details['import_pronouns'] == 'import'
        inactivate_students_not_in_import = False if connection_details.get('inactivate_students_not_in_import') == 'keep' else True
        # fetch users from canvas
        data = {}
        expected_records = 0
        for canvas_course_id in canvas_course_ids:
            data[canvas_course_id] = self.get_course_students_as_df(course_id=canvas_course_id)
            expected_records += len(data[canvas_course_id].index)
        if import_avatar_urls:
            # if importing avatars, make sure column exists
            existing_avatar_columns = find_column_by_name(
                term='CANVAS_AVATAR',
                table_uuid=table_uuid,
                return_loaded_instances=True,
                term_is_system_name='either'
            )
            if existing_avatar_columns:
                # column exists
                existing_avatar_column = existing_avatar_columns[0]
            else:
                # create new
                existing_avatar_column = Column()
                if existing_avatar_column.create(table_uuid=table_uuid, override_username=self.override_username):
                    existing_avatar_column.config['name'] = 'CANVAS_AVATAR'
                    existing_avatar_column.config['system_name'] = 'CANVAS_AVATAR'
                    existing_avatar_column.config['type'] = 'imgurl'
                    existing_avatar_column.update(override_username=self.override_username)
        if import_pronouns:
            # if importing pronouns, make sure column exists
            existing_pronouns_columns = find_column_by_name(
                term='CANVAS_PRONOUNS',
                table_uuid=table_uuid,
                return_loaded_instances=True,
                term_is_system_name='either'
            )
            if existing_pronouns_columns:
                # column exists
                existing_pronouns_column = existing_pronouns_columns[0]
            else:
                # create new
                existing_pronouns_column = Column()
                if existing_pronouns_column.create(table_uuid=table_uuid, override_username=self.override_username):
                    existing_pronouns_column.config['name'] = 'CANVAS_PRONOUNS'
                    existing_pronouns_column.config['system_name'] = 'CANVAS_PRONOUNS'
                    existing_pronouns_column.config['type'] = 'mark'
                    existing_pronouns_column.update(override_username=self.override_username)
        # iterate to import student records
        if expected_records > 0:
            # make the combined df
            df_students = pd.concat(data).drop_duplicates(subset=['canvas_id'])
            # mock mapping
            mapping = {
                'preferred_name': {'field': 'preferred_name'},
                'given_names': {'field': 'given_names'},
                'surname': {'field': 'family_name'},
                'sid': {'field': 'sid'},
                'email': {'field': 'email'},
                'username': {'field': 'login_id'},
                'alternative_id1': {'field': 'canvas_id'}
            }
            # run the enrolment update!
            table = Table()
            if table.load(table_uuid):
                result = table._update_enrollments(
                    df=df_students.to_dict('records'),
                    mapping=mapping, 
                    remove_not_present=inactivate_students_not_in_import, 
                    overwrite_details=not no_overwrite
                )
                ret['messages'].append(("{} records saved.".format(result['now_active']), "success"))
                table.store_enrolment_update_status(
                    updated_by=self.override_username, 
                    update_success=True, 
                    update_source='Canvas', 
                    updated_active_student_count=result['now_active']
                )
                ret['success'] = True
        else:
            ret['messages'].append(("No records found.", "warning"))
        # import avatars if necessary
        if expected_records and import_avatar_urls and not no_overwrite:
            student_data = StudentData(table_uuid)
            for canvas_id, student in df_students.iterrows():
                student_data._reset()
                if student_data.find_student({'sid': student['sid'], 'email': student['email']}):
                    student_data.set_data(
                        column_uuid=existing_avatar_column.config['uuid'], 
                        data=student['avatar_url'], 
                        auth_user_override=self.override_username,
                        preloaded_column=existing_avatar_column,
                        commit_immediately=True,
                        ignore_active=True
                    )
            existing_avatar_column.config['description'] = "Last updated {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            existing_avatar_column.config['system_name'] = 'CANVAS_AVATAR'
            existing_avatar_column.update(override_username=self.override_username)
        # import pronouns if necessary
        if expected_records and import_pronouns and not no_overwrite:
            student_data = StudentData(table_uuid)
            for canvas_id, student in df_students.iterrows():
                student_data._reset()
                if student_data.find_student({'sid': student['sid'], 'email': student['email']}):
                    student_data.set_data(
                        column_uuid=existing_pronouns_column.config['uuid'], 
                        data=student.get('pronouns', ''), 
                        auth_user_override=self.override_username,
                        preloaded_column=existing_pronouns_column,
                        commit_immediately=True,
                        ignore_active=True
                    )
            existing_pronouns_column.config['description'] = "Last updated {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            existing_pronouns_column.config['system_name'] = 'CANVAS_PRONOUNS'
            existing_pronouns_column.update(override_username=self.override_username)
        return ret
    
    def import_teacher_enrollments(self, table_uuid, canvas_course_ids):
        ret = {
            'success': False,
            'messages': []
        }
        connection_details = self.get_connection(table_uuid, 'teacher_enrollment')
        overwrite = connection_details.get('overwrite', 'no')
        type_mapping = {
            'TeacherEnrollment': connection_details.get('type_mapping_TeacherEnrollment', 'administrator'),
            'TaEnrollment': connection_details.get('type_mapping_TaEnrollment', 'user'),
            'DesignerEnrollment': connection_details.get('type_mapping_DesignerEnrollment', 'no'),
            'ObserverEnrollment': connection_details.get('type_mapping_ObserverEnrollment', 'auditor')
        }
        # fetch users from canvas
        data = {}
        for canvas_course_id in canvas_course_ids:
            enrollments = self._get_course_enrollments(canvas_course_id, enrollment_types=list(type_mapping.keys()))['enrollments']
            for enrollment in enrollments:
                enrollment_type = enrollment.get('type')
                canvas_user_id = enrollment.get('user_id')
                login_id = enrollment.get('user', {}).get('login_id')
                if enrollment_type is None or canvas_user_id is None or login_id is None:
                    continue
                if enrollment_type in type_mapping.keys() and type_mapping[enrollment_type] != 'no':
                    data[canvas_user_id] = {
                        'sres_list_role': type_mapping[enrollment_type],
                        'login_id': login_id
                    }
        # update the list config
        table = Table()
        if table.load(table_uuid):
            # gather neatly
            for user_role in USER_ROLES:
                role_name = user_role['name']
                usernames_for_role = []
                for canvas_user_id, user in data.items():
                    if user['sres_list_role'] == role_name:
                        usernames_for_role.append(user['login_id'])
                user_oids_for_role = usernames_to_oids(usernames=usernames_for_role, add_if_not_exists=True)
                # add to table config
                if overwrite == 'no':
                    table.config['staff'][f'{role_name}s'] = list(set(table.config['staff'][f'{role_name}s'] + user_oids_for_role))
                elif overwrite == 'yes':
                    table.config['staff'][f'{role_name}s'] = list(set(user_oids_for_role))
            # update table
            if table.update(override_username=self.override_username):
                ret['success'] = True
            else:
                ret['messages'].append('Could not update list configuration')
        return ret
    
    def get_user_enrollments_friendly_as_df(self, identifiers=[]):
        STATES = ['active', 'inactive', 'completed']
        ret = {
            'success': False,
            'df': None,
            'mapping': {}
        }
        identifier_type = self.config.get('id_map', {}).get('sid')
        if identifier_type:
            _data = {}
            all_course_ids = []
            # get data
            for identifier in identifiers:
                enrollment_data = self._get_user_enrollments(identifier, identifier_type)
                canvas_user_id = enrollment_data.get('canvas_user_id')
                _data[canvas_user_id] = enrollment_data
                all_course_ids.extend(enrollment_data['all_course_ids'])
            # translate course_ids to course codes
            all_course_ids = list(set(all_course_ids))
            course_code_mapping = {}
            for course_id in all_course_ids:
                result = self._send_request(
                    url=r'{api_url}courses/{course_id}'.format(
                        api_url=self.config['api_url'],
                        course_id=course_id
                    ),
                    method='GET',
                    use_admin_token=True
                )
                course_code = result['data'].get('course_code')
                if course_code:
                    course_code_mapping[course_id] = course_code
            # merge course code with course id
            data = {}
            header = 'CANVAS_COURSE_ENROLLMENTS'
            for canvas_user_id, record in _data.items():
                row = {
                    'sid': record['identifier'],
                    'canvas_id': canvas_user_id
                }
                cell = []
                for state in STATES:
                    cell.append( [ course_code_mapping.get(c, '??') for c in record['enrollments'][state] ])
                row[header] = json.dumps(cell)
                data[canvas_user_id] = deepcopy(row)
            # build mapping
            mapping = {}
            mapping[header] = {
                'multi_entry_options': []
            }
            for state in STATES:
                mapping[header]['multi_entry_options'].append({
                    'label': state,
                    'type': 'regex',
                    'regex': '.*'
                })
            ret['df'] = pd.DataFrame.from_dict(data, dtype='str', orient='index')
            ret['mapping'] = mapping
            ret['success'] = True
        else:
            logging.error('Could not determine identifier type')
        return ret
        
    def _get_user_enrollments(self, identifier, identifier_type=None):
        ret = {
            'canvas_user_id': '',
            'identifier': identifier,
            'enrollments': {
                'active': [],
                'inactive': [],
                'completed': []
            },
            'all_course_ids': []
        }
        if identifier_type not in ['sis_integration_id', 'sis_login_id', 'sis_user_id', 'id', None]:
            return ret
        # get user enrolments
        if identifier_type in ['sis_integration_id', 'sis_login_id', 'sis_user_id']:
            identifier_string = '{}:{}'.format(identifier_type, identifier)
        else:
            identifier_string = identifier
        result = self._send_request(
            url=r'{api_url}users/{identifier_string}/enrollments?per_page=25&type[]=StudentEnrollment&state[]=active&state[]=inactive&state[]=completed'.format(
                api_url=self.config['api_url'],
                identifier_string=identifier_string
            ),
            method='GET',
            use_admin_token=True
        )
        # parse enrollments
        canvas_user_id = None
        if result is not None and result.get('data'):
            for enrollment in result['data']:
                state = enrollment.get('enrollment_state')
                if enrollment.get('user_id') is not None:
                    canvas_user_id = enrollment.get('user_id')
                    ret['canvas_user_id'] = canvas_user_id
                if state in ret['enrollments'].keys():
                    course_id = enrollment.get('course_id')
                    ret['all_course_ids'].append(course_id)
                    if course_id and course_id not in ret['enrollments'][state]:
                        ret['enrollments'][state].append(course_id)
        else:
            logging.warning('_get_user_enrollments data is None for {}'.format(identifier))
            print('_get_user_enrollments data is None for {}'.format(identifier))
        return ret
    
    def get_course_students_as_df(self, course_id=None, course_ids=None):
        """
            Returns a df, with rows {
                canvas_id # WARNING: Canvas user ids are type int but this method turns them into str.
                sid
                email
                given_names
                family_name
                avatar_url
            }
            
            [get_course_students_as_df replaces get_course_students_as_query]
        """
        students = {}
        if course_id:
            if isinstance(course_id, list):
                course_ids = course_id
            else:
                course_ids = [course_id]
            del course_id
        if not course_ids:
            return None
        for course_id in course_ids:
            # get course enrollments
            course_enrollments = self._get_course_enrollments(course_id)
            parsed_enrollments = {}
            if course_enrollments['enrollments'] is None:
                continue
            for course_enrollment in course_enrollments['enrollments']:
                parsed_enrollments[course_enrollment['user_id']] = {
                    'canvas_id': course_enrollment['user_id'],
                    'name': course_enrollment['user']['name'],
                    'pronouns': course_enrollment['user'].get('pronouns', ''),
                    'sis_user_id': course_enrollment['user']['sis_user_id'] if 'sis_user_id' in course_enrollment['user'].keys() else ''
                }
            # get course users
            course_users = self._get_course_users(course_id=course_id, enrollment_type='student')
            for course_user in course_users['users']:
                if not course_user['id'] in students.keys():
                    students[course_user['id']] = {
                        'canvas_id': course_user['id'],
                        'sid': course_user['sis_user_id'] if 'sis_user_id' in course_user.keys() else parsed_enrollments[course_user['id']]['sis_user_id'] if (course_user['id'] in parsed_enrollments.keys()) else '',
                        'email': course_user['email'] if 'email' in course_user.keys() else '',
                        'given_names': course_user['sortable_name'].split(',')[-1].strip(),
                        'family_name': course_user['sortable_name'].split(',')[0].strip(),
                        'avatar_url': course_user['avatar_url'] if 'avatar_url' in course_user.keys() else '',
                        'pronouns': parsed_enrollments[course_user['id']].get('pronouns', ''),
                        'login_id': course_user['login_id'] if 'login_id' in course_user.keys() else ''
                    }
                    students[course_user['id']]['preferred_name'] = course_user['short_name'].split(' ')[0].strip() if ('short_name' in course_user.keys() and course_user['short_name']) else students[course_user['id']]['given_names'].split(' ')[0].strip()
        return pd.DataFrame.from_dict(data=students, dtype='str', orient='index')
    
    def _get_course_enrollments(self, course_id, enrollment_type='StudentEnrollment', enrollment_types=None):
        ret = {
            'status_code': '',
            'enrollments': []
        }
        if not enrollment_type and enrollment_types is None:
            enrollment_type_argument = ''
        elif enrollment_types is not None and type(enrollment_types) is list:
            enrollment_type_argument = '&type[]=' + '&type[]='.join(enrollment_types)
        elif enrollment_type:
            enrollment_type_argument = f'&type[]={enrollment_type}'
        else:
            enrollment_type_argument = ''
            
        result = self._send_request(
            url=r'{api_url}courses/{course_id}/enrollments?{enrollment_type_argument}&per_page=200&state[]=active&state[]=invited&state[]=completed'.format(
                api_url=self.config['api_url'],
                course_id=course_id,
                enrollment_type_argument=enrollment_type_argument
            ),
            method='GET'
        )
        ret['enrollments'] = result['data']
        return ret
    
    def _get_course_users(self, course_id, enrollment_type=''):
        ret = {
            'status_code': '',
            'users': []
        }
        enrollment_type_argument = '' if not enrollment_type else '&enrollment_type={}'.format(enrollment_type)
        result = self._send_request(
            url=r'{api_url}courses/{course_id}/users?include[]=email&include[]=avatar_url{enrollment_type_argument}&per_page=200&enrollment_state[]=active&enrollment_state[]=invited'.format(
                api_url=self.config['api_url'],
                course_id=course_id,
                enrollment_type_argument=enrollment_type_argument
            ),
            method='GET'
        )
        ret['users'] = result['data']
        return ret
    
    def _get_course_students(self, course_id):
        ret = {
            'status_code': '',
            'users': [],
            'user_ids': []
        }
        result = self._send_request(
            url=r'{api_url}courses/{course_id}/users?enrollment_type[]=student'.format(
                api_url=self.config['api_url'],
                course_id=course_id
            ),
            method='GET'
        )
        if result and result['data']:
            ret['users'] = result['data']
            user_ids = []
            for user in result['data']:
                user_ids.append(user.get('id'))
            ret['user_ids'] = user_ids
        return ret
    
    def _get_course_instructors(self, course_id):
        ret = {
            'status_code': '',
            'users': [],
            'user_ids': []
        }
        result = self._send_request(
            url=r'{api_url}courses/{course_id}/users?enrollment_type[]=teacher&enrollment_type[]=ta'.format(
                api_url=self.config['api_url'],
                course_id=course_id
            ),
            method='GET'
        )
        if result and result['data']:
            ret['users'] = result['data']
            user_ids = []
            for user in result['data']:
                user_ids.append(user.get('id'))
            ret['user_ids'] = user_ids
        return ret
    
    def _get_course_user(self, course_id, user_id):
        ret = {
            'status_code': '',
            'user': {}
        }
        result = self._send_request(
            url=r'{api_url}courses/{course_id}/users/{user_id}?include[]=email&include[]=avatar_url'.format(
                api_url=self.config['api_url'],
                course_id=course_id,
                user_id=user_id
            ),
            method='GET'
        )
        ret['user'] = result['data']
        return ret
    
    def are_all_courses_concluded(self, course_ids):
        """Checks to see if the concluded flag has been set. Returns True only if all courses specified by course_ids are concluded."""
        for course_id in course_ids:
            if not self.is_course_concluded(course_id):
                return False
        return True
    
    def is_course_concluded(self, course_id):
        course = self._get_course(course_id)
        if course['course']:
            return course['course'].get('concluded', False)
        else:
            # Typically this would be because the requesting user has lost access.
            # So, judge it as concluded as this will stop the syncs.
            return True
    
    def _get_course(self, course_id):
        ret = {
            'status_code': '',
            'course': {}
        }
        result = self._send_request(
            url=r'{api_url}courses/{course_id}?include[]=concluded'.format(
                api_url=self.config['api_url'],
                course_id=course_id
            ),
            method='GET'
        )
        ret['course'] = result['data']
        return ret
    
    def get_courses(self):
        ret = {
            'status_code': "",
            'courses': [], # this will include concluded courses
            'concluded_courses_ids': []
        }
        result = self._send_request(
            url=r'{api_url}courses?enrollment_type=teacher&per_page=50&include[]=concluded'.format(
                api_url=self.config['api_url']
            ),
            method='GET'
        )
        # parse
        if result['data']:
            for course in result['data']:
                ret['courses'].append({
                    'id': course['id'],
                    'course_code': course['course_code'],
                    'name': course['name'],
                    'start_at': course['start_at'],
                    'end_at': course['end_at'],
                    'concluded': course['concluded'],
                    'time_zone': course['time_zone'],
                    'enrollment_term_id': course['enrollment_term_id']
                })
        # get concluded courses
        for course in ret['courses']:
            if course['concluded']:
                ret['concluded_courses_ids'].append(course['id'])
        # sort
        ret['courses'] = natsorted(ret['courses'], key=lambda e: e['course_code'], alg=ns.IGNORECASE)
        # return
        return ret
    
    def _get_assignment(self, canvas_course_id, canvas_assignment_id):
        result = self._send_request(
            url=r'{api_url}courses/{canvas_course_id}/assignments/{assignment_id}'.format(
                api_url=self.config['api_url'],
                canvas_course_id=canvas_course_id,
                assignment_id=canvas_assignment_id
            ),
            method='GET'
        )
        return result['data']
    
    def _get_assignments_for_courses(self, canvas_course_ids, external_tool_tag_attributes_url_contains='', only_assignment_ids=[]):
        ret = {
            'status_code': "",
            'assignments': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                url=r'{api_url}courses/{canvas_course_id}/assignments?per_page=50'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                ),
                method='GET'
            )
            # parse
            if result and result['data']:
                for assignment in result['data']:
                    if only_assignment_ids and assignment['id'] not in only_assignment_ids:
                        # need to limit the assignments returned, and also the current assignment is not found in the limiting array
                        #print('LLL', assignment['id'], only_assignment_ids)
                        continue
                    if external_tool_tag_attributes_url_contains:
                        if 'external_tool_tag_attributes' in assignment.keys():
                            #print('xxx', assignment['external_tool_tag_attributes'])
                            if not external_tool_tag_attributes_url_contains.lower() in assignment['external_tool_tag_attributes']['url'].lower():
                                continue
                        else:
                            continue
                    ret['assignments'][assignment['id']] = {
                        'id': assignment['id'],
                        'name': assignment['name'],
                        'safe_name': re.sub("[^A-Z0-9a-z_]", "_", assignment['name']),
                        'description': assignment['description'],
                        'points_possible': assignment['points_possible'],
                        'due_at': assignment['due_at'],
                        'course_id': assignment['course_id'],
                        'html_url': assignment['html_url'],
                        'url': assignment['url'] if 'url' in assignment.keys() else '',
                        'muted': assignment['muted'],
                        'published': assignment['published']
                    }
        # sort
        ret['assignments'] = dict(natsorted(ret['assignments'].items(), key=lambda kv: kv[1]['name'], alg=ns.IGNORECASE))
        # return
        return ret
    
    def _get_quizzes_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': "",
            'quizzes': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                url=r'{api_url}courses/{canvas_course_id}/quizzes?per_page=50'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                ),
                method='GET'
            )
            # parse
            if result and result['data']:
                for quiz in result['data']:
                    ret['quizzes'][quiz['id']] = {
                        'id': quiz['id'],
                        'course_id': canvas_course_id,
                        'title': quiz['title'],
                        'safe_title': re.sub("[^A-Z0-9a-z_]", "_", quiz['title']),
                        'description': bleach.clean(quiz['description']) if quiz['description'] else '',
                        'points_possible': quiz['points_possible'],
                        'due_at': quiz['due_at'],
                        'question_count': quiz['question_count']
                    }
        # sort
        ret['quizzes'] = dict(natsorted(ret['quizzes'].items(), key=lambda kv: kv[1]['title'], alg=ns.IGNORECASE))
        # return
        return ret
    
    def get_module_completion_for_courses_as_df(self, canvas_course_ids):
        """
            
            [get_module_completion_for_courses_as_df replaces get_module_completion_for_courses_as_query]
        """
        ret = {
            'success': False,
            'df': None,
            'mapping': {}
        }
        # build module structures
        module_structure = {}
        module_structure_ordered_ids = {}
        for canvas_course_id in canvas_course_ids:
            column_header = 'CANVAS_MODULE_COMPLETION_{}'.format(canvas_course_id)
            module_structure[canvas_course_id] = {}
            module_structure_ordered_ids[canvas_course_id] = []
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/modules?include[]=items'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                )
            )
            if result['data'] is None:
                continue
            for module in result['data']:
                module_structure[canvas_course_id][module['id']] = {
                    'course_id': canvas_course_id,
                    'type': "module",
                    'id': module['id'],
                    'name': module['name'],
                    'safe_name': re.sub("[^A-Z0-9a-z_]", "", module['name']),
                    'position': module['position']
                }
                module_structure_ordered_ids[canvas_course_id].append(module['id'])
                if 'items' in module.keys() and isinstance(module['items'], list):
                    for module_item in module['items']:
                        if 'completion_requirement' in module_item.keys():
                            module_structure[canvas_course_id][module_item['id']] = {
                                'course_id': canvas_course_id,
                                'type': "item",
                                'id': module_item['id'],
                                'name': module_item['title'],
                                'safe_name': re.sub("[^A-Z0-9a-z_]", "", module_item['title']),
                                'completion_requirement_type': module_item['completion_requirement']['type'],
                                'position': module_item['position'],
                                'parent_module_position': module['position']
                            }
                            module_structure_ordered_ids[canvas_course_id].append(module_item['id'])
        # get completion statuses for all students
        module_element_completions = {}
        for canvas_course_id in canvas_course_ids:
            # get all students
            course_users = self._get_course_users(canvas_course_id, 'student')['users']
            if course_users is None:
                continue
            for course_user in course_users:
                # add user to local dataset if not already
                if not course_user['id'] in module_element_completions.keys():
                    module_element_completions[course_user['id']] = {
                        canvas_course_id: {}
                    }
                # then request module completion on a loop for each
                result = self._send_request(
                    method='GET', 
                    url=r'{api_url}courses/{canvas_course_id}/modules?student_id={user_id}&include[]=items'.format(
                        api_url=self.config['api_url'],
                        canvas_course_id=canvas_course_id,
                        user_id=course_user['id']
                    )
                )
                if result['data'] is None:
                    continue
                for module in result['data']:
                    module_element_completions[course_user['id']][canvas_course_id][module['id']] = module['state']
                    if 'items' in module.keys() and isinstance(module['items'], list):
                        for module_item in module['items']:
                            if 'completion_requirement' in module_item.keys():
                                module_element_completions[course_user['id']][canvas_course_id][module_item['id']] = 1 if module_item['completion_requirement']['completed'] else 0
        # format for output
        students = self.get_course_students_as_df(course_ids=canvas_course_ids)
        data = {}
        for index, df_row in students.iterrows():
            row = {
                'email': df_row['email'],
                'sid': df_row['sid']
            }
            canvas_user_id = int(df_row['canvas_id'])
            if canvas_user_id in module_element_completions.keys():
                for canvas_course_id in canvas_course_ids:
                    cell = []
                    for module_element_id in module_structure_ordered_ids[canvas_course_id]:
                        try:
                            cell.append(
                                module_element_completions[canvas_user_id][canvas_course_id][module_element_id]
                            )
                        except:
                            cell.append('?')
                    row['CANVAS_MODULE_COMPLETION_{}'.format(canvas_course_id)] = json.dumps(cell)
            data[df_row['canvas_id']] = deepcopy(row)
            del row
        # set up mapping
        mapping = {}
        for canvas_course_id in canvas_course_ids:
            current_column_header = 'CANVAS_MODULE_COMPLETION_{}'.format(canvas_course_id)
            mapping[current_column_header] = {
                'multi_entry_options': []
            }
            for module_element_id in module_structure_ordered_ids[canvas_course_id]:
                module_element = module_structure[canvas_course_id][module_element_id]
                mapping[current_column_header]['multi_entry_options'].append({
                    'label': ("Module {element_name}" if module_element['type'] == 'module' else "Module item {completion_requirement_type} {element_name}").format(
                        element_name=module_element['name'],
                        completion_requirement_type=module_element['completion_requirement_type'] if 'completion_requirement_type' in module_element.keys() else ''
                    ),
                    'type': 'regex',
                    'regex': '.*'
                })
        # return
        ret['mapping'] = mapping
        ret['success'] = True
        ret['df'] = pd.DataFrame.from_dict(data, dtype='str', orient='index')
        return ret
    
    def get_gradebook_custom_columns_friendly_as_df(self, canvas_course_ids):
        
        custom_columns_metadata = self._get_gradebook_custom_columns_for_courses(canvas_course_ids)['gradebook_custom_columns']
        custom_columns_data = self._get_gradebook_custom_columns_data_for_courses(canvas_course_ids)['gradebook_custom_columns_data']
        students = self.get_course_students_as_df(canvas_course_ids)
        
        data_by_user = {}
        for canvas_course_id in canvas_course_ids:
            if canvas_course_id in custom_columns_data.keys() and custom_columns_data[canvas_course_id]:
                for custom_column_id, custom_column in custom_columns_data[canvas_course_id].items():
                    if isinstance(custom_column, list):
                        for custom_column_data_element in custom_column:
                            if not custom_column_data_element['user_id'] in data_by_user.keys():
                                data_by_user[custom_column_data_element['user_id']] = {}
                            data_by_user[custom_column_data_element['user_id']][custom_column_id] = {
                                'content': custom_column_data_element['content']
                            }
        
        data = {}
        for index, row in students.iterrows():
            canvas_user_id = int(row['canvas_id'])
            data[canvas_user_id] = {
                'email': row['email'],
                'sid': row['sid']
            }
            for custom_column_id in custom_columns_metadata:
                header = "CANVAS_{custom_column_id}_CUSTOM_{safe_title}".format(
                    custom_column_id=custom_column_id,
                    safe_title=custom_columns_metadata[custom_column_id]['safe_title']
                )
                if canvas_user_id in data_by_user.keys() and custom_column_id in data_by_user[canvas_user_id].keys():
                    data[canvas_user_id][header] = data_by_user[canvas_user_id][custom_column_id]['content']
                else:
                    data[canvas_user_id][header] = ''
        
        return pd.DataFrame.from_dict(data, dtype='str', orient='index')
    
    def _get_gradebook_custom_columns_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': "",
            'gradebook_custom_columns': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/custom_gradebook_columns'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id,
                )
            )
            if result['data'] is None:
                continue
            for gradebook_custom_column in result['data']:
                ret['gradebook_custom_columns'][gradebook_custom_column['id']] = {
                    'id': gradebook_custom_column['id'],
                    'course_id': canvas_course_id,
                    'title': gradebook_custom_column['title'],
                    'safe_title': re.sub("[^A-Z0-9a-z_]", "_", gradebook_custom_column['title']),
                    'hidden': gradebook_custom_column['hidden']
                }
        return ret
    
    def _get_rubrics_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': '',
            'rubrics': {},
            'flattened_criteria': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/rubrics'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                )
            )
            ret['status_code'] = result['status_code']
            if result['data'] is None:
                continue
            for rubric in result['data']:
                rubric_record = {
                    'course_id': canvas_course_id,
                    'rubric_id': rubric.get('id'),
                    'criteria': []
                }
                for criterion in rubric.get('data', []):
                    criterion_record = {
                        'description': criterion.get('description', ''),
                        'long_description': criterion.get('long_description', ''),
                        'total_points': criterion.get('points'),
                        'criterion_id': criterion.get('_id'),
                        'ratings': []
                    }
                    for rating in criterion.get('ratings', []):
                        criterion_record['ratings'].append({
                            'display': rating.get('description', ''),
                            'value': rating.get('points'),
                            'description': rating.get('long_description', '')
                        })
                    # add the zero rating
                    if criterion_record['ratings']:
                        criterion_record['ratings'].append({
                            'display': '0',
                            'value': 0,
                            'description': ''
                        })
                    # add to ret flattened_criteria
                    if criterion.get('_id') not in ret['flattened_criteria'].keys():
                        ret['flattened_criteria'][criterion.get('_id')] = criterion_record
                    # add to rubric_record
                    rubric_record['criteria'].append(criterion_record)
                # append to ret rubrics
                ret['rubrics'][rubric.get('id')] = rubric_record
        return ret
    
    def _get_gradebook_custom_columns_data_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': "",
            'gradebook_custom_columns_data': {}
        }
        gradebook_custom_columns = self._get_gradebook_custom_columns_for_courses(canvas_course_ids)['gradebook_custom_columns']
        ret['gradebook_custom_columns_data'] = {}
        for id, gradebook_custom_column in gradebook_custom_columns.items():
            current_course_id = gradebook_custom_column['course_id']
            current_custom_column_id = gradebook_custom_column['id']
            ret['gradebook_custom_columns_data'][current_course_id] = {
                current_custom_column_id: []
            }
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{current_course_id}/custom_gradebook_columns/{current_custom_column_id}/data'.format(
                    api_url=self.config['api_url'],
                    current_course_id=current_course_id,
                    current_custom_column_id=current_custom_column_id
                )
            )
            ret['gradebook_custom_columns_data'][current_course_id][current_custom_column_id] = result['data']
        return ret
    
    def _get_submissions_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': '',
            'submissions': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/students/submissions?student_ids[]=all&grouped=true'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id,
                )
            )
            if result['data'] is None:
                continue
            for user in result['data']:
                if not user['user_id'] in ret['submissions'].keys():
                    ret['submissions'][user['user_id']] = {}
                for submission in user['submissions']:
                    ret['submissions'][user['user_id']][submission['assignment_id']] = {}
                    for submission_attribute in 'id,score,grade,submitted_at,late,seconds_late,workflow_state'.split(','):
                        if submission_attribute in submission.keys():
                            ret['submissions'][user['user_id']][submission['assignment_id']][submission_attribute] = submission[submission_attribute]
        return ret
    
    def _get_rubric_peer_assessments(self, canvas_course_id, canvas_assignment_id, criterion_aggregation_method='mean', overall_aggregation_method='mean'):
        
        ret = {
            'status_code': 0,
            'assessments_by_assessee': {},
            'rubric_criteria': {}
        }
        
        # get the rubric id
        rubric_id = ''
        rubric_posts_possible = 0
        result = self._send_request(
            method='GET', 
            url=r'{api_url}courses/{canvas_course_id}/assignments/{canvas_assignment_id}'.format(
                api_url=self.config['api_url'],
                canvas_course_id=canvas_course_id,
                canvas_assignment_id=canvas_assignment_id
            )
        )
        try:
            rubric_id = result['data']['rubric_settings']['id']
            rubric_posts_possible = result['data']['rubric_settings']['points_possible']
        except:
            return ret
        
        # get the submissions for the assignment
        submissions_by_submission_id = {}
        result = self._send_request(
            method='GET', 
            url=r'{api_url}courses/{canvas_course_id}/assignments/{canvas_assignment_id}/submissions'.format(
                api_url=self.config['api_url'],
                canvas_course_id=canvas_course_id,
                canvas_assignment_id=canvas_assignment_id
            )
        )
        if result['data']:
            for submission in result['data']:
                submissions_by_submission_id[submission['id']] = {
                    'user_id': submission['user_id'],
                    'workflow_state': submission['workflow_state']
                }
        
        # get the rubric peer reviews
        result = self._send_request(
            method='GET', 
            url=r'{api_url}courses/{canvas_course_id}/rubrics/{rubric_id}?style=full&include=peer_assessments'.format(
                api_url=self.config['api_url'],
                canvas_course_id=canvas_course_id,
                rubric_id=rubric_id
            )
        )
        rubric_criteria = {}
        rubric_criteria_for_loading = {}
        if result['data']:
            for rubric_criterion in result['data']['criteria']:
                if not 'long_description' in rubric_criterion.keys():
                    rubric_criterion['long_description'] = ''
                rubric_criteria[rubric_criterion['id']] = {
                    'description': rubric_criterion['description'],
                    'long_description': rubric_criterion['long_description'],
                    'points': rubric_criterion['points']
                }
                rubric_criteria_for_loading[rubric_criterion['id']] = []
        
        # grab the assessments
        assessments_by_assessee = {}
        if 'assessments' in result['data'].keys():
            for rubric_assessment in result['data']['assessments']:
                if rubric_assessment['artifact_id'] in submissions_by_submission_id.keys():
                    assessee_user_id = submissions_by_submission_id[rubric_assessment['artifact_id']]['user_id']
                    if not assessee_user_id in assessments_by_assessee.keys():
                        assessments_by_assessee[assessee_user_id] = {
                            'assessments_by_criterion': deepcopy(rubric_criteria_for_loading),
                            'scores': []
                        }
                    for rubric_assessment_data in rubric_assessment['data']:
                        if 'points' in rubric_assessment_data.keys():
                            assessments_by_assessee[assessee_user_id]['assessments_by_criterion'][rubric_assessment_data['criterion_id']].append(rubric_assessment_data['points'])
                    if 'score' in rubric_assessment.keys():
                        assessments_by_assessee[assessee_user_id]['scores'].append(rubric_assessment['score'])
        # calculate counts and aggregations
        for assessee_user_id in assessments_by_assessee:
            # calculate overall score
            if overall_aggregation_method == 'mean':
                assessments_by_assessee[assessee_user_id]['aggregated_score'] = utils.list_avg(assessments_by_assessee[assessee_user_id]['scores'])
            elif overall_aggregation_method == 'median':
                assessments_by_assessee[assessee_user_id]['aggregated_score'] = utils.list_median(assessments_by_assessee[assessee_user_id]['scores'])
            elif overall_aggregation_method == 'sum':
                assessments_by_assessee[assessee_user_id]['aggregated_score'] = utils.list_sum(assessments_by_assessee[assessee_user_id]['scores'])
            # number of peer assessments
            assessments_by_assessee[assessee_user_id]['count_peer_assessments'] = len(assessments_by_assessee[assessee_user_id]['scores'])
            # calculate criterion scores
            assessments_by_assessee[assessee_user_id]['aggregated_points_by_criterion_for_loading'] = []
            for rubric_criterion_id in rubric_criteria:
                if criterion_aggregation_method == 'mean':
                    criterion_average = utils.list_avg(assessments_by_assessee[assessee_user_id]['assessments_by_criterion'][rubric_criterion_id])
                elif criterion_aggregation_method == 'median':
                    criterion_average = utils.list_median(assessments_by_assessee[assessee_user_id]['assessments_by_criterion'][rubric_criterion_id])
                elif criterion_aggregation_method == 'sum':
                    criterion_average = utils.list_sum(assessments_by_assessee[assessee_user_id]['assessments_by_criterion'][rubric_criterion_id])
                if not 'aggregated_points_by_criterion' in assessments_by_assessee[assessee_user_id].keys():
                    assessments_by_assessee[assessee_user_id]['aggregated_points_by_criterion'] = {}
                assessments_by_assessee[assessee_user_id]['aggregated_points_by_criterion'][rubric_criterion_id] = criterion_average
                assessments_by_assessee[assessee_user_id]['aggregated_points_by_criterion_for_loading'].append(criterion_average)
        ret['assessments_by_assessee'] = assessments_by_assessee
        ret['rubric_criteria'] = rubric_criteria
        return ret
    
    def _get_peer_reviews_for_assignment(self, canvas_course_id, canvas_assignment_id, include_comments=False, include_user=False):
        ret = {
            'status_code': "",
            'peer_reviews_by_assessor': {},
            'peer_reviews_by_assessee': {}
        }
        url_params = ['per_page=25']
        if include_comments: url_params.append('include[]=submission_comments')
        if include_user: url_params.append('include[]=user')
        result = self._send_request(
            method='GET', 
            url=r'{api_url}courses/{canvas_course_id}/assignments/{canvas_assignment_id}/peer_reviews?{url_params}'.format(
                api_url=self.config['api_url'],
                canvas_course_id=canvas_course_id,
                canvas_assignment_id=canvas_assignment_id,
                url_params='&'.join(url_params)
            )
        )
        if result['data']:
            peer_assessor_user_ids = set()
            for peer_review in result['data']:
                peer_assessor_user_ids.add(peer_review['assessor_id'])
                # parse for reviewer/assessor
                if not peer_review['assessor_id'] in ret['peer_reviews_by_assessor'].keys():
                    ret['peer_reviews_by_assessor'][peer_review['assessor_id']] = {
                        'assigned': 0,
                        'completed': 0
                    }
                ret['peer_reviews_by_assessor'][peer_review['assessor_id']][peer_review['workflow_state']] += 1
                # parse for reviewed/assessee
                if not peer_review['user_id'] in ret['peer_reviews_by_assessee']:
                    ret['peer_reviews_by_assessee'][peer_review['user_id']] = {
                        'assessors': [],
                        'assigned_assessors': [],
                        'completed_assessors': [],
                        'completed': 0,
                        'assigned': 0, # this means how many reviewers have been assigned and not yet completed
                        'peer_comments': [],
                        'peer_comments_ids': []
                    }
                ret['peer_reviews_by_assessee'][peer_review['user_id']][peer_review['workflow_state']] += 1
                ret['peer_reviews_by_assessee'][peer_review['user_id']]['assessors'].append(peer_review['assessor_id'])
                ret['peer_reviews_by_assessee'][peer_review['user_id']]['{}_assessors'.format(peer_review['workflow_state'])].append(peer_review['assessor_id'])
            if include_comments:
                for peer_review in result['data']:
                    for submission_comment in peer_review['submission_comments']:
                        if submission_comment['author_id'] in peer_assessor_user_ids and submission_comment.get('comment') and submission_comment.get('id') not in ret['peer_reviews_by_assessee'][peer_review['user_id']]['peer_comments_ids']:
                            ret['peer_reviews_by_assessee'][peer_review['user_id']]['peer_comments_ids'].append(submission_comment['id'])
                            ret['peer_reviews_by_assessee'][peer_review['user_id']]['peer_comments'].append(submission_comment['comment'])
        return ret
    
    def _get_submissions_for_assignment(self, canvas_course_id, canvas_assignment_id, include_comments=False, include_rubric=False, include_user=False, include_group=False, grouped=False, include_attachments=False):
        ret = {
            'status_code': "",
            'submissions': {},
            'rubric': {}
        }
        url_params = []
        if include_comments or include_rubric:
            url_params.append('per_page=25')
        else:
            url_params.append('per_page=100')
        if include_comments: url_params.append('include[]=submission_comments')
        if include_rubric: url_params.append('include[]=rubric_assessment')
        if include_user: url_params.append('include[]=user')
        if include_group: url_params.append('include[]=group')
        if grouped: url_params.append('grouped=true')
        result = self._send_request(
            method='GET', 
            url=r'{api_url}courses/{canvas_course_id}/assignments/{canvas_assignment_id}/submissions?{url_params}'.format(
                api_url=self.config['api_url'],
                canvas_course_id=canvas_course_id,
                canvas_assignment_id=canvas_assignment_id,
                url_params='&'.join(url_params)
            )
        )
        if result['data']:
            # prepare a few things
            if include_rubric:
                rubric = self._get_assignment(canvas_course_id, canvas_assignment_id)['rubric']
                ret['rubric'] = rubric
            if include_comments:
                student_user_ids = self._get_course_students(canvas_course_id)['user_ids']
            # iterate through data
            for user_submission in result['data']:
                ret['submissions'][user_submission['user_id']] = {}
                for submission_attribute in 'id,score,grade,submitted_at,late,seconds_late,workflow_state'.split(','):
                    try:
                        ret['submissions'][user_submission['user_id']][submission_attribute] = user_submission[submission_attribute]
                        #print(user_submission['user_id'], submission_attribute, user_submission[submission_attribute])
                    except:
                        ret['submissions'][user_submission['user_id']][submission_attribute] = ''
                if include_comments:
                    if 'grader_id' in user_submission.keys() and user_submission['grader_id']:
                        ret['submissions'][user_submission['user_id']]['submission_comments'] = []
                        grader_id = user_submission['grader_id']
                        submission_comments = user_submission['submission_comments']
                        if isinstance(submission_comments, list):
                            for submission_comment in submission_comments:
                                if submission_comment.get('author_id') not in student_user_ids:
                                    ret['submissions'][user_submission['user_id']]['submission_comments'].append({
                                        'id': submission_comment['id'],
                                        'author_id': submission_comment['author_id'],
                                        'author_name': submission_comment['author_name'],
                                        'comment': submission_comment['comment']
                                    })
                if include_rubric:
                    rubric_outcome = ''
                    rubric_comments = ''
                    if 'rubric_assessment' in user_submission.keys():
                        rubric_outcome = []
                        rubric_comments = []
                        #logging.debug('rubric ' + str(rubric))
                        for criterion in rubric:
                            rubric_outcome.append(user_submission.get('rubric_assessment', {}).get(criterion.get('id'), {}).get('points', ''))
                            rubric_comments.append(user_submission.get('rubric_assessment', {}).get(criterion.get('id'), {}).get('comments', ''))
                    ret['submissions'][user_submission['user_id']]['rubric_outcomes'] = {
                        'data': rubric_outcome,
                        'comments': rubric_comments
                    }
                if include_attachments:
                    attachments = []
                    if 'attachments' in user_submission.keys():
                        for attachment in user_submission.get('attachments', []):
                            attachments.append({
                                'original_filename': attachment.get('display_name', 'file'),
                                'saved_filename': attachment.get('filename', 'file'),
                                'content_type': attachment.get('content-type', 'application/octet-stream'),
                                'url': attachment.get('url', '')
                            })
                    ret['submissions'][user_submission['user_id']]['attachments'] = attachments
        return ret
    
    def get_analytics_student_summaries_as_df(self, canvas_course_ids):
        """
        
            [get_analytics_student_summaries_as_df replaces get_analytics_student_summaries_as_query]
        """
        student_summaries = self._get_analytics_student_summaries(canvas_course_ids)
        df_students = self.get_course_students_as_df(canvas_course_ids)
        df_student_summaries = pd.DataFrame.from_dict(student_summaries, dtype='str', orient='index')
        # rename and keep only some headers
        df_student_summaries = df_student_summaries[['page_views', 'participations']].rename(
            index=int, 
            columns={
                'page_views': 'CANVAS_ANALYTICS_PAGE_VIEWS', 
                'participations': 'CANVAS_ANALYTICS_PARTICIPATIONS'
            }
        )
        # merge
        df_data = df_students.merge(df_student_summaries, how='left', left_index=True, right_index=True)
        return df_data
    
    def _get_analytics_student_summaries(self, canvas_course_ids):
        student_summaries = {}
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/analytics/student_summaries?per_page=200'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id,
                )
            )
            if result['data'] is None:
                continue
            for student in result['data']:
                if not student['id'] in student_summaries.keys():
                    student_summaries[student['id']] = {
                        'page_views': student['page_views'],
                        'page_views_level': student['page_views_level'],
                        'participations': student['participations'],
                        'participations_level': student['participations_level']
                    }
                else:
                    student_summaries[student['id']] = {
                        'page_views': student_summaries[student['id']]['page_views'] + student['page_views'],
                        'page_views_level': student_summaries[student['id']]['page_views_level'] + student['page_views_level'],
                        'participations': student_summaries[student['id']]['participations'] + student['participations'],
                        'participations_level': student_summaries[student['id']]['participations_level'] + student['participations_level']
                    }
        return student_summaries
    
    def get_gradebook_friendly_as_df(self, canvas_course_ids, only_assignment_ids=[]):
        """
        
            [get_gradebook_friendly_as_df replaces get_gradebook_friendly_as_query]
        """
        # get data
        assignments = self._get_assignments_for_courses(
            canvas_course_ids=canvas_course_ids,
            only_assignment_ids=only_assignment_ids
        )['assignments']
        gradebook = self._get_gradebook_friendly(canvas_course_ids, only_assignment_ids, preloaded_assignments=assignments)
        # parse
        data = {}
        for user_id in gradebook:
            data[user_id] = {
                'email': gradebook[user_id]['user']['email'],
                'sid': gradebook[user_id]['user']['sid']
            }
            for assignment_id in assignments:
                header = 'CANVAS{assignment_id}_{safe_name}'.format(
                    assignment_id=assignment_id,
                    safe_name=assignments[assignment_id]['safe_name']
                )
                try:
                    #print('XXX', user_id, assignment_id, header, gradebook[user_id]['assignments'].keys(), gradebook[user_id])
                    if header in gradebook[user_id]['assignments'].keys() and gradebook[user_id]['assignments'][header]:
                        if gradebook[user_id]['assignments'][header]['submitted_at']:
                            dt = parser.parse(gradebook[user_id]['assignments'][header]['submitted_at'])
                            dt = utils.utc_to_local(dt)
                        else:
                            dt = ''
                        score = gradebook[user_id]['assignments'][header]['score'] if 'score' in gradebook[user_id]['assignments'][header].keys() else ''
                        if score == 'null' or score is None:
                            score = ''
                        grade = gradebook[user_id]['assignments'][header]['grade'] if 'grade' in gradebook[user_id]['assignments'][header].keys() else ''
                        if grade == 'null' or grade is None:
                            grade = ''
                        data[user_id][header] = json.dumps([
                            score,
                            gradebook[user_id]['assignments'][header]['workflow_state'],
                            str(gradebook[user_id]['assignments'][header]['late']),
                            '' if not dt else dt.strftime('%Y-%m-%d %H:%M:%S'),
                            grade
                        ])
                except Exception as e:
                    logging.error("error in get_gradebook_friendly_as_df [{}] [{}] [{}]".format(
                        assignment_id,
                        header,
                        user_id
                    ))
                    logging.exception(e)
        return pd.DataFrame.from_dict(data, dtype='str', orient='index')
    
    def _get_gradebook_friendly(self, canvas_course_ids, only_assignment_ids=[], preloaded_assignments=None):
        if preloaded_assignments:
            assignments = preloaded_assignments
        else:
            assignments = self._get_assignments_for_courses(
                canvas_course_ids=canvas_course_ids,
                only_assignment_ids=only_assignment_ids
            )['assignments']
        df_students = self.get_course_students_as_df(course_ids=canvas_course_ids)
        ret = {}
        if df_students is None:
            logging.error("_get_gradebook_friendly error, df_students is None [{}]".format(str(canvas_course_ids)))
            return ret
        for index, row in df_students.iterrows():
            #print('row', row)
            student_canvas_id = int(row['canvas_id'])
            if not student_canvas_id in ret.keys():
                ret[student_canvas_id] = {}
            ret[student_canvas_id]['user'] = {
                'email': row['email'],
                'sid': row['sid']
            }
            ret[student_canvas_id]['assignments'] = {}
        for canvas_course_id in canvas_course_ids:
            #print('WWW', assignments)
            for assignment_id in assignments:
                #print('QQQ', type(canvas_course_id), assignment_id, type(assignments[assignment_id]['course_id']), canvas_course_id == assignments[assignment_id]['course_id'])
                if canvas_course_id == assignments[assignment_id]['course_id']:
                    #print('PPP')
                    # get submissions for this assignment keyed by user id
                    assignment_submissions = self._get_submissions_for_assignment(canvas_course_id, assignment_id)
                    #print('RRR', assignment_submissions)
                    # parse and save for each student
                    for student_canvas_id in ret:
                        try:
                            ret[student_canvas_id]['assignments']['CANVAS{assignment_id}_{safe_name}'.format(
                                assignment_id=assignment_id,
                                safe_name=assignments[assignment_id]['safe_name']
                            )] = assignment_submissions['submissions'][student_canvas_id] if student_canvas_id in assignment_submissions['submissions'].keys() else None
                        except:
                            raise
                            pass
        return ret
             
    def _get_recent_students(self, canvas_course_ids):
        students = {}
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/users?include[]=enrollments&include[]=email&enrollment_type=student&per_page=200'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id,
                )
            )
            if result['data'] is None:
                continue
            for user in result['data']:
                if not user['id'] in students.keys():
                    students[user['id']] = {
                        'login_id': user['login_id'] if 'login_id' in user.keys() else '',
                        'email': user['email'] if 'email' in user.keys() else '',
                        'sis_user_id': user['sis_user_id'] if 'sis_user_id' in user.keys() else '',
                        'last_activity_at': None,
                        'total_activity_time': 0
                    }
                    for user_enrollment in user['enrollments']:
                        if canvas_course_id == user_enrollment['course_id']:
                            students[user['id']]['last_activity_at'] = user_enrollment['last_activity_at']
                            students[user['id']]['total_activity_time'] = user_enrollment['total_activity_time']
                else:
                    for user_enrollment in user['enrollments']:
                        if canvas_course_id == user_enrollment['course_id']:
                            if students[user['id']]['last_activity_at'] is None:
                                students[user['id']]['last_activity_at'] = user_enrollment['last_activity_at']
                            else:
                                try:
                                    if parser.parse(students[user['id']]['last_activity_at']) < parser.parse(user_enrollment['last_activity_at']):
                                        students[user['id']]['last_activity_at'] = user_enrollment['last_activity_at']
                                except:
                                    pass
                            students[user['id']]['total_activity_time'] += user_enrollment['total_activity_time']
        return students
    
    def get_recent_students_as_df(self, canvas_course_ids):
        """
        
            [get_recent_students_as_df replaces get_recent_students_as_query]
        """
        students = self._get_recent_students(canvas_course_ids)
        data = {}
        for canvas_student_id, student in students.items():
            last_login_temp = student['last_activity_at']
            if last_login_temp != '' and last_login_temp is not None:
                dt = parser.parse(last_login_temp)
                dt = utils.utc_to_local(dt)
                data[canvas_student_id] = {
                    'sid': student['sis_user_id'],
                    'email': student['email'],
                    'CANVAS_RECENT_LOGIN': dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'CANVAS_TOTAL_ACTIVITY_TIME': int(float(student['total_activity_time'] / 60))
                }
        return pd.DataFrame.from_dict(data, dtype='str', orient='index')
        
    def _get_quiz_submissions_for_quizzes(self, canvas_course_ids, quiz_ids=[]):
        quizzes = self._get_quizzes_for_courses(canvas_course_ids)['quizzes']
        submissions_by_quiz = {}
        for quiz_id in quizzes:
            if len(quiz_ids) > 0 and not quiz_id in quiz_ids:
                continue
            if not quiz_id in submissions_by_quiz.keys():
                submissions_by_quiz[quiz_id] = {
                    'submissions_by_user': {},
                    'quiz_title': quizzes[quiz_id]['safe_title'],
                    'course_id': ""
                }
            course_id = quizzes[quiz_id]['course_id']
            submissions_by_quiz[quiz_id]['course_id'] = course_id
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/quizzes/{quiz_id}/submissions'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=course_id,
                    quiz_id=quiz_id
                ),
                unpack_key='quiz_submissions'
            )
            if result['data'] is None:
                continue
            for submission in result['data']:
                if submission['user_id'] not in submissions_by_quiz[quiz_id]['submissions_by_user']:
                    submissions_by_quiz[quiz_id]['submissions_by_user'][submission['user_id']] = {}
                try:
                    started_at = utils.utc_to_local(parser.parse(submission['started_at']))
                    started_at = started_at.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    started_at = ''
                submissions_by_quiz[quiz_id]['submissions_by_user'][submission['user_id']][submission['id']] = {
                    'submission_id': submission['id'],
                    'quiz_id': submission['quiz_id'],
                    'course_id': course_id,
                    'user_id': submission['user_id'],
                    'started_at': started_at,
                    'time_spent': submission['time_spent'],
                    'workflow_state': submission['workflow_state'],
                    'score': submission['score']
                }
        return submissions_by_quiz
    
    def _get_quiz_submission_questions(self, submission_id):
        ret = {
            'status_code': "",
            'questions': {}
        }
        result = self._send_request(
            method='GET', 
            url=r'{api_url}quiz_submissions/{submission_id}/questions'.format(
                api_url=self.config['api_url'],
                submission_id=submission_id
            ),
            unpack_key='quiz_submission_questions'
        )
        # first sort according to position
        quiz_submission_questions = sorted(result['data'], key=lambda e: e['position'])
        # then parse
        for question in quiz_submission_questions:
            ret['questions'][question['id']] = {
                'id': question['id'],
                'quiz_id': question['quiz_id'],
                'question_text': bleach.clean(question['question_text']),
                'question_type': question['question_type'],
                'correct': question['correct']
            }
        return ret
    
    def _do_import_quiz_submissions_question_text_for_submission_worker(self, q): #, table, column, canvas_user_id, course_id, user_quiz_submissions):
        """
            Method for threading. Support method for do_import_quiz_submissions_question_text
            
            Incoming queue item: {
                column_uuid (str)
                canvas_user_id (int)
                course_id (int)
                user_quiz_submissions (dict)
            }
        """
        while True:
            item = q.get()
            print('in worker, item:', item)
            column_uuid = item['column_uuid']
            canvas_user_id = item['canvas_user_id']
            course_id = item['course_id']
            user_quiz_submissions = item['user_quiz_submissions']
            
            for submission_id in user_quiz_submissions:
                # call API
                submission_questions = self._get_quiz_submission_questions(submission_id)['questions']
                questions_by_correct_status = {
                    'correct': [],
                    'incorrect': []
                }
                for submission_question_id, submission_question in submission_questions.items():
                    if submission_question['correct']:
                        questions_by_correct_status['correct'].append(submission_question['question_text'])
                    else:
                        questions_by_correct_status['incorrect'].append(submission_question['question_text'])
            user_info = self._get_course_user(course_id, canvas_user_id)['user']
            column = Column()
            column.load(column_uuid)
            student_data = StudentData(table=column.table)
            if student_data.find_student(identifiers={
                'email': user_info['email'],
                'sid': user_info['sis_user_id'],
                'username': user_info['login_id']
            }, match_all=False):
                student_data.set_data(
                    column_uuid=column.config['uuid'],
                    data=json.dumps([questions_by_correct_status['correct'], questions_by_correct_status['incorrect']]),
                    auth_user_override=self.override_username,
                    commit_immediately=True,
                    preloaded_column=column
                )
            print('worker done')
            q.task_done()
    
    def do_import_quiz_submissions_question_text(self, canvas_course_ids, target_table_uuid, quiz_ids=[]):
        """
        
            This method is a memory hog so needs direct-write instead of returning 
            for another function to write to db.
        """
        ret = {
            'success': False,
            'messages': []
        }
        
        submissions_by_quiz = self._get_quiz_submissions_for_quizzes(canvas_course_ids, quiz_ids)
        column = Column()
        # set up queue for threading
        q = Queue(maxsize=0)
        worker = Thread(
            target=self._do_import_quiz_submissions_question_text_for_submission_worker,
            args=(q, )
        )
        worker.setDaemon(True)
        worker.start()
        # iterate through quizzes and start workers
        for quiz_id in submissions_by_quiz:
            quiz_submissions = submissions_by_quiz[quiz_id]
            column_name = "CANVAS_{quiz_id}_{quiz_title}_SUBMISSIONS_QUESTION_TEXT".format(
                quiz_id=quiz_id,
                quiz_title=quiz_submissions['quiz_title']
            )
            target_column_uuids = find_column_by_name(
                term=column_name,
                table_uuid=target_table_uuid,
                term_is_system_name='either'
            )
            if target_column_uuids:
                # column exists
                column.load(target_column_uuids[0])
                target_column_uuid = target_column_uuids[0]
            else:
                # create new
                target_column_uuid = column.create(target_table_uuid, override_username=self.override_username)
                column.config['name'] = column_name
                column.config['system_name'] = column_name
                column.config['type'] = 'multiEntry'
                column.config['multi_entry']['options'] = [
                    {'label': "Stems of correctly answered Qs", 'type': 'regex', 'regex': '.*'},
                    {'label': "Stems of incorrectly answered Qs", 'type': 'regex', 'regex': '.*'}
                ]
                column.update(override_username=self.override_username)
            # iterate through quiz_submissions.submissions_by_user collection
            # for each submission_id, call API to fetch submission details
            for canvas_user_id in quiz_submissions['submissions_by_user']:
                # add to queue
                q.put({
                    'column_uuid': column.config['uuid'],
                    'canvas_user_id': canvas_user_id,
                    'course_id': quiz_submissions['course_id'],
                    'user_quiz_submissions': quiz_submissions['submissions_by_user'][canvas_user_id]
                })
        # wait for queue to finish
        q.join()
        # update
        column.config['description'] = "Last updated on {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        column.config['system_name'] = column_name
        column.update(override_username=self.override_username)
        
    def _get_sessionless_launch_url(self, request_url):
        ret = {
            'status_code': "",
            'launch_url': ""
        }
        result = self._send_request(method='GET', url=request_url)
        ret['status_code'] = result['status_code']
        ret['launch_url'] = result['data']['url']
        return ret
    
    def _get_tii_assignments_data(self, canvas_course_ids, data_types=None, only_assignment_ids=[]):
        if not data_types: data_types = ['quickmarks_usage','review_status']
        tii_assignments = self._get_assignments_for_courses(
            canvas_course_ids=canvas_course_ids,
            external_tool_tag_attributes_url_contains='turnitin'
        )
        data = {}
        metadata = []
        all_quickmarks = {}
        all_rubrics_as_multientry = {}
        
        for canvas_assignment_id in tii_assignments['assignments']:
            if only_assignment_ids and not canvas_assignment_id in only_assignment_ids:
                continue
            all_quickmarks[canvas_assignment_id] = []
            all_rubrics_as_multientry[canvas_assignment_id] = []
            # get sessionless launch request URL
            sessionless_launch = self._get_sessionless_launch_url(tii_assignments['assignments'][canvas_assignment_id]['url'])
            if sessionless_launch['status_code'] != 200:
                logging.debug('failed getting sessionless_launch [{}]'.format(canvas_assignment_id))
                continue
            # perform the sessionless launch
            s = requests.Session()
            s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36'})
            r = s.get(sessionless_launch['launch_url'], proxies=_get_proxies())
            # parse launch parameters
            document = BeautifulSoup(r.text, 'html.parser')
            tii_form_inputs = document.select('form[action*="api.turnitin.com"] > input') # TODO FIX this doesn't work anymore
            # get assignment
            payload = {}
            for tii_form_input in tii_form_inputs:
                payload[tii_form_input['name']] = tii_form_input['value']
            #logging.debug('tii payload ' + str(payload))
            r = s.post('https://api.turnitin.com/api/lti/1p0/assignment', data=payload, proxies=_get_proxies())
            assignment_id = re.findall('(?<=redirect/assignment/comp/)[0-9]+', r.text)
            #logging.debug('assignment_id ' + str(assignment_id))
            # get assignments in inbox
            r = s.get('https://api.turnitin.com/api/rest/1p0/assignment/{assignment_id}/inbox?lang=en_us'.format(assignment_id=assignment_id[0]), proxies=_get_proxies())
            inbox_assignments = r.json()
            current_assignment_key = canvas_assignment_id
            metadata.append({
                'canvas_assignment_id': canvas_assignment_id,
                'safe_name': tii_assignments['assignments'][canvas_assignment_id]['safe_name']
			})
            # clear out rubric meta, even if not being used
            rubric_meta = {}
            # iterate through students in this assignment
            for inbox_student in inbox_assignments['students']:
                # get metadata
                s.headers.update({'cookie': 'session-id={}'.format(s.cookies['session-id'])})
                r = s.get('https://ev.turnitin.com/user/{author_userid}?lang=en_us&cv=1&output=json'.format(author_userid=inbox_student['author_userid']), proxies=_get_proxies())
                student_info = r.json()
                _student_email = student_info['User'][0]['email']
                #logging.debug('_student_email ' + _student_email)
                if _student_email not in data.keys():
                    data[_student_email] = {}
                data[_student_email][current_assignment_key] = {
                    'quickmark_symbol_usage': {},
                    'tii_userid': inbox_student['author_userid'],
                    'student_view_last': '',
                    'grade': inbox_student['grade'] if 'grade' in inbox_student.keys() else '--',
                    'title': inbox_student['title'],
                    'assignment_id': inbox_student['id'],
                    'summary_comment': ""
                }
                # assignment review status
                if 'student_view_last' in inbox_student.keys() and inbox_student['student_view_last']:
                    data[_student_email][current_assignment_key]['student_view_last'] = inbox_student['student_view_last']
                else:
                    data[_student_email][current_assignment_key]['student_view_last'] = 'not viewed'
                # other data
                if ('quickmarks_usage' in data_types or 'rubric_outcomes' in data_types) or 'summary_comments' in data_types:
                    # get overall grademark data
                    r = s.get(
                        'https://ev.turnitin.com/paper/{student_id}/grade_mark?lang=en_us&cv=1&output=json'.format(student_id=inbox_student['id']), 
                        proxies=_get_proxies()
                    )
                    grademark_data = r.json()
                    # get summary comment
                    if 'summary_comments' in data_types:
                        try:
                            summary_comment = grademark_data['Read'][0]['summary']
                        except:
                            summary_comment = '??'
                        data[_student_email][current_assignment_key]['summary_comment'] = summary_comment
                    # get quickmark usage
                    if 'quickmarks_usage' in data_types:
                        if 'QuickMarkTemplateStub' in grademark_data.keys():
                            for quickmark in grademark_data['QuickMarkTemplateStub']:
                                quickmark_cleaned = re.sub("[^A-Z0-9a-z_-]", "_", quickmark['symbol'])
                                # add to quickmark collection
                                if not quickmark_cleaned in all_quickmarks[canvas_assignment_id]:
                                    all_quickmarks[canvas_assignment_id].append(quickmark_cleaned)
                                # save data for student
                                if quickmark_cleaned in data[_student_email][current_assignment_key]['quickmark_symbol_usage'].keys():
                                    data[_student_email][current_assignment_key]['quickmark_symbol_usage'][quickmark_cleaned] += 1
                                else:
                                    data[_student_email][current_assignment_key]['quickmark_symbol_usage'][quickmark_cleaned] = 1
                    # get rubric outcomes
                    if 'rubric_outcomes' in data_types:
                        if 'RubricScoring' in grademark_data.keys():
                            rubric_score_lookup = grademark_data['RubricScoring'][0]['score_lookup']
                            # only request rubric meta if not already
                            if len(rubric_meta) == 0:
                                # grab rubric id
                                rubric_meta['rubric_id'] = grademark_data['RubricScoring'][0]['rubric']
                                # request rubric information
                                r = s.get('https://ev.turnitin.com/gm3/rubric/{rubric_id}?lang=en_us&cv=1&output=json&assignment_id={assignment_id}'.format(
                                    rubric_id=rubric_meta['rubric_id'],
                                    assignment_id=assignment_id[0]
                                ), proxies=_get_proxies())
                                rubric_data = r.json()
                                #logging.debug('tii rubric_meta ' + str(rubric_meta))
                                #logging.debug('tii rubric_data ' + str(rubric_data))
                                # general rubric information
                                rubric_meta['rubric_info'] = rubric_data['Rubric']
                                # parse rubric scales
                                rubric_meta['rubric_scale'] = {}
                                if 'RubricScale' in rubric_data.keys():
                                    rubric_scale_temp = rubric_data['RubricScale']
                                    for scale_item in rubric_scale_temp:
                                        rubric_meta['rubric_scale'][scale_item['id']] = {
                                            'name': scale_item['name'],
                                            'position': scale_item['position'],
                                            'num': scale_item['num'],
                                            'value': scale_item['value']
                                        }
                                # parse rubric criteria
                                rubric_meta['rubric_criteria'] = rubric_data['RubricCriterion']
                                rubric_meta['rubric_criteria'] = sorted(rubric_meta['rubric_criteria'], key=lambda e: e['position'])
                                # parse rubric criterion scales
                                rubric_meta['rubric_criteria_scale'] = {}
                                if 'RubricCriterionScale' in rubric_data.keys():
                                    rubric_criteria_scale_temp = rubric_data['RubricCriterionScale']
                                    for criterion_scale_item in rubric_criteria_scale_temp:
                                        criterion_scale_item_description = criterion_scale_item['description'] if 'description' in criterion_scale_item.keys() else ''
                                        rubric_meta['rubric_criteria_scale'][criterion_scale_item['id']] = {
                                            'value': {
                                                'scoring_method_2': rubric_meta['rubric_scale'][criterion_scale_item['scale_value']]['value'],
                                                'scoring_method_4': criterion_scale_item['value'],
                                                'default': criterion_scale_item['value'] if rubric_meta['rubric_info'][0]['scoring_method'] == 4 else rubric_meta['rubric_scale'][criterion_scale_item['scale_value']]['value']
                                            },
                                            'description': criterion_scale_item_description,
                                            'scale_value_id': criterion_scale_item['scale_value'],
                                            'scale_name': rubric_meta['rubric_scale'][criterion_scale_item['scale_value']]['name']
                                        }
                                # recreate rubric as multientry
                                rubric_meta['rubric_as_multientry'] = []
                                for criterion_scale_item in rubric_data['RubricCriterion']:
                                    if rubric_meta['rubric_info'][0]['scoring_method'] == 6:
                                        rubric_multientry_row = {
                                            'type': "regex",
                                            'label': criterion_scale_item['name'],
                                            'regex': '.*'
                                        }
                                    else:
                                        criterion_multientry_selects = []
                                        for criterion_scale_id in criterion_scale_item['criterion_scales']:
                                            criterion_multientry_selects.append({
                                                'display': rubric_meta['rubric_scale'][rubric_meta['rubric_criteria_scale'][criterion_scale_id]['scale_value_id']]['name'],
                                                'value': rubric_meta['rubric_criteria_scale'][criterion_scale_id]['value']['default'],
                                                'description':rubric_meta['rubric_criteria_scale'][criterion_scale_id]['description']
                                            })
                                        rubric_multientry_row = {
                                            'type': "dropdown",
                                            'label': criterion_scale_item['name'],
                                            'select': criterion_multientry_selects,
                                            'selectmode': 'single'
                                        }
                                    rubric_meta['rubric_as_multientry'].append(rubric_multientry_row)
                                all_rubrics_as_multientry[canvas_assignment_id] = rubric_meta['rubric_as_multientry']
                            # load up this student's rubric outcomes
                            current_student_rubric_outcome = []
                            for rubric_criterion in rubric_meta['rubric_criteria']:
                                if rubric_meta['rubric_info'][0]['scoring_method'] == 6 and 'RubricScoringFeedback' in grademark_data.keys():
                                    try:
                                        current_student_rubric_outcome.append(grademark_data['RubricScoringFeedback'][rubric_criterion['num'] - 1].get('score'))
                                    except:
                                        current_student_rubric_outcome.append('')
                                elif str(rubric_criterion['num']) in rubric_score_lookup.keys():
                                    score_index = rubric_score_lookup[str(rubric_criterion['num'])] - 1
                                    criterion_scale_id = rubric_criterion['criterion_scales'][score_index]
                                    current_student_rubric_outcome.append(rubric_meta['rubric_criteria_scale'][criterion_scale_id]['value']['default'])
                                else:
                                    current_student_rubric_outcome.append('')
                            data[_student_email][current_assignment_key]['rubric_outcome'] = current_student_rubric_outcome
                        else:
                            # no grademark rubric data available
                            data[_student_email][current_assignment_key]['rubric_outcome'] = ''
        return {
            'data': data,
            'metadata': metadata,
            'all_quickmarks': all_quickmarks,
            'all_rubrics_as_multientry': all_rubrics_as_multientry
        }
    
    def get_tii_assignments_grademark_rubric_as_df(self, canvas_course_ids, only_assignment_ids=[]):
        tii_data = self._get_tii_assignments_data(canvas_course_ids, ['rubric_outcomes'], only_assignment_ids)
        column_header_by_assignment_id = {}
        rubrics_as_multientry_mapping = {}
        for assignment_metadata in tii_data['metadata']:
            column_header = "CANVAS{canvas_assignment_id}_TII_RUBRIC_{safe_name}".format(
                canvas_assignment_id=assignment_metadata['canvas_assignment_id'],
                safe_name=assignment_metadata['safe_name']
            )
            column_header_by_assignment_id[assignment_metadata['canvas_assignment_id']] = column_header
            # store mapping for multientry
            rubrics_as_multientry_mapping[column_header] = {
                'canvas_assignment_id': assignment_metadata['canvas_assignment_id'],
                'multi_entry_options': tii_data['all_rubrics_as_multientry'][assignment_metadata['canvas_assignment_id']]
            }
        data = {}
        for student_email in tii_data['data']:
            data[student_email] = {
                'email': student_email
            }
            for assignment_metadata in tii_data['metadata']:
                if assignment_metadata['canvas_assignment_id'] in tii_data['data'][student_email].keys():
                    data[student_email][column_header_by_assignment_id[assignment_metadata['canvas_assignment_id']]] = json.dumps(tii_data['data'][student_email][assignment_metadata['canvas_assignment_id']]['rubric_outcome'])
                else:
                    data[student_email][column_header_by_assignment_id[assignment_metadata['canvas_assignment_id']]] = ''
        return {
            'df': pd.DataFrame.from_dict(data, dtype='str', orient='index'),
            'rubrics_as_multientry_mapping': rubrics_as_multientry_mapping
        }
    
    def get_tii_assignments_data_as_df(self, canvas_course_ids, data_types=['quickmarks_usage','review_status'], only_assignment_ids=[]):
        # get tii data
        tii_data = self._get_tii_assignments_data(canvas_course_ids, data_types, only_assignment_ids)
        quickmarks_mapping = {}
        assignment_meta = {}
        # set up column headers for df
        for assignment_metadata in tii_data['metadata']:
            assignment_meta[assignment_metadata['canvas_assignment_id']] = {
                'column_header_for_data_type': {
                    t: '' for t in data_types
                }
            }
            for data_type in data_types:
                column_header = "CANVAS{canvas_assignment_id}_TII_{data_type}_{safe_name}".format(
                    canvas_assignment_id=assignment_metadata['canvas_assignment_id'],
                    data_type=data_type.upper(),
                    safe_name=assignment_metadata['safe_name']
                )
                assignment_meta[assignment_metadata['canvas_assignment_id']]['column_header_for_data_type'][data_type] = column_header
                if data_type == 'quickmarks_usage':
                    quickmarks_mapping[column_header] = {
                        'canvas_assignment_id': assignment_metadata['canvas_assignment_id'],
                        'multi_entry_options': []
                    }
        # set up quickmarks mapping/multi_entry_options if applicable
        if 'quickmarks_usage' in data_types:
            for column_header in quickmarks_mapping:
                for quickmark in tii_data['all_quickmarks'][quickmarks_mapping[column_header]['canvas_assignment_id']]:
                    quickmarks_mapping[column_header]['multi_entry_options'].append({
                        'label': quickmark,
                        'type': 'regex',
                        'regex': '.*'
					})
        # parse
        data = {}
        for student_email in tii_data['data']:
            data[student_email] = {
                'email': student_email
            }
            for assignment_metadata in tii_data['metadata']:
                if assignment_metadata['canvas_assignment_id'] in tii_data['data'][student_email].keys():
                    for data_type in data_types:
                        column_header = assignment_meta[assignment_metadata['canvas_assignment_id']]['column_header_for_data_type'][data_type]
                        if data_type == 'summary_comments':
                            data[student_email][column_header] = tii_data['data'][student_email][assignment_metadata['canvas_assignment_id']]['summary_comment']
                        elif data_type == 'review_status':
                            dt = tii_data['data'][student_email][assignment_metadata['canvas_assignment_id']]['student_view_last']
                            if dt == 'not viewed':
                                data[student_email][column_header] = dt
                            else:
                                if dt is not None:
                                    data[student_email][column_header] = parser.parse(dt).strftime('%Y-%m-%d %H:%M:%S')
                                else:
                                    data[student_email][column_header] = ''
                        elif data_type == 'quickmarks_usage':
                            # store quickmarks in cell as serialised json array
                            quickmark_usage_data = []
                            for quickmark in tii_data['all_quickmarks'][assignment_metadata['canvas_assignment_id']]:
                                if quickmark in tii_data['data'][student_email][assignment_metadata['canvas_assignment_id']]['quickmark_symbol_usage'].keys():
                                    quickmark_usage_data.append(int(tii_data['data'][student_email][assignment_metadata['canvas_assignment_id']]['quickmark_symbol_usage'][quickmark]))
                                else:
                                    quickmark_usage_data.append(0)
                            data[student_email][column_header] = json.dumps(quickmark_usage_data)
        return {
            'df': pd.DataFrame.from_dict(data, dtype='str', orient='index'),
            'quickmarks_mapping': quickmarks_mapping
        }
    
    def get_assignments_submission_comments_as_df(self, canvas_course_ids, only_assignment_ids=[]):
        assignments = self._get_assignments_for_courses(canvas_course_ids)['assignments']
        students = self.get_course_students_as_df(canvas_course_ids)
        data_by_user = {}
        for index, row in students.iterrows():
            data_by_user[int(row['canvas_id'])] = {
                'assignments': {}
            }
        for canvas_assignment_id in assignments:
            if only_assignment_ids and not canvas_assignment_id in only_assignment_ids:
                continue
            header_name = "CANVAS{canvas_assignment_id}_{safe_name}_SUBMISSION_COMMENTS".format(
                canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                safe_name=assignments[canvas_assignment_id]['safe_name']
            )
            submissions = self._get_submissions_for_assignment(
                canvas_course_id=assignments[canvas_assignment_id]['course_id'],
                canvas_assignment_id=canvas_assignment_id,
                include_comments=True
            )['submissions']
            for user_id in submissions:
                if user_id in data_by_user.keys():
                    data_by_user[user_id]['assignments'][canvas_assignment_id] = {
                        'assignment_safe_name': assignments[canvas_assignment_id]['safe_name'],
                        'header_name': header_name,
                        'submission_comments': []
                    }
                    if 'submission_comments' in submissions[user_id].keys():
                        for submission_comment in submissions[user_id]['submission_comments']:
                            data_by_user[user_id]['assignments'][canvas_assignment_id]['submission_comments'].append(
                                submission_comment['comment']
                            )
                    else:
                        data_by_user[user_id]['assignments'][canvas_assignment_id]['submission_comments'] = ""
                else:
                    pass # problem
        # put into final dict
        data = {}
        for index, df_row in students.iterrows():
            row = {
                'email': df_row['email'],
                'sid': df_row['sid']
            }
            canvas_user_id = int(df_row['canvas_id'])
            for canvas_assignment_id in data_by_user[canvas_user_id]['assignments']:
                header_name = data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['header_name']
                if isinstance(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['submission_comments'], list) and len(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['submission_comments']):
                    row[header_name] = json.dumps(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['submission_comments'])
                else:
                    row[header_name] = ''
            data[canvas_user_id] = deepcopy(row)
            del row
        return pd.DataFrame.from_dict(data, dtype='str', orient='index')
    
    def get_assignments_rubric_outcomes_as_df(self, canvas_course_ids, only_assignment_ids=[], import_comments=False):
        assignments = self._get_assignments_for_courses(canvas_course_ids)['assignments']
        students = self.get_course_students_as_df(canvas_course_ids)
        data_by_user = {}
        rubric_mapping = {}
        for index, row in students.iterrows():
            data_by_user[int(row['canvas_id'])] = {
                'assignments': {}
            }
        for canvas_assignment_id in assignments:
            if only_assignment_ids and not canvas_assignment_id in only_assignment_ids:
                continue
            header_name = "CANVAS{canvas_assignment_id}_{safe_name}_RUBRIC_OUTCOMES".format(
                canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                safe_name=assignments[canvas_assignment_id]['safe_name']
            )
            header_name_comments = "CANVAS{canvas_assignment_id}_{safe_name}_RUBRIC_COMMENTS".format(
                canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                safe_name=assignments[canvas_assignment_id]['safe_name']
            )
            _data = self._get_submissions_for_assignment(
                canvas_course_id=assignments[canvas_assignment_id]['course_id'],
                canvas_assignment_id=canvas_assignment_id,
                include_rubric=True
            )
            # process rubric
            rubric = _data['rubric']
            rubric_mapping[header_name] = {
                'canvas_assignment_id': canvas_assignment_id,
                'multi_entry_options': []
            }
            for i, criterion in enumerate(rubric):
                label = criterion.get('description', 'Criterion {}'.format(i))
                if criterion.get('points'):
                    label += ' (/{})'.format(criterion.get('points'))
                multi_entry_option = {
                    'label': label,
                    'type': 'slider',
                    'regex': '.*',
                    'select': [],
                    'range_mode': 'roundup',
                    'slider_mode': 'numeric-free',
                    'slider_step': 0.1
                }
                for j, rating in enumerate(criterion.get('ratings', [])):
                    multi_entry_option['select'].append({
                        'display': rating.get('description', 'Rating {}'.format(j)),
                        'description': rating.get('long_description', ''),
                        'value': rating.get('points', '')
                    })
                # add zero rating
                if multi_entry_option['select'] and criterion.get('points'):
                    multi_entry_option['select'].append({
                        'display': '0',
                        'description': '',
                        'value': 0
                    })
                # append to multientry options
                rubric_mapping[header_name]['multi_entry_options'].append(multi_entry_option)
            if import_comments:
                rubric_mapping[header_name_comments] = {
                    'canvas_assignment_id': canvas_assignment_id,
                    'multi_entry_options': []
                }
                for i, criterion in enumerate(rubric):
                    rubric_mapping[header_name_comments]['multi_entry_options'].append({
                        'label': criterion.get('description', f'Criterion {i}'),
                        'type': 'regex',
                        'regex': '.*'
                    })
            # process submissions
            submissions = _data['submissions']
            for user_id in submissions:
                if user_id in data_by_user.keys():
                    data_by_user[user_id]['assignments'][canvas_assignment_id] = {
                        'assignment_safe_name': assignments[canvas_assignment_id]['safe_name'],
                        'header_name': header_name,
                        'rubric_outcomes': [],
                        'header_name_comments': header_name_comments,
                        'rubric_comments': ''
                    }
                    # rubric outcomes
                    if 'rubric_outcomes' in submissions[user_id].keys():
                        data_by_user[user_id]['assignments'][canvas_assignment_id]['rubric_outcomes'] = submissions[user_id]['rubric_outcomes']['data']
                    else:
                        data_by_user[user_id]['assignments'][canvas_assignment_id]['rubric_outcomes'] = ""
                    # rubric comments
                    if import_comments:
                        if 'rubric_outcomes' in submissions[user_id].keys():
                            data_by_user[user_id]['assignments'][canvas_assignment_id]['rubric_comments'] = submissions[user_id]['rubric_outcomes']['comments']
                        else:
                            data_by_user[user_id]['assignments'][canvas_assignment_id]['rubric_comments'] = ""
                else:
                    pass # problem
        # put into final dict
        data = {}
        for index, df_row in students.iterrows():
            row = {
                'email': df_row['email'],
                'sid': df_row['sid']
            }
            canvas_user_id = int(df_row['canvas_id'])
            for canvas_assignment_id in data_by_user[canvas_user_id]['assignments']:
                # rubric outcomes
                header_name = data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['header_name']
                if isinstance(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['rubric_outcomes'], list) and len(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['rubric_outcomes']):
                    row[header_name] = json.dumps(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['rubric_outcomes'])
                else:
                    row[header_name] = ''
                # rubric comments
                if import_comments:
                    header_name_comments = data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['header_name_comments']
                    if isinstance(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['rubric_comments'], list) and len(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['rubric_comments']):
                        row[header_name_comments] = json.dumps(data_by_user[canvas_user_id]['assignments'][canvas_assignment_id]['rubric_comments'])
                    else:
                        row[header_name_comments] = ''
            data[canvas_user_id] = deepcopy(row)
            del row
        # return
        return {
            'data': pd.DataFrame.from_dict(data, dtype='str', orient='index'),
            'rubric_mapping': rubric_mapping
        }
    
    def get_assignments_peer_review_scores_as_df(self, canvas_course_ids, only_assignment_ids=[], include_comments=False, criterion_aggregation_method='mean', overall_aggregation_method='mean'):
        assignments = self._get_assignments_for_courses(canvas_course_ids)['assignments']
        students = self.get_course_students_as_df(canvas_course_ids)
        data_by_user = {}
        for index, row in students.iterrows():
            data_by_user[int(row['canvas_id'])] = {
                'email': row['email'],
                'sid': row['sid']
            }
        multi_entry_options_by_column = {}
        for canvas_assignment_id in assignments:
            if only_assignment_ids and canvas_assignment_id not in only_assignment_ids:
                continue
            ### process peer review assignations ###
            header_name_statuses = "CANVAS_{canvas_assignment_id}_{safe_name}_PEER_REVIEW_STATUSES".format(
                canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                safe_name=assignments[canvas_assignment_id]['safe_name']
            )
            # data
            peer_reviews = self._get_peer_reviews_for_assignment(
                canvas_course_id=assignments[canvas_assignment_id]['course_id'],
                canvas_assignment_id=canvas_assignment_id,
                include_comments=include_comments
            )
            for assessor_user_id in peer_reviews['peer_reviews_by_assessor']:
                if assessor_user_id in data_by_user.keys():
                    data_by_user[assessor_user_id][header_name_statuses] = json.dumps([
                        peer_reviews['peer_reviews_by_assessor'][assessor_user_id]['completed'],
                        peer_reviews['peer_reviews_by_assessor'][assessor_user_id]['assigned']
                    ])
                else:
                    pass # problem
            # mapping
            multi_entry_options_by_column[header_name_statuses] = {
                'multi_entry_options': [
                    {
                        'label': "Reviews completed",
                        'type': 'regex',
                        'regex': '^[0-9]*$'
                    },
                    {
                        'label': "Reviews assigned but not yet completed",
                        'type': 'regex',
                        'regex': '^[0-9]*$'
                    }
                ]
            }
            ### process peer review assessments ###
            header_name_scores = "CANVAS_{canvas_assignment_id}_{safe_name}_PEER_REVIEW_RUBRIC_SCORES".format(
                canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                safe_name=assignments[canvas_assignment_id]['safe_name']
            )
            peer_assessments = self._get_rubric_peer_assessments(
                canvas_course_id=assignments[canvas_assignment_id]['course_id'],
                canvas_assignment_id=canvas_assignment_id,
                criterion_aggregation_method=criterion_aggregation_method,
                overall_aggregation_method=overall_aggregation_method
            )
            for assessee_user_id in peer_assessments['assessments_by_assessee']:
                if assessee_user_id in data_by_user.keys():
                    assessee_data = peer_assessments['assessments_by_assessee'][assessee_user_id]['aggregated_points_by_criterion_for_loading']
                    assessee_data.append(
                        peer_assessments['assessments_by_assessee'][assessee_user_id]['count_peer_assessments']
                    )
                    assessee_data.append(
                        peer_assessments['assessments_by_assessee'][assessee_user_id]['aggregated_score']
                    )
                    data_by_user[assessee_user_id][header_name_scores] = json.dumps(assessee_data)
                else:
                    pass # problem
            # mapping
            multi_entry_options_by_column[header_name_scores] = { 'multi_entry_options': [] }
            for rubric_criterion_id in peer_assessments['rubric_criteria']:
                multi_entry_options_by_column[header_name_scores]['multi_entry_options'].extend(
                    [
                        {
                            'label': 'Criterion: {description} ({criterion_aggregation_method}, out of {points})'.format(
                                description=peer_assessments['rubric_criteria'][rubric_criterion_id]['description'],
                                criterion_aggregation_method=criterion_aggregation_method,
                                points=peer_assessments['rubric_criteria'][rubric_criterion_id]['points']
                            ),
                            'type': 'regex',
                            'regex': '.*'
                        }
                    ]
                )
            multi_entry_options_by_column[header_name_scores]['multi_entry_options'].extend(
                [
                    {
                        'label': 'Peer reviews received',
                        'type': 'regex',
                        'regex': '.*'
                    },
                    {
                        'label': '{} total score'.format(overall_aggregation_method.capitalize()),
                        'type': 'regex',
                        'regex': '.*'
                    }
                ]
            )
            ### process peer review comments ###
            if include_comments:
                header_name_comments = "CANVAS_{canvas_assignment_id}_{safe_name}_PEER_REVIEW_COMMENTS".format(
                    canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                    safe_name=assignments[canvas_assignment_id]['safe_name']
                )
                # data
                for assessee_user_id in peer_reviews['peer_reviews_by_assessee']:
                    if assessee_user_id in data_by_user.keys():
                        data_by_user[assessee_user_id][header_name_comments] = json.dumps([
                            peer_reviews['peer_reviews_by_assessee'][assessee_user_id]['peer_comments']
                        ])
                    else:
                        pass # problem
                # mapping
                multi_entry_options_by_column[header_name_comments] = {
                    'multi_entry_options': [
                        {
                            'label': "Peer comments",
                            'type': 'regex',
                            'regex': '.*'
                        }
                    ]
                }
        return {
            'data': pd.DataFrame.from_dict(data_by_user, dtype='str', orient='index'),
            'mapped_multi_entry_options': multi_entry_options_by_column
        }
    
    def get_assignments_submission_attachments_as_df(self, canvas_course_ids, only_assignment_ids=[]):
        assignments = self._get_assignments_for_courses(canvas_course_ids)['assignments']
        students = self.get_course_students_as_df(canvas_course_ids)
        data_by_user = {}
        for index, row in students.iterrows():
            data_by_user[int(row['canvas_id'])] = {
                'email': row['email'],
                'sid': row['sid']
            }
        multi_entry_options_by_column = {}
        for canvas_assignment_id in assignments:
            if only_assignment_ids and canvas_assignment_id not in only_assignment_ids:
                continue
            # make a header name
            header_name = "CANVAS_{canvas_assignment_id}_{safe_name}_SUBMISSION_FILES".format(
                canvas_assignment_id=assignments[canvas_assignment_id]['id'],
                safe_name=assignments[canvas_assignment_id]['safe_name']
            )
            # data
            _data = self._get_submissions_for_assignment(
                    assignments[canvas_assignment_id]['course_id'],
                    canvas_assignment_id,
                    include_attachments=True
                )['submissions']
            # loop people
            for student_canvas_user_id in data_by_user.keys():
                attachments = []
                if student_canvas_user_id in _data.keys():
                    attachments = _data[student_canvas_user_id].get('attachments', [])
                data_by_user[student_canvas_user_id][header_name] = json.dumps(attachments)
        return {
            'data': pd.DataFrame.from_dict(data_by_user, dtype='str', orient='index')
        }
    
    def _get_sections_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': '',
            'sections': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/sections?include[]=students'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                )
            )
            if result['data'] is None:
                continue
            for section in result['data']:
                ret['sections'][section['id']] = {
                    'id': section['id'],
                    'course_id': section['course_id'],
                    'name': section['name']
                }
                # gather students
                if 'students' not in ret['sections'][section['id']]:
                    ret['sections'][section['id']]['students'] = {}
                if isinstance(section['students'], list):
                    for section_student in section['students']:
                        ret['sections'][section['id']]['students'][section_student['id']] = {
                            'id': section_student['id'],
                            'sis_user_id': section_student['sis_user_id'] if 'sis_user_id' in section_student.keys() else '',
                            'login_id': section_student['login_id'] if 'login_id' in section_student.keys() else ''
                        }
            ret['status_code'] = result['status_code']
        return ret
    
    def _get_sections_by_user_for_courses(self, canvas_course_ids):
        sections = self._get_sections_for_courses(canvas_course_ids)['sections']
        users = self.get_course_students_as_df(canvas_course_ids)
        sections_by_user = {}
        for section_id in sections:
            section_name = sections[section_id]['name']
            for canvas_user_id in sections[section_id]['students']:
                if canvas_user_id not in sections_by_user.keys():
                    sections_by_user[canvas_user_id] = {
                        'sections': [section_name]
                    }
                else:
                    sections_by_user[canvas_user_id]['sections'].append(section_name)
        data = {}
        for index, row in users.iterrows():
            canvas_user_id = int(row['canvas_id'])
            data[canvas_user_id] = {
                'sid': row['sid'],
                'email': row['email'],
                "CANVAS_SECTION_MEMBERSHIPS": json.dumps(sections_by_user[canvas_user_id]['sections']) if canvas_user_id in sections_by_user.keys() else ''
            }
        return pd.DataFrame.from_dict(data, dtype='str', orient='index')
    
    def _get_group_categories_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': "",
            'group_categories': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/group_categories'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                )
            )
            if result['data'] is None:
                continue
            for group_category in result['data']:
                ret['group_categories'][group_category['id']] = {
                    'id': group_category['id'],
                    'course_id': group_category['course_id'],
                    'name': group_category['name'],
                    'context_type': group_category['context_type']
                }
            ret['status_code'] = result['status_code']
        return ret
    
    def _get_groups_in_group_category(self, group_category_id):
        ret = {
            'status_code': "",
            'groups': {}
        }
        result = self._send_request(
            method='GET', 
            url=r'{api_url}group_categories/{group_category_id}/groups'.format(
                api_url=self.config['api_url'],
                group_category_id=group_category_id
            )
        )
        if result is not None and result['data'] is not None:
            for group in result['data']:
                ret['groups'][group['id']] = {
                        'id': group['id'],
                        'group_category_id': group['group_category_id'],
                        'course_id': group['course_id'],
                        'name': group['name'],
                        'members_count': group['members_count'],
                        'context_type': group['context_type']
                    }
            ret['status_code'] = result['status_code']
        return ret
    
    def _get_users_in_group(self, group_id):
        ret = {
            'status_code': "",
            'user_ids': []
        }
        result = self._send_request(
            method='GET', 
            url=r'{api_url}groups/{group_id}/users'.format(
                api_url=self.config['api_url'],
                group_id=group_id
            )
        )
        if result['data'] is not None:
            ret['user_ids'] = [r['id'] for r in result['data']]
        ret['status_code'] = result['status_code']
        return ret
    
    def get_group_memberships_by_user_for_courses_as_df(self, canvas_course_ids):
        
        users = self.get_course_students_as_df(canvas_course_ids)
        groupsets_per_student = {}
        groupsets_headers = []
        group_categories = self._get_group_categories_for_courses(canvas_course_ids)['group_categories']
        
        for group_category_id in group_categories:
        
            group_category_name = group_categories[group_category_id]['name']
            unique_safe_groupset_name = "{group_category_id}_{safe_name}".format(
                group_category_id=group_category_id,
                safe_name=re.sub("[^A-Z0-9a-z_]", "_", group_category_name)
            )
            
            groups = self._get_groups_in_group_category(group_category_id)['groups']
            for group_id in groups:
                group_name = groups[group_id]['name']
                members = self._get_users_in_group(group_id)['user_ids']
                for canvas_user_id in members:
                    if canvas_user_id in groupsets_per_student.keys():
                        if group_category_id in groupsets_per_student[canvas_user_id].keys():
                            groupsets_per_student[canvas_user_id][group_category_id].append(group_name)
                        else:
                            groupsets_per_student[canvas_user_id][group_category_id] = [group_name]
                    else:
                        groupsets_per_student[canvas_user_id] = {
                            group_category_id: [group_name]
                        }
                groupsets_headers.append("CANVAS_GROUPSET_{unique_safe_groupset_name}_MEMBERSHIPS".format(
                    unique_safe_groupset_name=unique_safe_groupset_name
                ))
        
        data = {}
        for index, df_row in users.iterrows():
            canvas_user_id = int(df_row['canvas_id'])
            if canvas_user_id in groupsets_per_student:
                student_found = True
            else:
                student_found = False
            row = {
                'sid': df_row['sid'],
                'email': df_row['email']
            }
            for group_category_id in group_categories:
                if student_found:
                    try:
                        member_of = groupsets_per_student[canvas_user_id][group_category_id]
                    except:
                        member_of = []
                else:
                    member_of = []
                unique_safe_groupset_name = "{group_category_id}_{safe_name}".format(
                    group_category_id=group_category_id,
                    safe_name=re.sub('[^A-Z0-9a-z_]', '_', group_categories[group_category_id]['name'])
                )
                row["CANVAS_GROUPSET_{unique_safe_groupset_name}_MEMBERSHIPS".format(
                    unique_safe_groupset_name=unique_safe_groupset_name
                )] = json.dumps(member_of)
            data[canvas_user_id] = deepcopy(row)
            del row
        _df = pd.DataFrame.from_dict(data, dtype='str', orient='index')
        return _df
    
    def _get_discussion_topics_for_group(self, group_id):
        ret = {
            'status_code': "",
            'topics': {}
        }
        result = self._send_request(
            method='GET', 
            url=r'{api_url}groups/{group_id}/discussion_topics'.format(
                api_url=self.config['api_url'],
                group_id=group_id
            )
        )
        if result is not None and result['data'] is not None:
            for topic in result['data']:
                ret['topics'][topic['id']] = {
                    'id': topic['id'],
                    'title': topic['title'],
                    'root_topic_id': topic['root_topic_id'],
                    'url': topic['url'],
                    'group_category_id': topic['group_category_id'],
                    'group_id': group_id,
                    'discussion_subentry_count': topic['discussion_subentry_count']
                }
            ret['status_code'] = result['status_code']
        return ret
    
    def _get_discussion_topics_for_courses(self, canvas_course_ids):
        ret = {
            'status_code': "",
            'topics': {}
        }
        for canvas_course_id in canvas_course_ids:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/discussion_topics'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id
                )
            )
            if result['data'] is None:
                continue
            for discussion_topic in result['data']:
                ret['topics'][discussion_topic['id']] = {
                    'id': discussion_topic['id'],
                    'course_id': canvas_course_id,
                    'title': discussion_topic['title'],
                    'discussion_subentry_count': discussion_topic['discussion_subentry_count'],
                    'published': discussion_topic['published'],
                    'locked': discussion_topic['locked'],
                    'message': discussion_topic['message'],
                    'html_url': discussion_topic['html_url'],
                    'group_category_id': discussion_topic['group_category_id']
                }
                ret['status_code'] = result['status_code']
        return ret
    
    def _get_discussion_topic_participation_by_student(self, canvas_course_id, discussion_topic_id, student_data, group_category_id, fetch_posts_text=False, fetch_replies_text=False):
        if group_category_id == 'null' or group_category_id is None:
            result = self._send_request(
                method='GET', 
                url=r'{api_url}courses/{canvas_course_id}/discussion_topics/{discussion_topic_id}/view'.format(
                    api_url=self.config['api_url'],
                    canvas_course_id=canvas_course_id,
                    discussion_topic_id=discussion_topic_id
                )
            )
            full_view = result['data']
            self._parse_discussion_topic_view(
                view=full_view['view'], 
                student_data=student_data, 
                discussion_topic_id=discussion_topic_id,
                fetch_posts_text=fetch_posts_text,
                fetch_replies_text=fetch_replies_text
            )
        elif utils.is_number(group_category_id):
            groups = self._get_groups_in_group_category(group_category_id)['groups']
            for group_id in groups:
                group_topics = self._get_discussion_topics_for_group(group_id)['topics']
                for group_topic_id in group_topics:
                    if discussion_topic_id != group_topics[group_topic_id]['root_topic_id']:
                        # skip otherwise will double-count!
                        continue
                    result = self._send_request(
                        method='GET', 
                        url=r'{api_url}groups/{group_id}/discussion_topics/{group_topic_id}/view'.format(
                            api_url=self.config['api_url'],
                            group_id=group_id,
                            group_topic_id=group_topic_id
                        )
                    )
                    full_view = result['data']
                    self._parse_discussion_topic_view(
                        view=full_view['view'], 
                        student_data=student_data, 
                        discussion_topic_id=group_topics[group_topic_id]['root_topic_id'],
                        fetch_posts_text=fetch_posts_text,
                        fetch_replies_text=fetch_replies_text
                    )
        # no return value; modifies student_data directly
        pass
    
    def _parse_discussion_topic_view(self, view, student_data, discussion_topic_id, fetch_posts_text=False, fetch_replies_text=False):
        for root_reply in view:
            if 'user_id' in root_reply.keys() and root_reply['user_id'] in student_data.keys():
                student_data[root_reply['user_id']]['total']['posts'] += 1
                student_data[root_reply['user_id']]['total']['posts_text'].append(root_reply['message'])
                if discussion_topic_id in student_data[root_reply['user_id']].keys():
                    student_data[root_reply['user_id']][discussion_topic_id]['posts'] += 1
                    student_data[root_reply['user_id']][discussion_topic_id]['posts_text'].append(root_reply['message'])
            else:
                # problem
                pass
            if 'replies' in root_reply.keys():
                recursive_participation = self._parse_discussion_replies_for_participation(root_reply['replies'])
                for canvas_user_id in recursive_participation:
                    if canvas_user_id in student_data.keys():
                        student_data[canvas_user_id]['total']['replies'] += recursive_participation[canvas_user_id]['replies']
                        student_data[canvas_user_id]['total']['replies_text'].extend(recursive_participation[canvas_user_id]['replies_text'])
                        if discussion_topic_id in student_data[canvas_user_id].keys():
                            student_data[canvas_user_id][discussion_topic_id]['replies'] += recursive_participation[canvas_user_id]['replies']
                            student_data[canvas_user_id][discussion_topic_id]['replies_text'].extend(recursive_participation[canvas_user_id]['replies_text'])
        # no return value
        pass
    
    def _parse_discussion_replies_for_participation(self, replies, recursive=True):
        data = {}
        for reply in replies:
            if 'user_id' not in reply:
                continue
            if reply['user_id'] in data.keys():
                data[reply['user_id']]['replies'] += 1
                data[reply['user_id']]['replies_text'].append(reply['message'])
            else:
                data[reply['user_id']] = {
                    'replies': 1,
                    'replies_text': [ reply['message'] ]
                }
            if recursive and 'replies' in reply:
                next_level_data = self._parse_discussion_replies_for_participation(replies=reply['replies'], recursive=True)
                for next_level_user_id in next_level_data:
                    if next_level_user_id in data.keys():
                        data[next_level_user_id]['replies'] += 1
                        if type(data[next_level_user_id]['replies_text']) is not list:
                            data[next_level_user_id]['replies_text'] = []
                        data[next_level_user_id]['replies_text'].extend(next_level_data[next_level_user_id]['replies_text'])
                    else:
                        data[next_level_user_id] = {
                            'replies': 1,
                            'replies_text': next_level_data[next_level_user_id]['replies_text']
                        }
        return data
    
    def get_discussion_engagement_as_df(self, canvas_course_ids, mode='overall', fetch_posts_text=False, fetch_replies_text=False):
        """
            canvas_course_ids (list of ints)
            mode (str) overall|by_topic
        """
        students = {}
        # get discussion topics
        discussion_topics = self._get_discussion_topics_for_courses(canvas_course_ids)
        # get students
        for canvas_course_id in canvas_course_ids:
            df_students = self.get_course_students_as_df(canvas_course_id)
            for index, row in df_students.iterrows():
                canvas_user_id = int(row['canvas_id'])
                if canvas_user_id not in students.keys():
                    students[canvas_user_id] = {
                        'total': {
                            'posts': 0,
                            'replies': 0,
                            'posts_text': [],
                            'replies_text': []
                        },
                        'details': {
                            'sid': row['sid'],
                            'email': row['email']
                        }
                    }
                for discussion_topic_id in discussion_topics['topics']:
                    students[canvas_user_id][discussion_topic_id] = {
                        'posts': 0,
                        'replies': 0,
                        'posts_text': [],
                        'replies_text': []
                    }
        # loop through discussion topics and calculate participation
        for discussion_topic_id in discussion_topics['topics']:
            self._get_discussion_topic_participation_by_student(
                canvas_course_id=discussion_topics['topics'][discussion_topic_id]['course_id'], 
                discussion_topic_id=discussion_topic_id,
                student_data=students, # passed by reference; method changes variable directly
                group_category_id=discussion_topics['topics'][discussion_topic_id]['group_category_id'],
                fetch_posts_text=fetch_posts_text,
                fetch_replies_text=fetch_replies_text
            )
        # set up for return
        def _strip_tags(htmls):
            # cleaner
            ret = []
            #htmls, _topology = utils.flatten_list(htmls)
            for html in htmls:
                soup = BeautifulSoup(html, 'html.parser')
                #soup.script.decompose()
                ret.append(soup.get_text())
            return ret
        data = {}
        if mode == 'overall':
            for canvas_student_id in students:
                data[canvas_student_id] = {
                    'sid':students[canvas_student_id]['details']['sid'],
                    'email': students[canvas_student_id]['details']['email'],
                    'CANVAS_DISCUSSIONS_OVERALL': json.dumps([students[canvas_student_id]['total']['posts'], students[canvas_student_id]['total']['replies']])
                }
                if fetch_posts_text:
                    data[canvas_student_id]['CANVAS_DISCUSSIONS_OVERALL_POSTS_TEXT'] = json.dumps([ _strip_tags(students[canvas_student_id]['total']['posts_text']) ])
                if fetch_replies_text:
                    data[canvas_student_id]['CANVAS_DISCUSSIONS_OVERALL_REPLIES_TEXT'] = json.dumps([ _strip_tags(students[canvas_student_id]['total']['replies_text']) ])
        elif mode == 'by_topic':
            for canvas_student_id in students:
                data[canvas_student_id] = {
                    'sid': students[canvas_student_id]['details']['sid'],
                    'email': students[canvas_student_id]['details']['email']
                }
                for discussion_topic_id in discussion_topics['topics']:
                    _safe_title = re.sub("[^A-Z0-9a-z_]", "_", discussion_topics['topics'][discussion_topic_id]['title'])
                    safe_header = f"CANVAS_DISCUSSIONS_{discussion_topic_id}_{_safe_title}"
                    data[canvas_student_id][safe_header] = json.dumps([
                        students[canvas_student_id][discussion_topic_id]['posts'],
                        students[canvas_student_id][discussion_topic_id]['replies']
                    ])
                    # store posts_text and replies_text
                    if fetch_posts_text:
                        safe_header = f"CANVAS_DISCUSSIONS_POSTS_TEXT_{discussion_topic_id}_{_safe_title}"
                        data[canvas_student_id][safe_header] = json.dumps([ _strip_tags(students[canvas_student_id][discussion_topic_id]['posts_text']) ])
                    if fetch_replies_text:
                        safe_header = f"CANVAS_DISCUSSIONS_REPLIES_TEXT_{discussion_topic_id}_{_safe_title}"
                        data[canvas_student_id][safe_header] = json.dumps([ _strip_tags(students[canvas_student_id][discussion_topic_id]['replies_text']) ])
                #data[canvas_student_id] = deepcopy(row)
        # set up mappings for return
        mapped_multi_entry_options = {}
        if mode == 'overall':
            mapped_multi_entry_options['CANVAS_DISCUSSIONS_OVERALL'] = {}
            mapped_multi_entry_options['CANVAS_DISCUSSIONS_OVERALL']['multi_entry_options'] = [
                {
                    'label': 'Total number of posts',
                    'type': 'regex',
                    'regex': '^[0-9]*$'
                },
                {
                    'label': 'Total number of replies',
                    'type': 'regex',
                    'regex': '^[0-9]*$'
                }
            ]
            if fetch_posts_text:
                mapped_multi_entry_options['CANVAS_DISCUSSIONS_OVERALL_POSTS_TEXT'] = {
                    'multi_entry_options': [
                        {
                            'label': 'Text of posts',
                            'type': 'regex',
                            'regex': '.*'
                        }
                    ]
                }
            if fetch_replies_text:
                mapped_multi_entry_options['CANVAS_DISCUSSIONS_OVERALL_REPLIES_TEXT'] = {
                    'multi_entry_options': [
                        {
                            'label': 'Text of replies',
                            'type': 'regex',
                            'regex': '.*'
                        }
                    ]
                }
        elif mode == 'by_topic':
            for discussion_topic_id in discussion_topics['topics']:
                _safe_title = re.sub("[^A-Z0-9a-z_]", "_", discussion_topics['topics'][discussion_topic_id]['title'])
                safe_header = f"CANVAS_DISCUSSIONS_{discussion_topic_id}_{_safe_title}"
                mapped_multi_entry_options[safe_header] = {}
                mapped_multi_entry_options[safe_header]['multi_entry_options'] = [
                    {
                        'label': 'Total number of posts',
                        'type': 'regex',
                        'regex': '^[0-9]*$'
                    },
                    {
                        'label': 'Total number of replies',
                        'type': 'regex',
                        'regex': '^[0-9]*$'
                    }
                ]
                if fetch_posts_text:
                    safe_header = f"CANVAS_DISCUSSIONS_POSTS_TEXT_{discussion_topic_id}_{_safe_title}"
                    mapped_multi_entry_options[safe_header] = {
                        'multi_entry_options': {
                            {
                                'label': 'Text of posts',
                                'type': 'regex',
                                'regex': '.*'
                            },
                        }
                    }
                if fetch_replies_text:
                    safe_header = f"CANVAS_DISCUSSIONS_REPLIES_TEXT_{discussion_topic_id}_{_safe_title}"
                    mapped_multi_entry_options[safe_header] = {
                        'multi_entry_options': {
                            {
                                'label': 'Text of replies',
                                'type': 'regex',
                                'regex': '.*'
                            },
                        }
                    }
        return {
            'df': pd.DataFrame.from_dict(data, dtype='str', orient='index'),
            'mapping': mapped_multi_entry_options
        }
    
    def create_conversation(self, subject, body, recipient, override_course_context_id=None):
        """Creates a Canvas Conversation with the specified recipients.
            
            subject (string)
            body (string)
            recipient (Canvas user id)
            override_course_context_id (string)
        """
        #print('create_conversation', subject, body, recipient, override_course_context_id)
        if override_course_context_id is None:
            self.load_connected_course_ids()
            context_code = f'course_{canvas_connector.connected_course_ids[0]}'
        else:
            context_code = f'course_{override_course_context_id}'
        #print('create_conversation about to send...', context_code)
        result = self._send_request(
            method='POST', 
            url=r'{api_url}conversations'.format(
                api_url=self.config['api_url'],
            ),
            parameters={
                'subject': subject,
                'body': body,
                'force_new': True,
                'recipients[]': recipient,
                'context_code': context_code
            }
        )
        #print(recipient, result)
    
    #####################
    # Generic functions #
    #####################
    
    def _send_request(self, url, method='GET', parameters={}, failure_count=0, ignore_link_header=False, unpack_key='', use_admin_token=False):
        """
            Sends a request to Canvas API and follows all link rels as necessary.
            
            url (string)
            method (string)
            parameters (dict) for data payload i.e. form fields
            failure_count (int)
            ignore_link_header (bool)
            unpack_key (string) If request is expected to return an object with an array of objects, 
                the unpack_key specifies the first-level object's key to read
            use_admin_token (boolean) Whether to use the configured admin token
        """
        #print('requesting', url)
        ret = {
            'data': None,
            'raw': None,
            'status_code': 0,
            'headers': None
        }
        # get auth token
        auth_token = None
        if use_admin_token:
            auth_token = self.config.get('admin_auth_token')
        if auth_token is None:
            auth_token = self.get_auth_token()
        # run the request
        if method.lower() == 'get':
            r = requests.get(
                url,
                headers={'Authorization': 'Bearer {}'.format(auth_token)},
                data=parameters,
                proxies=_get_proxies()
            )
        elif method.lower() == 'post':
            r = requests.post(
                url,
                headers={'Authorization': 'Bearer {}'.format(auth_token)},
                data=parameters,
                proxies=_get_proxies()
            )
        request_cost = r.headers['x-request-cost'] if 'x-request-cost' in [k.lower() for k in r.headers.keys()] else '?'
        ret['status_code'] = r.status_code
        ret['headers'] = deepcopy(r.headers)
        ret['raw'] = r.text
        if r.status_code == 200:
            ret['data'] = r.json()
            # deal with pagination
            if 'link' in [k.lower() for k in r.headers.keys()] and not ignore_link_header:
                temp_filecontent = r.json()
                r_internal = {
                    'status_code': r.status_code,
                    'headers': deepcopy(r.headers)
                }
                combined_filecontent = []
                if unpack_key == '' or isinstance(temp_filecontent, list):
                    combined_filecontent.extend(temp_filecontent)
                else:
                    if unpack_key.lower() in [k.lower() for k in temp_filecontent.keys()]:
                        combined_filecontent.extend(temp_filecontent[unpack_key])
                # loop through links
                for loop_counter in range(1, 201):
                    if 'link' in [k.lower() for k in r_internal['headers'].keys()]:
                        # parse link header
                        parsed_link_headers = {}
                        for link_string in r_internal['headers']['link'].split(','):
                            link_url = re.findall('(?<=<).+?(?=>)', link_string)[0]
                            link_url = parse.unquote(link_url)
                            link_rel = re.findall('(?<=rel=").+?(?=")', link_string)[0]
                            parsed_link_headers[link_rel] = link_url
                        # see if next link exists
                        if 'next' in parsed_link_headers.keys():
                            # next exists, so get it
                            r_internal = self._send_request(
                                url=parsed_link_headers['next'],
                                method=method,
                                parameters=parameters,
                                ignore_link_header=True,
                                unpack_key=unpack_key
                            )
                            #print('nexting', r_internal['headers'])
                            if r_internal['status_code'] == 200:
                                temp_filecontent = r_internal['data']
                                if unpack_key == '' or isinstance(temp_filecontent, list):
                                    if temp_filecontent:
                                        combined_filecontent.extend(temp_filecontent)
                                else:
                                    if unpack_key.lower() in [k.lower() for k in temp_filecontent.keys()]:
                                        combined_filecontent.extend(temp_filecontent[unpack_key])
                            else:
                                continue
                    else:
                        break
                ret['data'] = combined_filecontent
            else:
                if unpack_key == '':
                    pass
                else:
                    # if unpacking needed, just get response content
                    temp_filecontent = r.json()
                    if unpack_key.lower() in [k.lower() for k in temp_filecontent.keys()]:
                        ret['data'] = temp_filecontent[unpack_key]
            return ret
        elif r.status_code == 201:
            ret['data'] = r.json()
            ret['status_code'] = r.status_code
            return ret
        elif r.status_code == 401:
            if failure_count > 5:
                return ret
            # check if WWW-Authenticate header exists
            if 'www-authenticate' in [k.lower() for k in r.headers.keys()] and 'oauth2' in self.config['methods']:
                # request token refresh
                refresh_result = self.refresh_oauth2_token()
                if refresh_result and refresh_result['success']:
                    # retry request
                    return self._send_request(
                        url=url,
                        method=method,
                        parameters=parameters,
                        failure_count=(failure_count + 1)
                    )
                else:
                    # problem refreshing token
                    return ret
            else:
                # unauthenticated
                return ret
    
    def get_oauth2_login_url(self, table_uuid):
        state = {
            'table_uuid': table_uuid
        }
        state = base64.b64encode(pickle.dumps(state)).decode()
        return "{oauth2_endpoint}?response_type=code&client_id={client_id}&state={state}&redirect_uri={redirect_uri}".format(
            oauth2_endpoint=self.config['oauth2']['oauth2_endpoint'],
            client_id=self.config['oauth2']['client_id'],
            state=state,
            redirect_uri=self.config['oauth2']['redirect_uri']
        )
    
    def process_oauth2_response(self, code, state):
        parameters = {
            'grant_type': "authorization_code",
            'client_secret': self.config['oauth2']['client_secret'],
            'client_id': self.config['oauth2']['client_id'],
            'redirect_uri': self.config['oauth2']['redirect_uri'],
            'code': code
        }
        ret = {
			'success': False,
			'table_uuid': "",
			'messages': []
		}
        result = self._send_request(
            method='POST', 
            url=self.config['oauth2']['token_endpoint'],
            parameters=parameters
        )
        if result['status_code'] == 200:
            auth_response = result['data']
            self.set_token(auth_response['access_token'], "auth", datetime.now() + timedelta(hours=1))
            self.set_token(auth_response['refresh_token'], "refresh")
            state = pickle.loads(base64.b64decode(state))
            ret['success'] = True
            ret['table_uuid'] = state['table_uuid']
        else:
            # problem
            ret['messages'].append((result['raw'], "danger"))
        return ret
    
    def refresh_oauth2_token(self):
        parameters = {
            'grant_type': "refresh_token",
            'client_id': self.config['oauth2']['client_id'],
            'client_secret': self.config['oauth2']['client_secret'],
            'refresh_token': self.get_token("refresh")
        }
        ret = {
            'success': False,
            'messages': [],
            'status_code': 0
        }
        result = self._send_request(
            method='POST', 
            url=self.config['oauth2']['token_endpoint'],
            parameters=parameters
        )
        if result and result['status_code'] == 200:
            auth_response = result['data']
            self.set_token(auth_response['access_token'], "auth", datetime.now() + timedelta(hours=1))
            ret['success'] = True
        elif result:
            # problem
            ret['messages'].append((result['raw'], "danger"))
        else:
            # big problem
            pass
        return ret
    