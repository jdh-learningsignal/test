from flask import g, request, url_for
from datetime import datetime
import json
from bson import ObjectId
import logging
from natsort import natsorted, ns

from sres.db import _get_db
from sres.auth import get_auth_user, get_auth_user_oid, is_user_administrator
from sres import utils
from sres.tables import format_full_name as format_full_table_name, list_authorised_tables, Table
from sres.anonymiser import anonymise

def search_haystacks(term, haystack_types=None):
    """Searches for assets and other SRES elements (defined by haystack_types) for the provided term."""
    
    ret = {
        'success': False,
        'results': []
    }
    
    db = _get_db()
    user_oid = get_auth_user_oid()
    is_superadmin = is_user_administrator('super')
    
    if 'columns' in haystack_types or 'identifiers' in haystack_types or 'student_data' in haystack_types:
        if 'student_data' in haystack_types:
            authorised_tables = list_authorised_tables(
                show_archived=True,
                ignore_authorisation_state=False,
                show_deleted=True
            )
        else:
            authorised_tables = list_authorised_tables(
                show_archived=True,
                ignore_authorisation_state=is_superadmin,
                show_deleted=True
            )
        authorised_tables_uuids = list(set([ t['uuid'] for t in authorised_tables ]))
        authorised_tables_keyed = { t['uuid']:t for t in authorised_tables }
    
    for haystack_type in haystack_types:
        db_filters = []
        if haystack_type == 'tables':
            db_filters.append({'name': {'$regex': term, '$options': 'i'}})
            db_filters.append({'code': {'$regex': term, '$options': 'i'}})
            if is_superadmin:
                results = list(db.tables.find(
                    {
                        '$or': db_filters
                    }
                ))
            else:
                results = list(db.tables.find(
                    {
                        'workflow_state': { '$ne': 'deleted' },
                        'staff.administrators': user_oid,
                        '$or': db_filters
                    }
                ))
            for result in results:
                ret['results'].append({
                    'haystack_type': 'tables',
                    'display': format_full_table_name(result)['complete'],
                    'links': [
                        {
                            'target': 'table',
                            'action': 'view',
                            'url': url_for('table.view_table', table_uuid=result['uuid']),
                            'display': "View list"
                        }
                    ],
                    'asset_uuid': result['uuid'],
                    'related_asset_uuid': None,
                    'workflow_state': result['workflow_state']
                })
        elif haystack_type == 'columns':
            db_filters.append({'name': {'$regex': term, '$options': 'i'}})
            db_filters.append({'description': {'$regex': term, '$options': 'i'}})
            db_filters.append({'multi_entry.options.label': {'$regex': term, '$options': 'i'}})
            db_filters.append({'multi_entry.options.select.display': {'$regex': term, '$options': 'i'}})
            db_filters.append({'multi_entry.options.select.description': {'$regex': term, '$options': 'i'}})
            db_filters.append({'multi_entry.options.select.value': {'$regex': term, '$options': 'i'}})
            db_filters.append({'quick_info.single': {'$regex': term, '$options': 'i'}})
            db_filters.append({'quick_info.bulk': {'$regex': term, '$options': 'i'}})
            db_filters.append({'custom_options.quickinfo_rollview': {'$regex': term, '$options': 'i'}})
            if is_superadmin:
                results = list(db.columns.find(
                    {
                        'workflow_state': { '$ne': 'collective' },
                        '$or': db_filters
                    }
                ))
            else:
                results = list(db.columns.find(
                    {
                        'workflow_state': { '$nin': ['collective', 'deleted'] },
                        'table_uuid': { '$in': authorised_tables_uuids },
                        '$or': db_filters
                    }
                ))
            for result in results:
                ret['results'].append({
                    'haystack_type': 'columns',
                    'display': result['name'],
                    'display_subs': [
                        f"In {format_full_table_name(authorised_tables_keyed[result['table_uuid']])['complete']}"
                    ],
                    'links': [
                        {
                            'target': 'column',
                            'action': 'edit',
                            'url': url_for('table.edit_column', table_uuid=result['table_uuid'], column_uuid=result['uuid']),
                            'display': "Edit column"
                        },
                        {
                            'target': 'table',
                            'action': 'view',
                            'url': url_for('table.view_table', table_uuid=result['table_uuid']),
                            'display': "View list"
                        }
                    ],
                    'asset_uuid': result['uuid'],
                    'related_asset_uuid': result['table_uuid'],
                    'workflow_state': result['workflow_state'],
                    'config': json.dumps(result, default=str)
                })
        elif haystack_type == 'filters':
            db_filters.append({'name': {'$regex': term, '$options': 'i'}})
            db_filters.append({'description': {'$regex': term, '$options': 'i'}})
            db_filters.append({'email.sections.content': {'$regex': term, '$options': 'i'}})
            db_filters.append({'email.subject': {'$regex': term, '$options': 'i'}})
            db_filters.append({'email.body_first': {'$regex': term, '$options': 'i'}})
            db_filters.append({'email.body_last': {'$regex': term, '$options': 'i'}})
            if is_superadmin:
                results = list(db.filters.find(
                    {
                        'workflow_state': { '$ne': 'collective' },
                        '$or': db_filters
                    }
                ))
            else:
                results = list(db.filters.find(
                    {
                        'workflow_state': { '$nin': ['collective', 'deleted'] },
                        'administrators': user_oid,
                        '$or': db_filters
                    }
                ))
            for result in results:
                ret['results'].append({
                    'haystack_type': 'filters',
                    'display': result['name'],
                    'links': [
                        {
                            'target': 'filter',
                            'action': 'edit',
                            'url': url_for('filter.edit_filter', filter_uuid=result['uuid']),
                            'display': "Edit filter"
                        },
                        {
                            'target': 'filter',
                            'action': 'preview',
                            'url': url_for('filter.preview_filter', filter_uuid=result['uuid']),
                            'display': "Preview filter"
                        }
                    ],
                    'asset_uuid': result['uuid'],
                    'related_asset_uuid': None,
                    'workflow_state': result['workflow_state'],
                    'config': json.dumps(result, default=str)
                })
        elif haystack_type == 'portals':
            db_filters.append({'name': {'$regex': term, '$options': 'i'}})
            db_filters.append({'description': {'$regex': term, '$options': 'i'}})
            db_filters.append({'panels.content': {'$regex': term, '$options': 'i'}})
            if is_superadmin:
                results = list(db.portals.find(
                    {
                        'workflow_state': { '$ne': 'collective' },
                        '$or': db_filters
                    }
                ))
            else:
                results = list(db.portals.find(
                    {
                        'workflow_state': { '$nin': ['collective', 'deleted'] },
                        'administrators': user_oid,
                        '$or': db_filters
                    }
                ))
            for result in results:
                ret['results'].append({
                    'haystack_type': 'portals',
                    'display': result['name'],
                    'links': [
                        {
                            'target': 'portal',
                            'action': 'edit',
                            'url': url_for('portal.edit_portal', portal_uuid=result['uuid']),
                            'display': "Edit portal"
                        },
                        {
                            'target': 'portal',
                            'action': 'preview',
                            'url': url_for('portal.view_portal', portal_uuid=result['uuid'], preview=1),
                            'display': "Preview portal"
                        }
                    ],
                    'asset_uuid': result['uuid'],
                    'related_asset_uuid': None,
                    'workflow_state': result['workflow_state'],
                    'config': json.dumps(result, default=str)
                })
        elif haystack_type == 'identifiers':
            db_filters.append({'sid': {'$regex': term, '$options': 'i'}})
            db_filters.append({'given_names': {'$regex': term, '$options': 'i'}})
            db_filters.append({'preferred_name': {'$regex': term, '$options': 'i'}})
            db_filters.append({'surname': {'$regex': term, '$options': 'i'}})
            db_filters.append({'email': {'$regex': term, '$options': 'i'}})
            db_filters.append({'alternative_id1': {'$regex': term, '$options': 'i'}})
            db_filters.append({'alternative_id2': {'$regex': term, '$options': 'i'}})
            if is_superadmin:
                results = list(db.data.find(
                    {
                        '$or': db_filters
                    }
                ))
            else:
                results = list(db.data.find(
                    {
                        'table_uuid': { '$in': authorised_tables_uuids },
                        '$or': db_filters
                    }
                ))
            for result in results:
                _result = {
                    'haystack_type': 'identifiers',
                    'display': f"{result.get('preferred_name')} {result.get('surname')}",
                    'display_subs': [
                        f"{result.get('sid', 'Unknown SID')} {result.get('email', 'Unknown email')}",
                        f"In {format_full_table_name(authorised_tables_keyed[result['table_uuid']])['complete']}"
                    ],
                    'links': [
                        {
                            'target': 'table',
                            'action': 'view',
                            'url': url_for('table.view_table', table_uuid=result['table_uuid']),
                            'display': "View list"
                        },
                        {
                            'target': 'entry',
                            'action': 'single',
                            'url': url_for('entry.view_single_student', table_uuid=result['table_uuid'], mode='view', identifier=result.get('sid')),
                            'display': "View single student"
                        }
                    ],
                    'asset_uuid': result['_id'],
                    'related_asset_uuid': None,
                    'workflow_state': result['status']
                }
                ret['results'].append(_result)
        elif haystack_type == 'student_data':
            for table_uuid, table_meta in authorised_tables_keyed.items():
                table = Table()
                if table.load(table_uuid) and table.is_user_authorised():
                    available_columns_uuids = table.get_available_columns(uuids_only=True)
                    if len(available_columns_uuids) == 0:
                        continue
                    db_filter = {
                        'table_uuid': table_uuid,
                        '$or': [ {column_uuid: {'$regex': term, '$options': 'i'}} for column_uuid in available_columns_uuids ]
                    }
                    results = list(db.data.find(db_filter))
                    for result in results:
                        _result = {
                            'haystack_type': 'student_data',
                            'display': f"{result.get('preferred_name')} {result.get('surname')}",
                            'display_subs': [
                                f"{result.get('sid', 'Unknown SID')} {result.get('email', 'Unknown email')}",
                                f"In {format_full_table_name(authorised_tables_keyed[result['table_uuid']])['complete']}"
                            ],
                            'links': [
                                {
                                    'target': 'table',
                                    'action': 'view',
                                    'url': url_for('table.view_table', table_uuid=result['table_uuid']),
                                    'display': "View list"
                                },
                                {
                                    'target': 'entry',
                                    'action': 'single',
                                    'url': url_for('entry.view_single_student', table_uuid=result['table_uuid'], mode='view', identifier=result.get('sid')),
                                    'display': "View single student"
                                }
                            ],
                            'asset_uuid': result['_id'],
                            'related_asset_uuid': result['table_uuid'],
                            'related_asset_type': 'table',
                            'workflow_state': result['status'],
                            'config': json.dumps(result, default=str)
                        }
                        ret['results'].append(_result)
        
        for i, result in enumerate(ret['results']):
            if 'config' in result.keys():
                pos = result['config'].lower().find(term.lower())
                start = 0 if pos < 10 else pos - 10
                end = pos + len(term) + 30
                search_result_preview = "..." + result['config'][start:end] + "..."
                ret['results'][i]['search_result_preview'] = search_result_preview
    return ret

def _load_students_by_table_uuids(table_uuids, restrictor_column_uuid=None):
    """Loads students from the provided table uuid(s).
        Returns a dict (keyed by sid) of dicts of students.
        Rechecks permission on each table queried.
        
        table_uuids (list of str)
    """
    # get all students
    _students = []
    for table_uuid in table_uuids:
        table = Table()
        if table.load(table_uuid) and table.is_user_authorised(categories=['administrator', 'user', 'auditor']):
            _students.extend(table.load_all_students(get_email=True, restrict_by_username_column=restrictor_column_uuid))
    # parse _students
    ret = {}
    for _student in _students:
        if _student.get('sid'):
            if _student['sid'] not in ret.keys():
                ret[_student['sid']] = _student
    return ret

def find_students_from_tables_by_term(search_term, table_uuids, anonymise_identities=False, return_email=False, restrictor_column_uuid=None):
    """Searches the table(s) (defined by provided tableuuid(s)) for students who match
        the search term.
        Will recheck permission on each table queried.
        
        search_term (str)
        table_uuids (list of str)
        anonymise_identities (boolean)
        return_email (boolean)
        restrictor_column_uuid (str or None)
        
        Returns a list of dicts.
    """
    students = _load_students_by_table_uuids(table_uuids, restrictor_column_uuid=restrictor_column_uuid)
    return find_students_by_term(search_term, students, anonymise_identities, return_email)

def find_students_by_term(search_term, students, anonymise_identities=False, return_email=False):
    """Searches a provided dict of dicts (students) for the search term.
        Returns a list of dicts that match the search term.
    """
    found_students = []
    if search_term == '':
        for sid, student in students.items():
            found_students.append(student)
    else:
        for sid, student in students.items():
            if (search_term.lower() in student['sid'].lower() or 
                    search_term.lower() in student['preferred_name'].lower() or 
                    search_term.lower() in student['surname'].lower() or 
                    search_term.lower() in student['email'].lower()):
                found_students.append(student)
    if not return_email:
        # remove email address from returned result
        for i, student in enumerate(found_students):
            found_students[i].pop('email', None)
    # add a display_sid
    for i, student in enumerate(found_students):
        found_students[i]['display_sid'] = student['sid']
    # sort
    found_students = natsorted(found_students, key=lambda i: i['surname'], alg=ns.IGNORECASE)
    # anonymise if necessary
    if anonymise_identities:
        for i, s in enumerate(found_students):
            for k in ['display_sid', 'preferred_name', 'surname']:
                found_students[i][k] = anonymise(k, s[k])
    # return
    return found_students

