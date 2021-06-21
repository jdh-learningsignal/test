from flask import g, escape, url_for, current_app, request, Markup, session
from copy import deepcopy
import re
from bs4 import BeautifulSoup
import json
from natsort import natsorted, ns
from datetime import datetime
import pprint
import random
import base64
import os
from pymongo import MongoClient
from bson import ObjectId
import logging
import html
from multiprocessing import Process, active_children, JoinableQueue
from threading import Thread
import concurrent.futures
from urllib import parse

from sres.db import _get_db, DbCookie
from sres.columns import Column, SYSTEM_COLUMNS, MAGIC_FORMATTERS_LIST, get_friendly_column_name
from sres.auth import get_auth_user
from sres import utils
from sres.files import get_file_access_url, GridFile
from sres import change_history
from sres.anonymiser import anonymise, is_identity_anonymiser_active
from sres.summaries import Summary, Representation

NAME_FIELDS = [
    'preferred_name',
    'given_names',
    'surname'
]

IDENTIFIER_FIELDS = [
    'sid', 
    'email', 
    'username', 
    'alternative_id1', 
    'alternative_id2'
]

REQUIRED_BASE_FIELDS = [
    'preferred_name',
    'given_names',
    'surname',
    'sid',
    'email'
]

NON_DATA_FIELDS = NAME_FIELDS + IDENTIFIER_FIELDS + ['status']

GENERAL_FIELDS = {
    'TIMESTAMP': {
        'format': '%Y-%m-%d %H:%M:%S',
        'label': 'Timestamp',
        'field': 'TIMESTAMP',
        'hint': 'YYYY-MM-DD HH:MM:SS'
    },
    'TIME': {
        'format': '%H:%M:%S',
        'label': 'Time (24 hr)',
        'field': 'TIME',
        'hint': 'HH:MM:SS'
    },
    'DATE': {
        'format': '%Y-%m-%d',
        'label': 'Date',
        'field': 'DATE',
        'hint': 'YYYY-MM-DD'
    },
    'LONGDATE': {
        'format': '%-d %B %Y',
        'label': 'Long date',
        'field': 'LONGDATE',
        'hint': 'Day Month Year'
    }
}

STUDENT_DATA_DEFAULT_CONFIG = {
    'sid': None,
    'email': None, 
    'username': None, 
    'alternative_id1': None, 
    'alternative_id2': None,
    'preferred_name': None,
    'given_names': None,
    'surname': None,
    'status': None
}

def _preload_columns(input_text, default_table_uuid, enclosing_char='$'):
    """Preloads Column instances according to column references found in input_text.
        Returns a dict of Column instances, keyed by column reference
    """
    # restore any encoded enclosing chars
    input_text = input_text.replace(enclosing_char, '$')
    # find column references
    if enclosing_char:
        column_references = re.findall("(?<=\$)[A-Z0-9a-z_\.]+(?=\$)", input_text)
    else:
        column_references = re.findall("(?<=\$?)[A-Z0-9a-z_\.]+(?=\$?)", input_text)
    ret = {}
    for column_reference in column_references:
        if column_reference in ret.keys():
            # skip
            pass
        else:
            ret[column_reference] = Column()
            if not ret[column_reference].load(column_reference=column_reference, default_table_uuid=default_table_uuid):
                del ret[column_reference]
    return ret

def substitute_text_variables(input, identifier, default_table_uuid='', enclosing_char='$', do_not_encode=False, preloaded_student_data=None, preloaded_columns=None, blank_replacement='', preloaded_student_data_instances=None, only_process_global_magic_formatters=False):
    """Replaces column references ('variables' in the supplied text) with their actual values.
        
        Args:
            input (str): The text containing column references to be replaced, amongst other text.
            identifier (str)
            default_table_uuid (str): uuid
            enclosing_char (str): The character prefix and suffix for column references.
                If an empty string, then most words will look like a column reference so only use this
                in certain circumstances.
            do_not_encode (bool): If True, will not escape or get_text() on the returned text.
            preloaded_student_data (StudentData): A loaded instance of StudentData, used for faster access to data.
                A student's records must have been loaded already.
            preloaded_columns (dict): dict of Column instances, keyed by column reference.
            blank_replacement (any): Value to use if data cannot be found.
            preloaded_student_data_instances (None or dict): Keyed by table_uuid, a dict of preloaded StudentData instances.
                A student's record must have been loaded already in each of these instances.
            only_process_global_magic_formatters (bool): Whether to only process the global magic formatters.
        
        Returns:
            dict {
                new_text (str): Substituted text
                scripts (str): Any javascript needed for rendering
            }
    """
    
    return_text = input
    return_scripts = ''
    
    if preloaded_student_data_instances is None:
        preloaded_student_data_instances = {}
    if len(preloaded_student_data_instances) == 0 and preloaded_student_data is not None:
        preloaded_student_data_instances[ preloaded_student_data.table.config['uuid'] ] = preloaded_student_data
    
    # restore any encoded enclosing chars
    return_text = return_text.replace(enclosing_char, '$')
    if preloaded_columns is None:
        preloaded_columns = {}
    
    identity_anonymiser_active = is_identity_anonymiser_active()
    
    ### preloaded student_data loading method
    def _load_student_data_from_preload(identifier, column, preloaded_student_data_instances):
        """Loads a StudentData instance from preloaded_student_data_instances
            
            Args:
                identifier (str): identifier for the student
                column (Column): loaded Column instance
                preloaded_student_data_instances (dict): Keyed by table_uuid
        """
        if column.table.config['uuid'] in preloaded_student_data_instances.keys():
            student_data = preloaded_student_data_instances[ column.table.config['uuid'] ]
        else:
            student_data = StudentData(column.table)
            student_data.find_student(identifier)
            if student_data._id:
                preloaded_student_data_instances[ column.table.config['uuid'] ] = student_data
        return student_data
    
    ### column reference finding method
    def _find_column_references(enclosing_char, text):
        if enclosing_char:
            _refs = re.findall("(?:\$)[A-Z0-9a-z_\.]+(?:\$)", text)
        else:
            _refs = re.findall("(?:\$?)[A-Z0-9a-z_\.]+(?:\$?)", text)
        return [ r for r in _refs if not r.startswith('$SMY_') ]
    
    ### summary reference finding method
    def _find_summary_references(text):
         return re.findall(utils.DELIMITED_SUMMARY_REFERENCE_PATTERN, text)
    
    ### data getting
    def _get_replacement_data(column, student_data, allow_html, do_not_encode, substitution_aggregation_mode='latest', blank_replacement='', append_auth_user_mode=None):
        """Loads up the data in the specified column and student_data.
            Returns list of strings.
            
            column (Column)
            student_data (StudentData)
            allow_html (bool)
            do_not_encode (bool)
            substitution_aggregation_mode (str) latest|earliest|user_latest|user_earliest|all|given_latest|given_earliest|given_user_latest|given_user_earliest|given_all
            blank_replacement (any) Pass-through from parent method
            append_auth_user_mode (None|str) username|full_name|given_names|surname|email
        """
        replacement_data = []
        replacement_data_metadata = []
        return_scripts = ''
        # load up data
        if substitution_aggregation_mode == 'latest':
            if column.subfield is not None:
                # is a multientry column
                data_temp = student_data.get_data(column_uuid=column.config['uuid'], preloaded_column=column)
                if data_temp['success']:
                    if column.subfield < len(data_temp['data']):
                        replacement_data = [data_temp['data'][column.subfield]]
                    else:
                        # problem; required index is beyond what is available in data
                        replacement_data = [blank_replacement]
                else:
                    replacement_data = [blank_replacement]
            else:
                # not a multientry column
                data_temp = student_data.get_data(column_uuid=column.config['uuid'], preloaded_column=column)
                if data_temp['success']:
                    replacement_data = [data_temp['data']]
                else:
                    replacement_data = [blank_replacement]
        else:
            replacement_data = []
            # get change history to render
            if substitution_aggregation_mode.startswith('given_'):
                # a bit different here - we need to fetch the change history records that were GIVEN or SAVED
                # by this student
                ch = change_history.get_change_history(
                    column_uuids=[column.config['uuid']],
                    max_rows=0,
                    auth_users=[student_data.config['username']], 
                    sid=None, 
                    email=None
                )
                sid_to_username = column.table.get_sid_username_map()['sid_to_username']
                if len(ch):
                    if substitution_aggregation_mode == 'given_latest':
                        replacement_data = [ch[0].get('new_value', '')]
                        replacement_data_metadata = [ sid_to_username.get(ch[0].get('identifier')) ]
                    elif substitution_aggregation_mode == 'given_earliest':
                        replacement_data = [ch[-1].get('new_value', '')]
                        replacement_data_metadata = [ sid_to_username.get(ch[-1].get('identifier')) ]
                    elif substitution_aggregation_mode in ['given_user_earliest', 'given_user_latest']:
                        receivers = []
                        if substitution_aggregation_mode == 'given_user_earliest':
                            ch_iter = reversed(ch)
                        else:
                            ch_iter = ch
                        for r in ch_iter:
                            if r.get('identifier') not in receivers:
                                replacement_data.append(r.get('new_value'))
                                replacement_data_metadata.append(sid_to_username.get(r.get('identifier')))
                                receivers.append(r.get('identifier'))
                    elif substitution_aggregation_mode == 'given_all':
                        replacement_data = [ e.get('new_value', '') for e in ch ]
                        replacement_data_metadata = [ sid_to_username.get(e.get('identifier')) for e in ch ]
            else:
                ch = student_data.get_change_history(column_uuids=[column.config['uuid']])
                if len(ch):
                    if substitution_aggregation_mode == 'earliest':
                        replacement_data = [ch[-1].get('new_value', '')]
                        replacement_data_metadata = [ch[-1].get('auth_user')]
                    elif substitution_aggregation_mode in ['user_earliest', 'user_latest']:
                        users = []
                        if substitution_aggregation_mode == 'user_earliest':
                            ch_iter = reversed(ch)
                        else:
                            ch_iter = ch
                        for r in ch_iter:
                            if r.get('auth_user') not in users:
                                replacement_data.append(r.get('new_value'))
                                replacement_data_metadata.append(r.get('auth_user'))
                                users.append(r.get('auth_user'))
                    elif substitution_aggregation_mode == 'all':
                        replacement_data = [ e.get('new_value', '') for e in ch ]
                        replacement_data_metadata = [ e.get('auth_user') for e in ch ]
            # expand subfields if needed
            if column.subfield is not None and len(replacement_data):
                _replacement_data = []
                for d in replacement_data:
                    try:
                        _replacement_data.append(json.loads(d)[column.subfield])
                    except:
                        _replacement_data.append('')
                replacement_data = _replacement_data
        # escape replacement_data if necessary
        if column.config['type'] == 'multiEntry' and column.subfield_type == 'audio-recording':
            pass
        if column.config['type'] == 'multiEntry' and column.subfield_type == 'html-simple':
            pass
        elif column.config['type'] == 'file':
            pass
        else:
            replacement_data = _escape_substituted_text(replacement_data, do_not_encode, allow_html)
        # transform and present loaded data
        if not isinstance(replacement_data, list):
            replacement_data = [replacement_data]
        # convert blanks if necessary
        if blank_replacement != '':
            replacement_data = [ blank_replacement if (r == '' or r is None) else r for r in replacement_data ]
        # replace and format
        replacement_data_formatted = []
        if column.magic_formatter == 'display' or column.magic_formatter == 'description':
            if column.config['type'] == 'multiEntry' and column.subfield is not None:
                if replacement_data:
                    # flatten if needed
                    replacement_data, _topology = utils.flatten_list(replacement_data)
                    for replacement_data_item in replacement_data:
                        # format
                        key = 'display' if not column.magic_formatter else column.magic_formatter
                        replacement_data_formatted_part = None
                        for _select_item in column.config['multi_entry']['options'][column.subfield]['select']:
                            #if _escape_substituted_text(str(_select_item['value']), do_not_encode, allow_html) == str(replacement_data_item):
                            #    replacement_data_formatted_part = _select_item[key]
                            #    break
                            if str(_select_item['value']) == Markup.unescape(str(replacement_data_item)):
                                replacement_data_formatted_part = _select_item[key]
                                break
                        if replacement_data_formatted_part is None and column.config['multi_entry']['options'][column.subfield].get('range_mode') is not None:
                            # try to use range to figure out where the value lies in the spectrum
                            _range_mode = column.config['multi_entry']['options'][column.subfield].get('range_mode')
                            _select_items = column.config['multi_entry']['options'][column.subfield]['select']
                            for i, select_item in enumerate(_select_items):
                                replacement_data_formatted_part = None
                                try:
                                    _challenge_value = float(replacement_data_item)
                                    _current_item_value = float(select_item['value'])
                                    try:
                                        _next_item_value = float(_select_items[i+1]['value'])
                                        if (_current_item_value < _challenge_value < _next_item_value) or (_current_item_value > _challenge_value > _next_item_value):
                                            if _range_mode == 'roundup':
                                                if _current_item_value > _next_item_value:
                                                    replacement_data_formatted_part = _select_items[i][key]
                                                else:
                                                    replacement_data_formatted_part = _select_items[i+1][key]
                                            else: # _range_mode == 'rounddown':
                                                if _current_item_value > _next_item_value:
                                                    replacement_data_formatted_part = _select_items[i+1][key]
                                                else:
                                                    replacement_data_formatted_part = _select_items[i][key]
                                        else:
                                            pass
                                    except:
                                        pass
                                except:
                                    pass
                                if replacement_data_formatted_part is not None:
                                    break
                        if replacement_data_formatted_part:
                            replacement_data_formatted.append(replacement_data_formatted_part)
            elif column.config['type'] == 'multiEntry' and column.subfield is None:
                replacement_data_formatted.append(blank_replacement)
            elif column.config['type'] == 'mark':
                for replacement_data_item in replacement_data:
                    # load if necessary
                    if utils.is_json(replacement_data_item):
                        replacement_data_item = json.loads(replacement_data_item)
                    # format
                    key = 'display' if not column.magic_formatter else column.magic_formatter
                    replacement_data_formatted_part = next(
                        (list_option[key] 
                        for list_option in column.config['simple_input']['options']
                        if str(list_option['value']) == str(replacement_data_item)),
                        None
                    )
                    if replacement_data_formatted_part:
                        replacement_data_formatted.append(replacement_data_formatted_part)
        elif column.magic_formatter == 'image' and (column.config['type'] == 'image' or column.config['type'] == 'imgurl'):
            # see if dimensions are set
            _style = ''
            try:
                _dim = column.column_reference.split('.')[column.column_reference.split('.').index('image') + 1]
                if _dim:
                    if utils.is_number(_dim):
                        _style = 'style="min-width:{d}px; min-height:{d}px; max-width:{d}px; max-height:{d}px;"'.format(d=_dim)
                    else:
                        _dim = re.sub('[^A-Z0-9a-z\%]', '', _dim)
                        if _dim.startswith('w') or _dim.startswith('h'):
                            _axis = 'height' if _dim.startswith('h') else 'width'
                            _dim = _dim[1:]
                            if utils.is_number(_dim):
                                _style = 'style="{a}: {d}px;"'.format(a=_axis, d=_dim)
                            else:
                                _style = 'style="{a}: {d};"'.format(a=_axis, d=_dim)
            except:
                pass
            # return the img tag
            if column.config['type'] == 'image':
                replacement_data_formatted = [ '<img src="{}" {}>'.format(get_file_access_url(d, full_path=True), _style) for d in replacement_data ]
            elif column.magic_formatter == 'image' and column.config['type'] == 'imgurl':
                replacement_data_formatted = [ '<img src="{}" {}>'.format(d, _style) for d in replacement_data ]
        elif column.magic_formatter == 'audio_player':
            if column.config['type'] == 'multiEntry': # technically only possible for a particular subfield type
                replacement_data, _topology = utils.flatten_list(replacement_data)
                replacement_data_formatted = [ '<audio controls src="{}"></audio>'.format(get_file_access_url(fn, full_path=True)) for fn in replacement_data ]
            else:
                replacement_data_formatted = str(replacement_data)
        elif column.magic_formatter in ['file_download_links', 'file_download_links_bullets'] and column.config['type'] == 'file':
            # load up the file list
            if replacement_data and utils.is_json(replacement_data):
                files = json.loads(replacement_data)
            elif type(replacement_data) is list:
                files = replacement_data
            else:
                files = None
            # format the file list
            if files:
                # flatten it first
                files, _topology = utils.flatten_list(files)
                download_links = []
                # render the download links
                for file in files:
                    if type(file) is dict:
                        saved_filename = file.get('saved_filename')
                        original_filename = file.get('original_filename', saved_filename)
                        url = file.get('url')
                        download_links.append(f'<a href="{url}">{original_filename}</a>')
                    else:
                        download_links.append("No file(s) are available") # Or should we use blank_replacement ??
                replacement_data_formatted = download_links
        elif column.magic_formatter in ['join_space', 'join_bullets', 'join_paragraphs']:
            replacement_data_formatted = [ utils.force_interpret_str_to_list(d, sort_list=False) for d in replacement_data ]
        elif column.magic_formatter == 'tabulate_reports':
            # grab all reports
            _all_reports_data = student_data.get_data_for_entry(column)['all_reports_data_keyed']
            _table_id = 'tabulated_reports_' + utils.create_uuid()
            replacement_data_formatted = f"""<table class="table" id="{_table_id}">"""
            # make table header
            _headers = ['Report #']
            if column.config['type'] == 'multiEntry':
                _headers.extend(column.get_multientry_labels(get_text_only=True))
            else:
                _headers.append(get_friendly_column_name(show_table_info=False, get_text_only=True, table=column.table, column=column))
            replacement_data_formatted += """<thead><tr>"""
            for _header in _headers:
                replacement_data_formatted += f"""<th>{_header}</th>"""
            replacement_data_formatted += """</tr></thead>"""
            # make table rows
            _rows = []
            replacement_data_formatted += """<tbody>"""
            _multientry_options = column.config['multi_entry'].get('options', [])
            for _report_number, _report_data in _all_reports_data.items():
                replacement_data_formatted += """<tr>"""
                for _a, _header in enumerate(_headers):
                    if _a == 0:
                        replacement_data_formatted += f"""<td>{_report_number}</td>"""
                    elif _a > len(_report_data):
                        replacement_data_formatted += f"""<td></td>"""
                    else:
                        _cell = _report_data[_a - 1]
                        _td_html = ''
                        if _a < len(_multientry_options):
                            _subfield_type = _multientry_options[_a - 1].get('type')
                            if _subfield_type == 'audio-recording':
                                _td_html += f"""<td>
                                    <div class="sres-audio-recording-recordings-container mt-2" data-sres-field="{_subfield_type}" data-sres-columnuuid="{column.config['uuid']}"
                                        data-sres-saved-recordings="{escape(json.dumps(_cell))}" readonly>
                                    </div></td>
                                """
                            elif _subfield_type == 'sketch-small':
                                _td_html += f"""<td>
                                    <input type="hidden" data-sres-field="sketch-small" data-sres-columnuuid="{column.config['uuid']}" value="{_cell}" readonly>
                                    <div class="sres-sketch-container" style="width:100%;">
                                        <canvas id="sres_sketch_{utils.create_uuid()}" height="100" width="300" class="sres-sketch-area sres-sketch-small">
                                    </div></td>
                                """
                        if len(_td_html) == 0:
                            if len(str(_cell)) > 200:
                                _td_html += f"""<td><div class="sres-multiple-reports-table-td-wrap">{_cell}</div></td>"""
                            else:
                                _td_html += f"""<td><div>{_cell}</div></td>"""
                        replacement_data_formatted += _td_html
                replacement_data_formatted += """</tr>"""
            replacement_data_formatted += """</tbody>"""
            # finish table
            replacement_data_formatted += """</table>"""
            # load up datatable
            return_scripts += """
            <script>$(document).ready(function(){
                oTable_""" + _table_id + """ = $('#""" + _table_id + """').DataTable({
                    dom: "<'row'<'col-sm-4'l><'col-sm-4'B><'col-sm-4'f>><'row'<'col-sm-12't>><'row'<'col-sm-6'i><'col-sm-6'p>>",
                    scrollX: true,
                    fixedColumns: true,
                    language: {
                        lengthMenu: "Show _MENU_ reports",
                        info: "Showing _START_ to _END_ of _TOTAL_ reports"
                    },
                    buttons: [
                        /*{
                            extend: 'print',
                            autoPrint: false,
                            customize: function(win){
                                let table = $(win.document.body).find('table');
                                table.prepend('message');
                            }
                        },*/
                        {
                            extend: 'excelHtml5',
                            text: '<span class="fa fa-download"></span> Excel',
                            title: """ + json.dumps(column.get_friendly_name(show_table_info=True, get_text_only=True) + ' - ' + student_data.config.get('sid', '')) + """
                        },
                        {
                            extend: 'excelHtml5',
                            text: '<span class="fa fa-download"></span> Page as Excel',
                            title: """ + json.dumps(column.get_friendly_name(show_table_info=True, get_text_only=True) + ' - ' + student_data.config.get('sid', '')) + """,
                            customize: function(xlsx) {
                                /** quickinfo dumping **/
                                let sheet = xlsx.xl.worksheets['sheet1.xml'];
                                let quickInfoContainer = $('.sres-quickinfo-container');
                                let quickInfoExceptTable = $('.sres-quickinfo-container :not(script):not(style):not(#""" + _table_id + """_wrapper, #""" + _table_id + """_wrapper *)');
                                let textRows = [];
                                for (let e = 0; e < quickInfoExceptTable.length; e++) {
                                    if (e > 0) {
                                        if (quickInfoExceptTable[e - 1].contains(quickInfoExceptTable[e])) {
                                            continue;
                                        }
                                    }
                                    textRows.push( quickInfoExceptTable[e].innerText );
                                }
                                $('c[r=A1] t', sheet).text(textRows.join('\\n'));
                                $('c[r=A1]', sheet).attr('s', 50);
                                /** data entry dumping **/
                                // make a new sheet, from https://codepen.io/RedJokingInn/pen/pVKWjz
                                let source = xlsx['[Content_Types].xml'].getElementsByTagName('Override')[1];
                                let clone = source.cloneNode(true);
                                clone.setAttribute('PartName','/xl/worksheets/sheet2.xml');
                                xlsx['[Content_Types].xml'].getElementsByTagName('Types')[0].appendChild(clone);
                                source = xlsx.xl._rels['workbook.xml.rels'].getElementsByTagName('Relationship')[0];
                                clone = source.cloneNode(true);
                                clone.setAttribute('Id','rId3');
                                clone.setAttribute('Target','worksheets/sheet2.xml');
                                xlsx.xl._rels['workbook.xml.rels'].getElementsByTagName('Relationships')[0].appendChild(clone);
                                source = xlsx.xl['workbook.xml'].getElementsByTagName('sheet')[0];
                                clone = source.cloneNode(true);
                                clone.setAttribute('name','Info');
                                clone.setAttribute('sheetId','2');
                                clone.setAttribute('r:id','rId3');
                                xlsx.xl['workbook.xml'].getElementsByTagName('sheets')[0].appendChild(clone);
                                let newSheet = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
                                    '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:mc="http://schemas.openxmlformats.org/markup-compatibility/2006" xmlns:x14ac="http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac" mc:Ignorable="x14ac">' +
                                    '<cols >' +
                                      '<col min="1" max="1" width="24.7" customWidth="1"/>' +
                                      '<col min="2" max="2" width="37.7" customWidth="1"/>' +
                                    '</cols>' +
                                    '<sheetData>';
                                // add data to new sheet
                                let rowCount = oTable_""" + _table_id + """.rows().count();
                                let dataFields = $('#set_data_container').find("[data-sres-field]");
                                let tmpData = collectMultientryData(dataFields);
                                for (let f = 0; f < dataFields.length; f++) {
                                    let label = $(dataFields[f]).parent().find('.sres-field-label').text();
                                    let data = tmpData[f];
                                    newSheet +='<row r="' + (f+1) + '">' + 
                                        '<c t="inlineStr" s="2" r="A' + (f+1) + '"><is><t>' + label + '</t></is></c>' + 
                                        '<c t="inlineStr" s="0" r="B' + (f+1) + '"><is><t>' + data  + '</t></is></c>' + 
                                    '</row>';
                                }
                                newSheet += '</sheetData></worksheet>';
                                xlsx.xl.worksheets['sheet2.xml'] = $.parseXML(newSheet);
                            }
                        }
                    ]
                });
                // hacky way to get datatable to render properly
                setTimeout(function(){ $('#""" + _table_id + """').DataTable().draw(); }, 1000);
            });
            </script>
            """
            # return
            replacement_data_formatted = [replacement_data_formatted]
        else:
            replacement_data_formatted = [ json.dumps(d) if not isinstance(d, str) else d for d in replacement_data ]
        
        
        if append_auth_user_mode is not None:
            # see what type of metadata is being requested
            from sres.users import User
            _unique_usernames = list(set(replacement_data_metadata))
            _user_map = {}
            for _username in _unique_usernames:
                _user_map[_username] = User()
                if not _user_map[_username].find_user(username=_username, add_if_not_exists=False):
                    del(_user_map[_username])
            # append the auth_user metadata
            replacement_data_flattened, _topology = utils.flatten_list(replacement_data_formatted)
            _expanded_replacement_data_metadata = []
            for i, _e in enumerate(_topology):
                _expanded_replacement_data_metadata.extend([replacement_data_metadata[i]] * _e)
            for i, meta in enumerate(_expanded_replacement_data_metadata):
                if meta in _user_map.keys():
                    replacement_data_flattened[i] += ' (' + str(_user_map[meta].config.get(append_auth_user_mode, meta)) + ')'
                else:
                    replacement_data_flattened[i] = f'{replacement_data_flattened[i]} ({meta})'
        else:
            replacement_data_flattened = utils.flatten_list(replacement_data_formatted)[0]
        return {
            'replacement_data': replacement_data_flattened,
            'scripts': return_scripts
        }
    
    ### Find global magic formatters
    if re.search(utils.GLOBAL_MAGIC_FORMATTER_REFERENCE_PATTERN, return_text) is not None:
        for mf in [m for m in MAGIC_FORMATTERS_LIST if m.get('enabled_for_global_magic')]:
            mf_pattern = '\$\{\s*' + mf['name'] + '\s*.*?\}.*?\{\s*' + mf['name'] + '\s*\}\$'
            gmf_matches = re.findall(mf_pattern, return_text)
            if gmf_matches:
                for gmf_match in gmf_matches:
                    substitution_aggregation_mode = 'latest'
                    append_auth_user_mode = None
                    # parse any config
                    gmf_prefix = re.findall('(?<=\$\{).*?(?=\})', gmf_match)
                    if len(gmf_prefix):
                        gmf_config = gmf_prefix[0].split()
                        for gmf_config_element in gmf_config:
                            if gmf_config_element.startswith('history'):
                                try:
                                    if gmf_config_element.split(':')[1] in ['all', 'earliest', 'user_latest', 'user_earliest', 'given_latest', 'given_earliest', 'given_user_latest', 'given_user_earliest', 'given_all']:
                                        substitution_aggregation_mode = gmf_config_element.split(':')[1]
                                except:
                                    # just ignore
                                    pass
                            elif gmf_config_element.startswith('auth_user'):
                                try:
                                    if gmf_config_element.split(':')[1] in ['username', 'full_name', 'given_names', 'surname', 'email']:
                                        append_auth_user_mode = gmf_config_element.split(':')[1]
                                except:
                                    # just ignore
                                    pass
                    # read the column references internal to this global magic formatter
                    column_references = _find_column_references(enclosing_char, gmf_match)
                    all_replacement_data = []
                    for column_reference in column_references:
                        column_loaded = False
                        column_reference = utils.clean_delimiter_from_column_references(column_reference)
                        if preloaded_columns and column_reference in preloaded_columns.keys():
                            column = preloaded_columns[column_reference]
                            column_loaded = True
                        else:
                            column = Column()
                            if column.load(column_reference=column_reference, default_table_uuid=default_table_uuid):
                                column_loaded = True
                                preloaded_columns[column_reference] = column
                        if column_loaded:
                            allow_html = column.config['custom_options']['allow_html']
                            # get data
                            student_data = _load_student_data_from_preload(identifier, column, preloaded_student_data_instances)
                            if student_data._id:
                                if column.is_system_column:
                                    column_config_reference = next(col for col in SYSTEM_COLUMNS if col['insert_value'].upper() == column_reference.upper())['name']
                                    all_replacement_data.extend([student_data.config[column_config_reference]])
                                else:
                                    _replacement_data = _get_replacement_data(column, student_data, allow_html, do_not_encode, substitution_aggregation_mode, blank_replacement, append_auth_user_mode)
                                    all_replacement_data.extend(_replacement_data['replacement_data'])
                                    return_scripts += _replacement_data['scripts']
                    # transform and present loaded data
                    replacement_text = None
                    if mf['name'] in ['join_space', 'join_bullets', 'join_paragraphs']:
                        # remove empty elements
                        all_replacement_data = [ x for x in all_replacement_data if x != '' ]
                        # format
                        if mf['name'] == 'join_space':
                            replacement_text = ' '.join(all_replacement_data)
                        elif mf['name'] == 'join_bullets':
                            replacement_text = '<ul><li>' + '</li><li>'.join(all_replacement_data) + '</li></ul>'
                        elif mf['name'] == 'join_paragraphs':
                            replacement_text = '<p>' + '</p><p>'.join(all_replacement_data) + '</p>'
                        # do replacement
                        if replacement_text is not None:
                            return_text = return_text.replace(gmf_match, replacement_text)
    
    if only_process_global_magic_formatters is False:
        ### Find general fields
        if re.search('\$(' + '|'.join(GENERAL_FIELDS.keys()) + ')\$', return_text) is not None:
            for key, field in GENERAL_FIELDS.items():
                if '${}$'.format(key) in return_text:
                    return_text = return_text.replace('${}$'.format(key), datetime.now().strftime(field['format']))
    
    ### Find standard column references
    # find column references
    column_references = _find_column_references(enclosing_char, return_text)
    # set up helper objects
    allow_html = 'false'
    
    if only_process_global_magic_formatters:
        column_references = []
    
    # perform replacement
    for column_reference in column_references:
        column_loaded = False
        column_reference = utils.clean_delimiter_from_column_references(column_reference)
        if preloaded_columns and column_reference in preloaded_columns.keys():
            column = preloaded_columns[column_reference]
            column_loaded = True
        else:
            column = Column()
            if column.load(column_reference=column_reference, default_table_uuid=default_table_uuid):
                column_loaded = True
                preloaded_columns[column_reference] = column
        if column_loaded:
            allow_html = column.config['custom_options']['allow_html']
            replacement_text = ''
            # get data
            student_data = _load_student_data_from_preload(identifier, column, preloaded_student_data_instances)
            if student_data._id:
                if column.is_system_column:
                    column_config_reference = next(col for col in SYSTEM_COLUMNS if col['insert_value'].upper() == column_reference.upper())['name']
                    replacement_text = student_data.config[column_config_reference]
                    if identity_anonymiser_active:
                        replacement_text = anonymise(column_config_reference, replacement_text)
                else:
                    _replacement_data = _get_replacement_data(column, student_data, allow_html, do_not_encode, blank_replacement=blank_replacement)
                    replacement_data = _replacement_data['replacement_data']
                    return_scripts += _replacement_data['scripts']
                    # transform and present loaded data
                    if column.magic_formatter == 'display' or column.magic_formatter == 'description':
                        replacement_text = ' '.join(replacement_data)
                    elif column.magic_formatter == 'image' and column.config['type'] == 'image':
                        replacement_text = ''.join(replacement_data)
                    elif column.magic_formatter == 'image' and column.config['type'] == 'imgurl':
                        replacement_text = ''.join(replacement_data)
                    elif column.magic_formatter == 'audio_player' and column.config['type'] == 'multiEntry':
                        replacement_text = '<br>'.join(replacement_data)
                    elif column.magic_formatter == 'file_download_links' and column.config['type'] == 'file':
                        replacement_text = '<br>'.join(replacement_data)
                    elif column.magic_formatter == 'file_download_links_bullets' and column.config['type'] == 'file':
                        replacement_text = '<ul><li>' + '</li><li>'.join(replacement_data) + '</li></ul>'
                    elif column.magic_formatter in ['join_space', 'join_bullets', 'join_paragraphs']:
                        # remove empty elements
                        replacement_data = [ x for x in replacement_data if x != '' ]
                        # format
                        if column.magic_formatter == 'join_space':
                            replacement_text = ' '.join(replacement_data)
                        elif column.magic_formatter == 'join_bullets':
                            replacement_text = '<ul><li>' + '</li><li>'.join(replacement_data) + '</li></ul>'
                        elif column.magic_formatter == 'join_paragraphs':
                            replacement_text = '<p>' + '</p><p>'.join(replacement_data) + '</p>'
                    elif column.magic_formatter and str(column.magic_formatter).startswith('round'):
                        round_to = column.magic_formatter.replace('round', '')
                        if utils.is_number(round_to) and utils.is_number(replacement_data[0]):
                            replacement_text = str(utils.round_number(float(replacement_data[0]), round_to))
                        else:
                            replacement_text = replacement_data[0]
                    else:
                        replacement_text = replacement_data[0]
                # do replacement
                column_reference_to_find = '${}$'.format(column_reference)
                return_text = return_text.replace('$', enclosing_char) # restore any unreplaced enclosing chars
                if replacement_text is not None:
                    # any final cleaning
                    if column.config['custom_options'].get('newline_character_conversion', 'disabled') in [' ', '<br>']:
                        newline_character = column.config['custom_options']['newline_character_conversion']
                        if newline_character == '<br>':
                            newline_character = Markup(newline_character)
                        replacement_text = utils.replace_newline_characters(replacement_text, newline_character)
                    if column.config['custom_options'].get('mojibake_conversion', 'enabled') == 'enabled':
                        replacement_text = utils.replace_mojibake(replacement_text)
                        replacement_text = utils.replace_quote_html_entities(replacement_text)
                    # perform the replacement
                    return_text = return_text.replace(column_reference_to_find, replacement_text)
            else:
                print('Could not find student', identifier)
                # default to replacing in nothing
                return_text = return_text.replace('${}$'.format(column_reference), '')
        else:
            logging.error('error loading column [{}]'.format(column_reference))
    
    if only_process_global_magic_formatters is False:
        # find summary references
        if '$SMY_' in return_text:
            summary_references = _find_summary_references(return_text)
            for summary_reference in summary_references:
                summary = Summary()
                summary_reference = summary_reference.replace('$', '').replace('SMY_', '')
                if summary.load(summary_reference):
                    # is grouping active?
                    grouping_values = None
                    if summary.is_grouping_active():
                        # get the grouping column
                        grouping_column_reference = summary.get_grouping_column_reference()
                        column_loaded = False
                        if preloaded_columns and grouping_column_reference in preloaded_columns.keys():
                            column = preloaded_columns[grouping_column_reference]
                            column_loaded = True
                        else:
                            column = Column()
                            if column.load(column_reference=grouping_column_reference, default_table_uuid=default_table_uuid):
                                column_loaded = True
                                preloaded_columns[grouping_column_reference] = column
                        if column_loaded:
                            student_data = _load_student_data_from_preload(identifier, column, preloaded_student_data_instances)
                            if student_data._id:
                                _data = student_data.get_data(column.config['uuid'])
                                if column.subfield is None:
                                    grouping_values = [ _data['data'] ]
                                else:
                                    try:
                                        _data = json.loads(_data['data'])
                                        grouping_values = [ _data[column.subfield] ]
                                    except:
                                        logging.error('Could not get grouping value')
                        else:
                            logging.error('Could not load column to get grouping value')
                    # prepare for the representation
                    presentation_mode = summary.config['representation_config']['presentation']['mode']
                    presentation_mode_extra_config = json.dumps(summary.config['representation_config']['presentation'].get('extra_config', {}))
                    calculation_mode = summary.config['representation_config']['calculation']['mode']
                    calculation_mode_extra_config = json.dumps(summary.config['representation_config']['calculation'].get('extra_config', {}))
                    grouping_mode = summary.config['representation_config']['grouping']['mode']
                    grouping_comparison_mode = summary.config['representation_config']['grouping']['comparison_mode']
                    grouping_column_reference = summary.config['representation_config']['grouping']['column_reference']
                    if type(summary.config['column_reference']) is str:
                        summary_column_reference_encoded = summary.config['column_reference']
                    elif type(summary.config['column_reference']) is list:
                        summary_column_reference_encoded = parse.quote(','.join(summary.config['column_reference']))
                    if presentation_mode.startswith('chart_'):
                        representation_class = 'sres-summary-representation-chart'
                        representation_styles = 'min-width:25vw; min-height:25vh;'
                        representation_element = 'div'
                    elif presentation_mode == 'wordcloud':
                        representation_class = 'sres-summary-representation-wordcloud'
                        representation_styles = 'width:100%; height:40vh;'
                        representation_element = 'div'
                    else:
                        representation_class = ''
                        representation_styles = ''
                        representation_element = 'span'
                    # add a span for the representation
                    chart_id = 'representation_' + utils.create_uuid()
                    summary_card_html = f"""<{representation_element} id="{chart_id}" class="{representation_class}" style="{representation_styles}"
                        data-sres-presentation-mode="{presentation_mode}"
                        data-sres-calculation-mode="{calculation_mode}"
                        data-sres-grouping-mode="{grouping_mode}"
                        data-sres-grouping-comparison-mode="{grouping_comparison_mode}"
                        data-sres-grouping-column-reference="{grouping_column_reference}"
                        data-sres-column-reference-encoded="{summary_column_reference_encoded}"
                        data-sres-summary-uuid="{summary.config['uuid']}">
                    """
                    if grouping_values is not None:
                        summary_card_html += """<select class="sres-summary-grouping-values"></select>"""
                    summary_card_html += """<span class="fa fa-circle-notch spinning" aria-label="Loading..."></span></{representation_element}>"""
                    return_text = return_text.replace( f"$SMY_{summary.config['uuid']}$", summary_card_html )
                    # add a script to load the representation upon DOM ready
                    return_scripts += f"""
                        <script>
                            $(document).ready(function(){{
                                $('#{chart_id}').attr('data-sres-calculation-mode-extra-config', JSON.stringify({calculation_mode_extra_config}));
                                $('#{chart_id}').attr('data-sres-presentation-mode-extra-config', JSON.stringify({presentation_mode_extra_config}));
                    """
                    if grouping_values is not None:
                        for grouping_value in grouping_values:
                            return_scripts += f"""$('#{chart_id}').find('.sres-summary-grouping-values').append('<option value="{grouping_value}">{grouping_value}</option>');"""
                    return_scripts += f"""
                                updateSummaryRepresentation('{chart_id}', false, true);
                            }});
                        </script>
                    """
                else:
                    logging.debug('could not load summary!')
    
    # return
    return {
        'new_text': return_text,
        'scripts': return_scripts
    }

def _escape_substituted_text(input, do_not_encode, allow_html):
    if isinstance(input, list):
        _i = []
        for i in input:
            _i.append(_escape_substituted_text(i, do_not_encode, allow_html))
        return _i
    else:
        if do_not_encode or allow_html == 'true':
            return input
        elif allow_html == 'strip':
            return BeautifulSoup(input, 'html.parser').get_text()
        else:
            return escape(input)

def _is_username_allowed_access(auth_user_challenge, auth_users):
    """
        Helper method to determine if username specified in auth_user_challenge (string) is found
        in auth_users (list of strings of usernames).
    """
    if utils.is_json(auth_users) and auth_user_challenge.lower() in [u.lower() for u in json.loads(auth_users)]:
        return True
    elif isinstance(auth_users, str) and auth_user_challenge.lower() in auth_users.lower():
        return True
    elif isinstance(auth_users, list) and auth_user_challenge.lower() in [u.lower() for u in auth_users]:
        return True
    else:
        return False
        
def search_students(term, table_uuid=None, column_uuid=None, preloaded_column=None, preloaded_student_data=None):
    from sres.tables import Table
    ret = {
        'search_results': [],
        'restricted_results_count': 0,
        'term': term
    }
    db = _get_db()
    # get the table ObjectId
    table_oid = None
    table = Table()
    column = Column()
    restrict_by_username_column = None
    anonymise_names = False
    allow_searching = True
    if preloaded_column is not None:
        column = preloaded_column
    if preloaded_column is not None or (column_uuid and column.load(column_uuid)) and (
        column.is_user_authorised(authorised_roles=['user', 'administrator'])
        or
        column.is_peer_data_entry_enabled()
    ):
        table_oid = column.table._id
        table.load(column.table.config['uuid'])
        restrict_by_username_column = column.config['custom_options']['restrict_by_username_column']
        anonymise_names = True if column.config['custom_options']['show_name_when_searching'] == 'hide' else False
        allow_searching = False if column.config['custom_options']['allow_identifier_entry_search'] == 'false' else True
    elif table_uuid and table.load(table_uuid) and table.is_user_authorised(categories=['user', 'administrator']):
        table_oid = table._id
    else:
        # error loading table or column
        return ret
    # find additional id columns
    additional_identifier_columns = [col['uuid'] for col in table.get_additional_identifier_columns()]
    # build filter to search db.data
    filters = []
    filters.append({'table_uuid': table.config['uuid']})
    term_filters = []
    # TODO special usyd codes; need to refactor
    if re.match('^[A-Z0-9a-z]{2}[0-9]{9}[A-Za-z0-9]{4}$', term):
        term_filters.append({'sid': {'$regex': '^{}$'.format(term[2:11]), '$options': 'i'}})
    if re.match('^[A-Za-z]{4}[0-9]{4}$', term):
        term_filters.append({'email': {'$regex': '^{}@uni.sydney.edu.au$'.format(term), '$options': 'i'}})
    # iterate through fields
    for field in NAME_FIELDS + IDENTIFIER_FIELDS + additional_identifier_columns:
        if allow_searching:
            term_filters.append({field: {'$regex': '{}'.format(term), '$options': 'i'}})
        else:
            term_filters.append({field: {'$regex': '^{}$'.format(term), '$options': 'i'}})
    filters.append({'$or': term_filters})
    # students searching other students as part of peer data entry
    if preloaded_column is not None and column.is_peer_data_entry_restricted():
        filters.extend(column.get_db_filter_restrictors_for_peer_data_entry(preloaded_student_data=preloaded_student_data))
    # process only_show when...
    if preloaded_column is not None and column.is_only_show_condition_enabled():
        filters.extend(column.get_db_filter_restrictors_for_only_show())
    # search db.data
    results = db.data.find({'$and': filters})
    results = list(results)
    # prepare for return
    identity_anonymiser_active = is_identity_anonymiser_active()
    for result in results:
        if restrict_by_username_column and restrict_by_username_column in result.keys():
            if not _is_username_allowed_access(auth_user_challenge=get_auth_user(), auth_users=result[restrict_by_username_column]):
                ret['restricted_results_count'] += 1
                continue
        _result = {
            'studentstatus': result['status'],
            'sid': result['sid']
        }
        if identity_anonymiser_active:
            _result['display_sid'] = anonymise('sid', result['sid'])
            _fullname = result.get('preferred_name', '') + ' ' + result.get('surname', '')
            _result['fullname'] = anonymise('full_name', _fullname)
            _result['fullgivenname'] = _result['fullname']
        else:
            _result['display_sid'] = result['sid']
            _result['fullname'] = '{} {}'.format(result.get('preferred_name', ''), result.get('surname', '')) if not anonymise_names else 'Anonymous'
            _result['fullgivenname'] = '{} {}'.format(result.get('given_names', ''), result.get('surname', '')) if not anonymise_names else 'Anonymous'
        ret['search_results'].append(_result)
    # sort
    ret['search_results'] = natsorted(ret['search_results'], key=lambda i: i['fullname'], alg=ns.IGNORECASE)
    # return
    return ret

def enumerate_distinct_data_by_column(column, search_term, search_any=False, sort_results=True):
    """
        Searches db.data and retrieves distinct values for column based on search term.
        
        column (Column) loaded class instance
        search_term (string)
        search_any (boolean) Whether to just search for anything, ignoring search_term
        sort_results (boolean)
    """
    db = _get_db()
    search_term = str(search_term)
    search_results = []
    # search db.data
    if search_any:
        results = db.data.find({
            'table_uuid': column.table.config['uuid'],
            f"{column.config['uuid']}": { '$exists': True }
        }, [ f"{column.config['uuid']}" ])
        results = [ r.get(column.config['uuid']) for r in results ]
        search_results = list(set(results))
    else:
        results = db.data.find({
            'table_uuid': column.table.config['uuid'],
            f"{column.config['uuid']}": { '$regex': search_term, '$options': 'i' }
        }, [ f"{column.config['uuid']}" ])
        results = [ r.get(column.config['uuid']) for r in results ]
        # parse results as necessary
        results = list(set(results))
        if column.subfield is not None:
            for result in results:
                try:
                    temp = json.loads(result)[column.subfield]
                    if search_term.lower() in str(temp).lower():
                        search_results.append(temp)
                except:
                    search_results.append(result)
            search_results = list(set(search_results))
        else:
            for result in results:
                if search_term.lower() in str(result).lower():
                    search_results.append(result)
    # order results
    if sort_results:
        search_results = natsorted(search_results, alg=ns.IGNORECASE)
    # return
    return search_results

def export_all():
    
    pass

def get_distinct_data_for_column(column_uuid, subfield=None):
    
    pass

def get_random_identifier_for_table(table_uuid, identifier_type='sid'):
    db = _get_db()
    if identifier_type in IDENTIFIER_FIELDS:
        results = db.data.find(
            {
                'table_uuid': table_uuid,
                'status': 'active'
            }, 
            [identifier_type]
        )
        results = list(results)
        if results:
            record_number = random.randint(0, len(results) - 1)
            return results[record_number][identifier_type]
        else:
            return None
    else:
        return None

def run_aggregation_bulk(source_column_uuids, target_identifiers, override_username=None):
    """
        Triggers the calculation of aggregators that aggregate the input columns.
        
        source_column_uuids (list of string uuids)
        target_identifiers (list of string identifiers)
        override_username (string)
    """
    t0 = datetime.now()
    from sres.aggregatorcolumns import find_aggregators_of_columns, AggregatorColumn
    aggregator_column_uuids = find_aggregators_of_columns(source_column_uuids)
    #print('time for find_aggregators_of_columns', (datetime.now() - t0).total_seconds())
    ret = {}
    for aggregator_column_uuid in aggregator_column_uuids:
        t0 = datetime.now()
        ret[aggregator_column_uuid] = {
            'successfully_aggregated': 0,
            'unsuccessfully_aggregated': 0
        }
        aggregator_column = AggregatorColumn()
        columns_already_traversed = []
        if aggregator_column.load(aggregator_column_uuid):
            if aggregator_column.config['aggregation_options']['recalculate_trigger'] != 'manual':
                aggregation_results = aggregator_column.calculate_aggregation(
                    identifiers=target_identifiers,
                    auth_user_override=override_username,
                    columns_already_traversed=columns_already_traversed
                )
                print('time for calculate_aggregation', (datetime.now() - t0).total_seconds())
                for identifier, aggregation_result in aggregation_results.items():
                    if aggregation_result['success']:
                        ret[aggregator_column_uuid]['successfully_aggregated'] += 1
                    else:
                        ret[aggregator_column_uuid]['unsuccessfully_aggregated'] += 1
                # log
                logging.info("Completed bulk aggregation [{}] [{}] [{}]".format(
                    aggregator_column_uuid,
                    ret[aggregator_column_uuid]['successfully_aggregated'],
                    ret[aggregator_column_uuid]['unsuccessfully_aggregated']
                ))
                print("Completed aggregation", aggregator_column_uuid, ret[aggregator_column_uuid]['successfully_aggregated'], ret[aggregator_column_uuid]['unsuccessfully_aggregated'])
    return ret

def _run_aggregation_single(identifier, column_uuid, columns_already_traversed=[], auth_user_override='', preloaded_columns=None):
    """
        Runs aggregation
        
        column_uuid (string) The aggregator column that needs to be recalculated
        columns_already_traversed (list of string column_uuids)
        auth_user_override (string??)
        preloaded_columns
        
        Returns boolean of success
    """
    from sres.aggregatorcolumns import AggregatorColumn
    if preloaded_columns is None:
        preloaded_columns = {}
    #logging.debug('in _run_aggregation_single ' + column_uuid + ' ' + identifier)
    if column_uuid in preloaded_columns.keys():
        aggregator_column = preloaded_columns[column_uuid]
    else:
        aggregator_column = AggregatorColumn()
        aggregator_column.load(column_uuid)
        preloaded_columns[column_uuid] = aggregator_column
    if aggregator_column.column_loaded:
        #logging.debug('calling calculate_aggregation [{}] [{}] [{}]'.format(aggregator_column.config['uuid'], aggregator_column.config['name'], str(columns_already_traversed)))
        identifiers=[identifier]
        # does this aggregator need to also be run for other identifiers?
        if aggregator_column.config['aggregation_options']['method'] == 'self_peer_review':
            # get the identifiers
            student_data = StudentData(aggregator_column.table)
            if student_data.find_student(identifier):
                identifiers.extend( student_data.get_identifiers_for_students_in_same_group(
                    aggregator_column.config['aggregation_options']['aggregator_type_self_peer_review_grouping_column']
                ) )
                identifiers = list(set(identifiers))
        # run aggregation
        result = aggregator_column.calculate_aggregation(
            identifiers=identifiers,
            columns_already_traversed=columns_already_traversed[:],
            auth_user_override=auth_user_override,
            preloaded_columns=preloaded_columns
        )
        return result[identifier]['success']
    return False

def _aggregation_consumer(args):
    _run_aggregation_single(
        identifier=args['identifier'],
        column_uuid=args['column_uuid'],
        columns_already_traversed=args['columns_already_traversed'],
        auth_user_override=args['auth_user_override']
    )
def _remove_task_id(future):
    cookie = DbCookie(future.sres_auth_user_override)
    cookie.delete(future.sres_task_id)
def _iterate_aggregated_by(identifier, aggregated_by, columns_already_traversed, auth_user_override, threaded_aggregation=False, task_id=None, preloaded_columns=None):
    """Helper method to iterate through a column's aggregated_by (i.e. run aggregations of aggregator
        columns that use the triggering column as a source). This method gives the option of running
        these aggregations in sequential processes within a parallel thread.
    """
    if preloaded_columns is None:
        preloaded_columns = {}
    if threaded_aggregation:
        # log that we are starting
        if task_id:
            cookie = DbCookie(auth_user_override)
            cookie.set(
                key=task_id,
                value="running"
            )
        try:
            with concurrent.futures.ProcessPoolExecutor() as executor:
                for aggregator_column_uuid in aggregated_by:
                    if len(aggregator_column_uuid.strip()) == 0:
                        continue
                    future = executor.submit(_aggregation_consumer, {
                        'identifier': identifier,
                        'column_uuid': aggregator_column_uuid, 
                        'columns_already_traversed': columns_already_traversed, 
                        'auth_user_override': auth_user_override
                    })
                    future.sres_auth_user_override = auth_user_override
                    future.sres_task_id = task_id
                    future.add_done_callback(_remove_task_id)
        except Exception as e:
            logging.exception(e)
            print(e)
    else:
        for aggregator_column_uuid in aggregated_by:
            if len(aggregator_column_uuid.strip()) == 0:
                continue
            try:
                _run_aggregation_single(
                    identifier=identifier,
                    column_uuid=aggregator_column_uuid, 
                    columns_already_traversed=columns_already_traversed, 
                    auth_user_override=auth_user_override,
                    preloaded_columns=preloaded_columns
                )
            except Exception as e:
                logging.exception(e)
                print(e)

def _modify_multientry_subfield(complete_data, subfield_n, subfield_data):
    """Modifies the data of a specific subfield.
        
        complete_data (str or list) Full data for multientry column. If str, will be json-loaded
        subfield_n (integer) 0-based subfield
        subfield_data (str or None) Data to be put into the specified subfield.
            If None, no modification is made.
        
        Returns dict {
            original_subfield_n
            original_subfield_data
            success
            new_complete_data
        }
    """
    
    ret = {
        'original_subfield_n': subfield_n,
        'original_subfield_data': None,
        'success': False,
        'new_complete_data': None
    }
    
    # parse input if needed
    if isinstance(complete_data, list):
        parsed_data = complete_data
    elif isinstance(complete_data, str):
        parsed_data = json.loads(complete_data)
    else:
        # quit
        return ret
    
    # process
    if subfield_n >= len(parsed_data):
        # index out of range
        pass
    else:
        ret['original_subfield_data'] = parsed_data[subfield_n]
        parsed_data[subfield_n] = subfield_data
        ret['new_complete_data'] = parsed_data
        ret['success'] = True
    return ret

class StudentData:
    
    def __init__(self, table, preview_only=False):
        """
            table (Table or string table_uuid) The table that StudentData should be based in.
            preview_only (boolean) Whether or not we are operating under preview mode.
        """
        from sres.tables import Table
        self.db = _get_db()
        # define instance variables
        if isinstance(table, str):
            self.table = Table()
            self.table.load(table_uuid=table)
        elif isinstance(table, Table):
            self.table = table
        elif preview_only == True:
            class _DummyTable:
                def __init__(self):
                    self._id = '_preview'
                    self.config = {
                        'uuid': 'preview'
                    }
            self.table = _DummyTable()
        else:
            logging.error('failed StudentData init due to table ' + str(table))
            raise Exception('failed StudentData init due to table ' + str(table))
        #print('StudentData.table', self.table._id, self.table.config['uuid'])
        self.default_config = deepcopy(STUDENT_DATA_DEFAULT_CONFIG)
        self.preview_only = preview_only
        # reset the config and data
        self._reset()
        # set some pretend config if previewing
        if preview_only == True:
            self.config = {
                'preferred_name': 'PreviewStudent',
                'given_names': 'PreviewStudent',
                'surname': 'PreviewStudent',
                'sid': 'PREVIEWONLY', 
                'email': 'previewstudent@sres.io'
            }
        # set up logger
        self.data_logger = logging.getLogger('sres.db.studentdata')
        # Column instance cache
        self.columns_cache = None
    
    def _reset(self):
        self.config = deepcopy(STUDENT_DATA_DEFAULT_CONFIG)
        self.data = {}
        self._id = None
    
    def find_student(self, identifiers, find_like=False, match_all=False):
        """
            Searches db.data for a student with the specified identifier.
            
            identifiers (str or dict) Identifier(s) to search. If a dict, then provide 
                identifiers in field: identifier format e.g. {'sid': '12345', 'email': 'test@test.edu'}.
                If a str, will convert it into the dict format in the order of IDENTIFIER_FIELDS.
            find_like (boolean) Whether to find like (True) or exact (False)
            match_all (boolean) If False, will search from the first identifier type.
                If True, will match on all identifier types.
            
            Returns True if found and loaded.
            Returns False otherwise.
        """
        if self.preview_only:
            return True
        t0 = datetime.now()
        from sres.users import User
        # if table not loaded then fail immediately
        if self.table._id is None:
            return False
        # clean and standardise identifiers
        id_dict = {}
        if isinstance(identifiers, dict):
            for k, v in identifiers.items():
                if k in IDENTIFIER_FIELDS:
                    id_dict[k] = str(v)
        else:
            identifiers = str(identifiers)
            # special usyd codes; TODO must be refactored elsewhere
            if re.match('^[A-Z0-9a-z]{2}[0-9]{9}[A-Za-z0-9]{4}$', identifiers):
                id_dict['sid'] = identifiers[2:11]
            elif re.match('^[A-Za-z]{4}[0-9]{4}$', identifiers):
                id_dict['email'] = '{}@uni.sydney.edu.au'.format(identifiers)
                id_dict['username'] = identifiers
            else:
                # find additional id columns
                additional_identifier_columns = [col['uuid'] for col in self.table.get_additional_identifier_columns()]
                for f in IDENTIFIER_FIELDS + additional_identifier_columns:
                    id_dict[f] = str(identifiers)
        # search based on provided identifier(s)
        if id_dict and match_all:
            filter = {
                #'table': self.table._id,
                'table_uuid': self.table.config['uuid']
            }
            for k, v in id_dict.items():
                regex = ('{}' if find_like else '^{}$').format(re.escape(v))
                filter[k] = {'$regex': regex, '$options': 'i'}
            results = self.db.data.find(filter)
            results = list(results)
        elif id_dict:
            # loop through identifier types
            for identifier_type, identifier in id_dict.items():
                filter = {
                    #'table': self.table._id,
                    'table_uuid': self.table.config['uuid'],
                    identifier_type: {
                        '$regex': ('{}' if find_like else '^{}$').format(re.escape(identifier)),
                        '$options': '-i'
                    }
                }
                results = self.db.data.find(filter)
                results = list(results)
                if len(results) == 1:
                    break
        else:
            return False
        # try force a primary identifier match i.e. to SID
        if len(results) != 1 and isinstance(identifiers, str):
            # try force a primary identifier match i.e. to SID
            return self.find_student(
                identifiers={'sid': identifiers},
                match_all=True
            )
        # next attempts - search the db.users table for more information about this person
        if len(results) == 0:
            identifier = None
            if isinstance(identifiers, str):
                identifier = identifiers
            elif id_dict.get('username'):
                identifier = id_dict['username']
            else:
                pass
            user = User()
            if identifier and user.find_user(username=identifier, add_if_not_exists=False):
                results = self.db.data.find({
                    #'table': self.table._id,
                    'table_uuid': self.table.config['uuid'],
                    'email': user.config['email']
                })
                results = list(results)
        # load and return
        if len(results) == 1:
            return self.load(results[0])
        else:
            return False
    
    def load_from_oid(self, oid):
        if isinstance(oid, str):
            oid = ObjectId(oid)
        results = self.db.data.find({'$and': [
            {'table_uuid': self.table.config['uuid']},
            {'_id': oid}
        ]})
        results = list(results)
        if len(results) == 1:
            return self.load(results[0])
        else:
            return False
    
    def load(self, db_result):
        """
            Loads a student's data into this instance.
            db_result (dict) containing the db document.
        """
        t0 = datetime.now()
        # load oid
        self._id = db_result['_id']
        # load up identifier, name, and other non-data fields
        for key, value in self.default_config.items():
            try:
                self.config[key] = db_result[key]
            except:
                pass
        # load up data fields
        for key, value in db_result.items():
            if key not in NON_DATA_FIELDS:
                self.data[key] = value
        return True
    
    def reload(self):
        if self._id:
            return self.load_from_oid(self._id)
        return False
    
    def save(self):
        # if table identifiers are null... fail
        if not self.table._id or not self.table.config['uuid']:
            print('table not identified', self.table._id, self.table.config['uuid'])
            return False
        # set up record
        record = {
            'table': self.table._id,
            'table_uuid': self.table.config['uuid'] # make sure the table_uuid is saved
        }
        # add the details
        for field in NON_DATA_FIELDS:
            if field in self.config.keys() and self.config[field] is not None:
                record[field] = self.config[field]
        # add the data
        for key, value in self.data.items():
            record[key] = value
        # save
        result = self.db.data.update_one(
            {
                #'table': self.table._id,
                'table_uuid': self.table.config['uuid'],
                'sid': self.config['sid']
            },
            {'$set': record},
            upsert=True
        )
        return result.acknowledged
    
    def add_single_student_from_scratch(self, username):
        ret = {
            'success': False,
            'messages': []
        }
        # try session
        if 'user_config' in session.keys() and session['user_config'].get('username'):
            result = self.add_single_student({
                'sid': session['user_config'].get('sid') or session['user_config'].get('username'),
                'username': session['user_config'].get('username'),
                'given_names': session['user_config'].get('given_names'),
                'preferred_name': session['user_config'].get('given_names').split(' ')[0],
                'surname': session['user_config'].get('surname'),
                'email': session['user_config'].get('email')
            })
            if result['success']:
                ret['success'] = True
                return ret
        # try ldap
        ldap_result = self.add_single_student_from_ldap(username)
        ret['messages'].extend(ldap_result['messages'])
        ret['success'] = ldap_result['success']
        return ret
    
    def add_single_student_from_ldap(self, username):
        from sres.ldap import find_user_by_username
        ret = {
            'success': False,
            'messages': []
        }
        users = find_user_by_username(username)
        if len(users) == 1:
            return self.add_single_student({
                'sid': users[0]['identifier'],
                'username': users[0]['username'],
                'given_names': users[0]['given_names'],
                'preferred_name': users[0]['given_names'].split(' ')[0],
                'surname': users[0]['surname'],
                'email': users[0]['email']
            })
        else:
            ret['messages'].append("Could not find unique user.")
        return ret
    
    def add_single_student(self, student_details):
        """
            Adds a single student to self.table.
            
            student_details (dict) with keys corresponding to SYSTEM_COLUMNS name.
                One of the keys must be 'sid'
        """
        ret = {
            'success': False,
            'identifier': None,
            'config': deepcopy(self.default_config),
            'messages': []
        }
        mapping = {}
        details = {}
        if 'sid' not in student_details.keys():
            return ret
        for system_column in SYSTEM_COLUMNS:
            if system_column['name'] in student_details.keys():
                details[system_column['name']] = student_details[system_column['name']]
                mapping[system_column['name']] = {'field': system_column['name']}
        if student_details['sid'] == '' or student_details['sid'] is None:
            # problem; try use username first and then email
            if student_details['username']:
                student_details['sid'] = student_details['username']
            elif student_details['email']:
                student_details['sid'] = student_details['email']
            else:
                # generate a uuid
                student_details['sid'] = utils.create_uuid(sep='-')
        result = self.table._update_enrollments(
            df=[student_details],
            mapping=mapping,
            remove_not_present=False,
            overwrite_details=True
        )
        # find the student to confirm
        ret['messages'].extend(result['messages'])
        if self.find_student(student_details['sid']):
            ret['success'] = True
            ret['identifier'] = self.config['sid']
            ret['config'] = deepcopy(self.config)
        # run other operations as necessary
        from concurrent.futures import ThreadPoolExecutor
        from sres.connector_canvas import run_triggerable_connections
        executor = ThreadPoolExecutor()
        future = executor.submit(run_triggerable_connections, self.table.config['uuid'], [self.config['sid']])
        executor.shutdown(wait=False)
        # return
        return ret
    
    def get_quick_info(self, column_uuid, type='single', template='', preloaded_column=None, preloaded_student_data=None, cache_columns=False):
        """
            Returns HTML (string) of the relevant quickinfo type (string; single|bulk|roll) 
            or provided template (string)
            
            Returns dict {
                quick_info_html (string)
                quick_info_scripts (string) Any javascript required
            }
        """
        ret = {
            'quick_info_html': '',
            'quick_info_scripts': ''
        }
        if preloaded_column is not None:
            column = preloaded_column
        else:
            column = Column()
            column.load(column_uuid)
        if column.column_loaded:
            # load the quick info html template
            if type == 'single' or type == 'bulk':
                quick_info_html = column.config['quick_info'][type]
            elif type == 'roll':
                quick_info_html = column.config['custom_options']['quickinfo_rollview']
            else:
                quick_info_html = template
            # simple replacements
            quick_info_html = quick_info_html.replace('$COLUMNNAME$', column.config['name'])
            quick_info_html = quick_info_html.replace('$COLUMNDESCRIPTION$', column.config['description'])
            if '$INACTIVEWARNING$' in quick_info_html:
                inactive_warning = ''
                if not column.is_active():
                    inactive_warning = '<div class="alert alert-warning">Warning: this column is currently inactive.</div>'
                quick_info_html = quick_info_html.replace('$INACTIVEWARNING$', inactive_warning)
            # other replacements
            cached_columns = {} if self.columns_cache is None else self.columns_cache
            _substituted_text = substitute_text_variables(
                input=quick_info_html, 
                identifier=self.config['sid'], 
                default_table_uuid=self.table.config['uuid'],
                preloaded_student_data=preloaded_student_data,
                preloaded_columns=cached_columns
            )
            ret['quick_info_html'] = _substituted_text['new_text']
            ret['quick_info_scripts'] += _substituted_text['scripts']
            try:
                if cache_columns and cached_columns:
                    self.columns_cache = cached_columns
            except Exception as e:
                logging.exception(e)
        return ret
    
    def get_data(self, column_uuid, preloaded_column=None, default_value=None, do_not_deserialise=False):
        """
            Retrieves data from the instance, which has been previously loaded from db.
            Does NOT take into account multiEntry subfields - just returns raw data for specified column.
            
            column_uuid (string) The base column UUID. Not including subfields etc
            preloaded_column (Column instance) if available, the relevant instance of class Column
                that corresponds to the column_uuid (to save this method from needing to load it again)
            default_value (any)
            do_not_deserialise (bool)
            
            Returns {
                success (boolean)
                aggregated_by (list) of string column_uuids corresponding to aggregator columns
                data (any) This method attempts to deserialise JSON sources
                messages (list) of tuples (string message, string type danger|warning|success)
            }
        """
        ret = {
            'success': False,
            'aggregated_by': [],
            'data': '',
            'messages': []
        }
        if column_uuid in self.data.keys():
            if preloaded_column:
                column = preloaded_column
            else:
                column = Column()
                if not column.load(column_uuid, default_table_uuid=self.table.config['uuid']):
                    return ret
                else:
                    column_uuid = column.config['uuid']
            # something with aggregation TODO
            ret['aggregated_by'] = column.config['aggregated_by']
            # parse data
            if column_uuid in self.data.keys():
                ret['data'] = self.data[column_uuid]
                if not ret['data'].isnumeric() and utils.is_json(ret['data']) and not do_not_deserialise:
                    ret['data'] = json.loads(ret['data'])
                ret['success'] = True
            else:
                ret['messages'].append(("Data does not exist in {} for {}.".format(column_uuid, self.config['sid']), "warning"))
                if default_value is not None:
                    ret['data'] = default_value
                    ret['success'] = True
        else:
            if default_value is not None:
                ret['data'] = default_value
                ret['success'] = True
        
        return ret
    
    def get_data_for_entry(self, column, report_index=-1, parse_if_json=True):
        """Fetch the right data from student_data (self) based on the given column's configuration.
            Used for fetching data for data entry and portal data entry workflows only.
            
            column (Column)
            report_index (integer) Only used if 'multiple_reports_mode' is enabled.
                To reduce (?) confusion and maintain consistency between UI and code, 
                this index is 1-based not 0-based.
            parse_if_json (boolean)
            
            Returns dict.
        """
        ret = {
            'data': '',
            'all_reports_data': [],
            'all_reports_data_keyed': {},
            'report_number': None,
            'report_index': None,
            'report_available_number_count': None,
            'report_all_number_count': None
        }
        
        if column.has_multiple_report_mode_enabled(): # and report_index is not None:
            # get the available change history records
            ch = self.get_change_history(column_uuids=[column.config['uuid']])
            # iterate through change history to get details on the available reports
            _all_report_numbers = []
            _all_report_records = {} # keyed by report_number (int)
            _available_report_numbers = []
            _available_report_records = {} # keyed by report_number (int)
            _auth_user = get_auth_user()
            for _ch in ch: # iterating from most recent to oldest
                _report_number = _ch.get('report_number')
                if _report_number is not None and utils.is_number(_report_number):
                    _report_number = int(_report_number)
                    # add to the temp vars
                    if _report_number not in _all_report_numbers:
                        _all_report_numbers.append(_report_number)
                        _all_report_records[_report_number] = deepcopy(_ch)
                    if _report_number not in _available_report_numbers:
                        if _ch.get('report_workflow_state') == 'deleted':
                            # ignore if marked as deleted
                            continue
                        # add to the temp vars specific for this user
                        if column.config['custom_options']['load_existing_data'] == 'user_latest':
                            if _ch.get('auth_user') != _auth_user:
                                continue
                        _available_report_numbers.append(_report_number)
                        _available_report_records[_report_number] = deepcopy(_ch)
            _available_report_numbers.sort()
            report_index = int(report_index)
            if report_index < 1:
                report_index = len(_all_report_numbers)
            if report_index > len(_available_report_numbers):
                # just return the max
                report_index = len(_available_report_numbers)
            # return
            if len(_available_report_numbers) >= 1:
                # save data
                ret['data'] = _available_report_records[_available_report_numbers[report_index - 1]].get('new_value')
                if parse_if_json and utils.is_json(ret['data']):
                    ret['data'] = json.loads(ret['data'])
                # save report number
                ret['report_number'] = _available_report_numbers[report_index - 1] # essentially an incremented int ID
                # save all reports
                for _available_report_number in _available_report_numbers:
                    _report_data = _available_report_records[_available_report_number].get('new_value')
                    if parse_if_json and utils.is_json(_report_data):
                        _report_data = json.loads(_report_data)
                    ret['all_reports_data'].append(_report_data)
                    ret['all_reports_data_keyed'][_available_report_number] = _report_data
            ret['report_index'] = report_index
            ret['report_available_number_count'] = len(_available_report_numbers) # number of distinct reports available to this user
            ret['report_all_number_count'] = len(_all_report_numbers) # number of distinct reports overall
        else:
            if column.config['custom_options']['load_existing_data'] == 'fresh':
                ret['data'] = ''
            elif column.config['custom_options']['load_existing_data'] == 'user_latest':
                ch = self.get_change_history(column_uuids=[column.config['uuid']], max_rows=1, auth_users=[get_auth_user()])
                if len(ch):
                    try:
                        if parse_if_json:
                            ret['data'] = json.loads(ch[0].get('new_value'))
                        else:
                            ret['data'] = ch[0].get('new_value')
                    except:
                        ret['data'] = ch[0].get('new_value')
                else:
                    ret['data'] = ''
            elif column.config['custom_options']['load_existing_data'] == 'latest':
                if not column.config['uuid'] in self.data.keys():
                    ret['data'] = ''
                elif parse_if_json and utils.is_json(self.data[column.config['uuid']]):
                    ret['data'] = json.loads(self.data[column.config['uuid']])
                else:
                    ret['data'] = self.data[column.config['uuid']]
        
        if column.config['type'] == 'image' and ret['data']:
            ret['data'] = get_file_access_url(ret['data'], full_path=True)
        
        #print('returning from get_data_for_entry')
        #print(str(ret))
        
        return ret
    
    def set_data(
            self,
            column_uuid,
            data, 
            auth_user_override='',
            skip_aggregation=False,
            columns_already_traversed=[], 
            ignore_active=False, 
            student_direct_access_key='', 
            commit_immediately=True, 
            preloaded_column=None,
            only_save_history_if_delta=True,
            skip_auth_checks=False, 
            threaded_aggregation=False,
            preloaded_columns=None,
            report_index=None,
            authorised_as_student=None,
            trigger_apply_to_others=False,
            real_auth_user=None):
        """
            Sets (!!but does not necessarily commit!!) data for the current student.
            Will check authorisation.
            
            column_uuid (string)
            data (any, but will be converted to string for storage)
            auth_user_override (string) If blank, will default to get_auth_user()
            skip_aggregation (boolean)
            columns_already_traversed (list of string column_uuids)
            ignore_active (boolean) Ignore whether the column is active or not. Useful when system is updating from scheduled tasks.
            student_direct_access_key (string)
            commit_immediately (boolean) Whether or not to commit to db
            preloaded_column (Column) A pre-loaded instance of Column to save processing.
            only_save_history_if_delta (boolean)
            skip_auth_checks (boolean) If True, assumes authorised.
            threaded_aggregation (boolean) Whether to use threaded and multiprocessing aggregation.
            preloaded_columns (dict of loaded Column instances keyed by column_uuid)
            report_index (int) For multiple reports mode
            authorised_as_student
            trigger_apply_to_others
            real_auth_user
            
            Returns {
                success (boolean)
                messages (list) of tuples (string message, string type danger|warning|success)
                status_code (int)
                is_aggregated_by_others (bool) Whether this column is aggregated by (other) aggregator columns.
            }
        """
        #print('set_data for [{}] [{}], [{}] [{}]'.format(self.config['sid'], column_uuid, str(columns_already_traversed), str(data)))
        t0 = datetime.now()
        ret = {
            'success': False,
            'column_uuid': column_uuid,
            'messages': [],
            'status_code': 0,
            'is_aggregated_by_others': False,
            'multiple_reports_meta': {}
        }
        if preloaded_columns is None:
            preloaded_columns = {}
        if preloaded_column and (preloaded_column.config['uuid'] == column_uuid or (preloaded_column.is_system_column and preloaded_column.column_reference == column_uuid)):
            column = preloaded_column
            if column_uuid not in preloaded_columns.keys():
                preloaded_columns[column_uuid] = column
        else:
            if column_uuid in preloaded_columns.keys():
                column = preloaded_columns[column_uuid]
            else:
                column = Column()
                column.load(column_uuid, default_table_uuid=self.table.config['uuid'])
                preloaded_columns[column_uuid] = column
        if column.column_loaded:
            authorised = False
            if authorised_as_student is None:
                authorised_as_student = False
            if not skip_auth_checks:
                auth_user = auth_user_override if auth_user_override else get_auth_user()
                try:
                    if real_auth_user is None and auth_user_override:
                        real_auth_user = get_auth_user()
                except:
                    logging.error('Could not get_auth_user(), being asked to override auth_user in set_data')
                # permissions checks
                column_is_active = True if ignore_active else column.is_active()
                authorised_column_user = column.is_user_authorised(username=auth_user, authorised_roles=['user'])
                authorised_column_administrator = column.is_user_authorised(username=auth_user, authorised_roles=['administrator'])
                authorised_table_administrator = column.table.is_user_authorised(username=auth_user)
                if (column_is_active and authorised_column_user) or authorised_column_administrator or authorised_table_administrator:
                    authorised = True
                elif student_direct_access_key and column_is_active:
                    if (column.is_peer_data_entry_enabled() or column.is_self_data_entry_enabled()) and column.is_writeable_by_students():
                        student_data_b = StudentData(column.table)
                        if student_data_b.find_student(get_auth_user()):
                            if column.is_peer_data_entry_allowed(student_data_b, self):
                                authorised = True
                                authorised_as_student = True
                            elif self.config['sid'] == student_data_b.config['sid'] and column.is_self_data_entry_enabled():
                                authorised = True
                                authorised_as_student = True
            elif skip_auth_checks:
                if not auth_user_override:
                    try:
                        auth_user = get_auth_user()
                    except:
                        auth_user = '__system__'
                else:
                    auth_user = auth_user_override
                authorised = True
            # if authorised as a student, check data integrity and adjust if saving data not permitted by configuration
            if authorised and authorised_as_student:
                if column.config['type'] == 'multiEntry':
                    # get previous data
                    _existing_data = self.get_data(column.config['uuid'], column).get('data', [])
                    # loop through to see if any subfields are protected
                    for _n, _subfield_config in enumerate(column.config['multi_entry']['options']):
                        if _subfield_config.get('editing_allowed_by') == 'staff':
                            if _existing_data and type(_existing_data) is list and len(_existing_data) > _n:
                                _res = _modify_multientry_subfield(data, _n, _existing_data[_n])
                            else:
                                _res = _modify_multientry_subfield(data, _n, '')
                            if _res['success']:
                                data = json.dumps(_res['new_complete_data'])
                            else:
                                logging.error('Could not modify multientry subfield')
            # proceed
            if authorised:
                # save change history
                existing_data = self.data.get(column_uuid) # retrieve existing data directly
                # set new data
                data = str(data)
                if column.is_system_column:
                    self.config[column_uuid] = data
                else:
                    self.data[column_uuid] = data
                # commit if necessary
                if commit_immediately:
                    result = self.db.data.update_one(
                        {
                            'table_uuid': self.table.config['uuid'],
                            '_id': self._id
                        },
                        {
                            '$set': {
                                column_uuid: data
                            }
                        }
                    )
                    ret['success'] = result.acknowledged
                else:
                    # return
                    ret['success'] = True
                if existing_data is None or not only_save_history_if_delta or (only_save_history_if_delta and existing_data != data):
                    # set up base change history record
                    if existing_data is None:
                        existing_data = ''
                    _record = {
                        'column_uuid': column_uuid,
                        'table_uuid': self.table.config['uuid'],
                        'existing_data': existing_data,
                        'new_data': data
                    }
                    if real_auth_user:
                        _record['real_auth_user'] = real_auth_user
                    # process multiple reports mode if needed
                    if column.has_multiple_report_mode_enabled():
                        if report_index is None or report_index == '' or (utils.is_number(report_index) and int(report_index) < 1):
                            # need to make a new incremented report number
                            # what's the max report number currently? then add one
                            report_index = -1
                            _report_data = self.get_data_for_entry(column, report_index=report_index)
                            _record['report_number'] = _report_data['report_all_number_count'] + 1
                        else:
                            # need to use existing report number
                            _report_data = self.get_data_for_entry(column, report_index=report_index)
                            _record['report_number'] = _report_data['report_number']
                        ret['multiple_reports_meta']['index'] = _report_data['report_index']
                    # save to db
                    self.save_change_history([_record], auth_user)
                    # get meta on multiple reports
                    if column.has_multiple_report_mode_enabled():
                        _report_data = self.get_data_for_entry(column, report_index=report_index)
                        if report_index == -1:
                            ret['multiple_reports_meta']['index'] = _report_data['report_available_number_count']
                        ret['multiple_reports_meta']['count'] = _report_data['report_available_number_count']
                # aggregation
                existing_data = self.get_data(column_uuid, preloaded_column=column)
                #print('calling _iterate_aggregated_by for {} {} {}'.format(self.config['sid'], column_uuid, datetime.now()))
                if not skip_aggregation and existing_data['aggregated_by'] and not column.is_system_column:
                    # call a method to thread and multiprocess this
                    _iterate_aggregated_by(
                        self.config['sid'],
                        existing_data['aggregated_by'],
                        columns_already_traversed,
                        auth_user,
                        threaded_aggregation,
                        task_id=None if not threaded_aggregation else self._get_parallel_aggregation_task_id(column),
                        preloaded_columns=preloaded_columns
                    )
                    ret['is_aggregated_by_others'] = True
                if trigger_apply_to_others:
                    self.trigger_default_apply_to_others_behaviour(column, auth_user_override=auth_user_override, real_auth_user=real_auth_user)
                # log
                self.data_logger.info("Data set [{}] [{}] [{}] [{}] [{}] [{}]".format(
                    commit_immediately,
                    column_uuid,
                    column.table.config['uuid'],
                    auth_user,
                    self.config['sid'],
                    data
                ))
            else:
                self.data_logger.warning("Data set failed unauthorised [{}] [{}] [{}] [{}] [{}]".format(
                    column_uuid,
                    column.table.config['uuid'],
                    auth_user,
                    self.config['sid'],
                    data
                ))
                ret['messages'].append(('Unauthorised', 'warning'))
                ret['status_code'] = 403
        else:
            self.data_logger.warning("Data set failed column not found [{}] [{}] [{}] [{}]".format(
                column_uuid,
                column.table.config['uuid'],
                self.config['sid'],
                data
            ))
            ret['messages'].append(('Column not found', 'danger'))
        if ret['success']:
            ret['status_code'] = 200
        return ret
    
    def set_data_image(self, column_uuid, image_data, save_image_only=False, student_direct_access_key=''):
        """
            Saves image_data (string, base64 encoded png) to filesystem and optionally records 
            filename in column_uuid.
            
            column_uuid (string)
            image_data (string, base64 encoded png data)
            save_image_only (boolean) If True, will only save the image to filesystem and not update column.
            student_direct_access_key (str)
            
            Return dict {
                'success': boolean
                'messages': [] # tuples
                'new_image_filename': string; filename only not path
            }
        """
        ret = {
            'success': False,
            'messages': [],
            'new_image_filename': ''
        }
        new_image_filename = '{}.png'.format(utils.create_uuid())
        gf = GridFile('files')
        if image_data.startswith('data:image/png;base64,'):
            image_data = image_data.replace('data:image/png;base64,', '')
        if gf.save_file(base64.b64decode(image_data.encode()), filename=new_image_filename, content_type='image/png'):
            ret['new_image_filename'] = new_image_filename
        if save_image_only:
            ret['success'] = True
        else:
            result = self.set_data(
                column_uuid,
                data=new_image_filename,
                commit_immediately=True,
                student_direct_access_key=student_direct_access_key
            )
            ret['success'] = result['success']
            ret['messages'].extend(result['messages'])
        return ret
    
    def set_data_rich(self, table_uuid, column_uuid, identifier, rich_data, student_direct_access_key=''):
        """Saves rich_data (file) to filesystem. Returns the filename.
            
            table_uuid (string)
            column_uuid (string)
            identifier (string) Student identifier
            rich_data (file)
            student_direct_access_key (str)
            
            Return dict {
                'success': boolean
                'messages': [] # tuples
                'saved_as_filename': string; filename only not path
            }
        """
        ret = {
            'success': False,
            'messages': [],
            'saved_as_filename': '',
            'url': ''
        }
        
        saved_as_filename = '{}.{}'.format(
            utils.create_uuid(),
            rich_data.filename.split('.')[-1]
        )
        gf = GridFile('files')
        if gf.save_file(rich_data, filename=saved_as_filename, content_type=rich_data.mimetype, original_filename=rich_data.filename):
            ret['saved_as_filename'] = saved_as_filename
            ret['url'] = get_file_access_url(saved_as_filename, full_path=True)
            # Save some metadata
            gf.set_metadata({
                'table_uuid': table_uuid,
                'column_uuid': column_uuid,
                'identifier': identifier
            })
            ret['success'] = True
        return ret
    
    def save_change_history(self, records=[], username=None):
        return change_history.save_change_history(self.config['sid'], records, username)
        #"""
        #    Saves one or many change history records.
        #    
        #    Each record {
        #        column_uuid (string)
        #        table_uuid (string)
        #        existing_data (string, or will be cast to a string)
        #        new_data (string, or will be cast to a string)
        #        report_number (None|int) Optional
        #        report_workflow_state (None|str) Optional; active|deleted
        #    }
        #"""
        ## collate the data to save
        #histories = []
        ## determine username
        #if not username:
        #    username = get_auth_user()
        ## determine caller
        #try:
        #    caller = request.full_path
        #except:
        #    caller = ''
        ## parse records
        #for record in records:
        #    history = {
        #        'old_value': str(record['existing_data']),
        #        'new_value': str(record['new_data']),
        #        'caller': caller,
        #        'timestamp': datetime.now(),
        #        'auth_user': username,
        #        'identifier': self.config['sid'],
        #        #'table': column_ids[column_uuid]['table'],
        #        #'column': column_ids[column_uuid]['_id'],
        #        'column_uuid': record['column_uuid'],
        #        'table_uuid': record['table_uuid']
        #    }
        #    if record.get('report_number') is not None and utils.is_number(record.get('report_number')):
        #        history['report_number'] = str(record.get('report_number'))
        #        history['report_workflow_state'] = record.get('report_workflow_state', 'active')
        #    
        #    #print('a history in save_change_history')
        #    #print(str(history))
        #    
        #    histories.append(history)
        ## save to db
        #result = self.db.change_history.insert_many(histories)
        ## return
        #return result.acknowledged
    
    def get_change_history(self, column_uuids=[], max_rows=0, auth_users=[], only_after=None, only_before=None):
        return change_history.get_change_history(
            column_uuids, max_rows, auth_users, 
            only_after, only_before, 
            self.config['sid'], self.config['email']
        )
        #"""
        #    Returns a list of db.change_history documents for the specified column_uuids.
        #    If max_rows == 1, this still returns a single-element list.
        #    
        #    column_uuids (list of strings) Must be supplied
        #    max_rows (int)
        #    auth_users (list of string usernames)
        #    only_after (datetime|None)
        #    only_before (datetime|None)
        #"""
        #if not column_uuids:
        #    return []
        #else:
        #    filters = [
        #        {
        #            'column_uuid': {'$in': column_uuids}
        #        }, 
        #        {
        #            'identifier': {'$in': [self.config['sid'], self.config['email']]}
        #        }
        #    ]
        #    # authuser filter
        #    if auth_users:
        #        filters.append({'auth_user': {'$in': auth_users}})
        #    # datetime filters
        #    if only_after is not None and isinstance(only_after, datetime):
        #        filters.append({'timestamp': {'$gte': only_after}})
        #    if only_before is not None and isinstance(only_before, datetime):
        #        filters.append({'timestamp': {'$lte': only_before}})
        #    # search
        #    results = self.db.change_history.find({'$and': filters}).sort([
        #        ('timestamp', -1)
        #    ])
        #    if results.count() > 0:
        #        if max_rows > 0:
        #            return list(results)[:max_rows]
        #        else:
        #            return list(results)
        #    else:
        #        return []
    
    def revert_change_history(self, _id, column_uuid, on_behalf_of=False):
        return change_history.revert_change_history(_id, column_uuid, self, on_behalf_of=on_behalf_of)
        #"""
        #    Applies a saved history. Expects that this instance has a student loaded already.
        #    
        #    _id (ObjectId) The _id of the db.change_history document to be restored
        #    column_uuid (string) The column to restore to
        #    identifier (string) The student to restore for
        #"""
        #ret = {
        #    'success': False,
        #    'messages': []
        #}
        ## get current value
        #results = self.db.change_history.find({
        #    'column_uuid': column_uuid,
        #    'identifier': {'$in': [self.config['sid'], self.config['email']]},
        #    '_id': ObjectId(_id)
        #})
        ## set again
        #if results.count() == 1:
        #    res = self.set_data(
        #        column_uuid=column_uuid,
        #        data=list(results)[0]['new_value'],
        #        commit_immediately=True
        #    )
        #    ret['success'] = res['success']
        #    ret['messages'].extend(res['messages'])
        #else:
        #    ret['messages'].append(("Could not find unique history entry.", "warning"))
        #return ret
    
    def get_identifiers_for_students_in_same_group(self, grouping_column_uuid, include_self=False):
        db_filter = {
            'table': self.table._id,
            f'{grouping_column_uuid}': self.data[grouping_column_uuid]
        }
        if include_self is False:
            db_filter['sid'] = {'$ne': self.config['sid']}
        results = list(self.db.data.find(db_filter, ['sid']))
        return [ r['sid'] for r in results if r.get('sid') ]
    
    def trigger_default_apply_to_others_behaviour(self, source_column, auth_user_override=None, real_auth_user=None):
        if source_column.config['apply_to_others']['active'] == 'true' and source_column.config['apply_to_others']['other_columnuuid']:
            return self.apply_to_others(
                original_column=source_column,
                other_column_uuid=source_column.config['apply_to_others']['other_columnuuid'], 
                notify_by_email=True if source_column.config['notify_email']['active'] == 'true' else False,
                threaded_aggregation=True,
                auth_user_override=auth_user_override,
                real_auth_user=real_auth_user
            )
        else:
            return {
                'success': False,
                'messages': [],
                'other_targets': []
            }
    
    def apply_to_others(self, 
            original_column,
            other_column_uuid=None,
            notify_by_email=False,
            threaded_aggregation=False,
            sdak=None,
            sda_mode='single',
            data_override=None,
            auth_user_override=None,
            assume_authorised_for_column=False,
            real_auth_user=None):
        """
            Applies data to other students from current student.
            Will check authorisation.
            
            original_column (Column, loaded) The column instance that contains the data to be shared around
            other_column_uuid (string) The column determining who to also share the data with
            notify_by_email (boolean) Whether to trigger send_notify_email
            threaded_aggregation (boolean) Whether to calculate aggregations using parallelism
            sdak (string|None)
            sda_mode (string)
            data_override (any, string, None) Data to use instead of existing data
            auth_user_override (string, None) auth_user to use as override in set_data
            assume_authorised_for_column (boolean) If True, will assume column.is_authorised returns True
            real_auth_user
            
            Returns a dict.
        """
        ret = {
            'success': False,
            'messages': [],
            'other_targets': []
        }
        column = original_column
        other_column_uuid = utils.clean_uuid(other_column_uuid)
        student_data = StudentData(self.table)
        if other_column_uuid:
            if other_column_uuid not in self.data.keys():
                ret['messages'].append(("Could not find other students to apply data for.", "warning", 400))
            else:
                if assume_authorised_for_column or column.is_user_authorised(authorised_roles=['user', 'administrator']) or (column.is_user_authorised(sdak=sdak, sda_mode=sda_mode) and column.can_student_trigger_apply_to_others()):
                    results = self.db.data.find({
                        'table': self.table._id,
                        '{}'.format(other_column_uuid): self.data[other_column_uuid],
                        'sid': {'$ne': self.config['sid']}
                    })
                    results = list(results)
                    _data = data_override if data_override is not None else self.data[column.config['uuid']]
                    for result in results:
                        student_data._reset()
                        other_target_result = {}
                        if student_data.find_student(result['sid']):
                            set_data_result = student_data.set_data(
                                column_uuid=column.config['uuid'], 
                                data=_data, 
                                commit_immediately=True,
                                threaded_aggregation=threaded_aggregation,
                                preloaded_column=column,
                                skip_auth_checks=True, # since auth already checked above
                                auth_user_override=auth_user_override,
                                real_auth_user=real_auth_user
                            )
                            ret['messages'].extend(set_data_result['messages'])
                            if set_data_result['success']:
                                other_target_result['name'] = '{} {}'.format(student_data.config['preferred_name'], student_data.config['surname'])
                                other_target_result['sid'] = student_data.config['sid']
                                if notify_by_email:
                                    other_target_result['notify_email'] = student_data.send_notify_email(
                                        column_uuid=column.config['uuid'],
                                        bypass_auth_check=True
                                    )
                            ret['other_targets'].append(other_target_result)
                    ret['success'] = True
                else:
                    ret['messages'].append(('Unauthorised.', 'danger', 403))
        else:
            ret['messages'].append(('Could not load column.', 'warning', 404))
        return ret
    
    def send_notify_email(self, column_uuid, bypass_auth_check=False):
        """
            Sends an email notifying student that a record has been saved.
            Will check authorisation.
            
            column_uuid (string) The column being manipulated
            bypass_auth_check (boolean) Whether to bypass the authorisation check
        """
        ret = {
            'success': False,
            'messages': []
        }
        if self.table.is_user_authorised(categories=['administrator', 'user']) or (bypass_auth_check):
            # authorised user is logged in, or we've bypassed the auth check
            column = Column()
            if column_uuid in self.data.keys() and column.load(column_uuid):
                if self._id:
                    replacements = {
                        'DATA': self.data[column_uuid],
                        'COLUMNNAME': column.config['name'],
                        'UOSCODE': self.table.config['code'],
                        'UOSNAME': self.table.config['name']
                    }
                    email_body = column.config['notify_email']['body']
                    email_subject = column.config['notify_email']['subject']
                    for key, value in replacements.items():
                        email_body = email_body.replace('${}$'.format(key), value)
                        email_subject = email_subject.replace('${}$'.format(key), value)
                    # do other custom replacements
                    email_body = substitute_text_variables(
                        email_body, 
                        self.config['sid'],
                        default_table_uuid=self.table.config['uuid'],
                        do_not_encode=True
                    )['new_text']
                    email_subject = substitute_text_variables(
                        email_subject, 
                        self.config['sid'],
                        default_table_uuid=self.table.config['uuid'],
                        do_not_encode=True
                    )['new_text']
                    # send email
                    try:
                        with current_app.mail.record_messages() as outbox:
                            current_app.mail.send_message(
                                sender=self.table.config['contact']['email'],
                                subject=email_subject,
                                html=email_body,
                                recipients=[self.config['email']],
                                charset='utf-8'
                            )
                            print(outbox)
                            ret['success'] = True
                            success_message = "Notification email has been sent to {}".format(self.config['email'])
                            ret['messages'].append((success_message, "success"))
                            logging.info(success_message)
                    except:
                        logging.error("Notification email to {}".format(self.config['email']))
                        ret['messages'].append(("Error sending notification email to {}".format(self.config['email']), "warning"))
                else:
                    logging.error("send_notify_email self._id is missing")
                    ret['messages'].append(('Could not locate student.', 'warning', 404))
            else:
                ret['messages'].append(('Could not locate data.', 'warning', 400))
        else:
            ret['messages'].append(('Unauthorised.', 'danger', 403))
            logging.error('Unauthorised in send_notify_email')
        return ret
    
    def in_time_delay_lockout(self, column_uuid, seconds_differential=0, challenge_time=None):
        if not challenge_time:
            challenge_time = datetime.now()
        if (isinstance(seconds_differential, int) and seconds_differential == 0) or not seconds_differential.isnumeric():
            return False
        seconds_differential = int(float(seconds_differential))
        history = self.get_change_history(column_uuids=[column_uuid], max_rows=1)
        if history:
            return (challenge_time - history[0]['timestamp']).total_seconds() < seconds_differential
        else:
            return False
    
    def is_username_allowed_access(self, target_columnuuid, restrict_by_username_column='', username=''):
        """
            Determine if username specified in username (string, optional, defaults to current user) 
            is allowed access to target_columnuuid (string)
        """
        if self.table.is_user_authorised():
            return True
        username = username if username else get_auth_user()
        target_columnuuid = utils.clean_uuid(target_columnuuid)
        if not restrict_by_username_column:
            column = Column()
            if column.load(target_columnuuid):
                restrict_by_username_column = column.config['custom_options']['restrict_by_username_column']
            else:
                return True
        restrict_by_username_column = utils.clean_uuid(restrict_by_username_column)
        if restrict_by_username_column in self.data.keys() and target_columnuuid in self.data.keys():
            return _is_username_allowed_access(
                auth_user_challenge=username, 
                auth_users=self.data[restrict_by_username_column]
            )
        else:
            return True
    
    def is_in_groups(self, groups_to_check, grouping_column):
        """
            Determines whether current student is part of the specified groups (string).
            Groups (string) will be deserialised if possible.
        """
        if not groups_to_check or not grouping_column in self.data.keys():
            return False
        current_groups = utils.force_interpret_str_to_list(self.data.get(grouping_column, ''))
        return any(str(current_group) in groups_to_check for current_group in current_groups)
    
    def _get_parallel_aggregation_task_id(self, column):
        return 'sres.column.aggregator.aggregation.parallel.t{}.c{}.i{}'.format(
            column.table.config['uuid'],
            column.config['uuid'],
            self.config['sid']
        )
    
    def still_waiting_for_aggregation(self, column):
        task_id = self._get_parallel_aggregation_task_id(column)
        cookie = DbCookie()
        if cookie.get(task_id):
            return True
        return False
    
    def merge_with(self, other_student_data, mapping):
        available_columns = self.table.get_available_columns()
        available_column_uuids = [ c['uuid'] for c in available_columns ]
        ret = {
            'primary_res': [],
            'secondary_res': [],
            'primary_original': self.config,
            'primary_now': None,
            'secondary_original': other_student_data.config,
            'secondary_now': None,
            'failure_count': 0,
            'success_count': 0
        }
        # determine if a or b is the primary
        primary_col = mapping['sid']
        # set the primary
        failure_count = 0
        success_count = 0
        for field, record in mapping.items():
            if record == primary_col:
                # nothing to do, keep existing
                continue
            if field in available_column_uuids:
                res = self.set_data(
                    column_uuid=field,
                    data=other_student_data.get_data(
                        column_uuid=field, default_value='', do_not_deserialise=True
                    ).get('data'),
                    commit_immediately=True,
                    skip_auth_checks=True
                )
            elif field in NON_DATA_FIELDS:
                res = self.set_data(
                    column_uuid=field,
                    data=other_student_data.config.get(field, ''),
                    commit_immediately=True,
                    skip_auth_checks=True
                )
            else:
                # problem! ignore the data
                failure_count += 1
                continue
            ret['primary_res'].append(res)
            if res['success']:
                success_count += 1
            else:
                failure_count += 1
        # remove the secondary by messing up its identifier fields
        if failure_count == 0:
            for field in IDENTIFIER_FIELDS:
                if other_student_data.config.get(field):
                    ret['secondary_res'].append(other_student_data.set_data(
                        column_uuid=field,
                        data='{}_merged'.format(
                            other_student_data.config.get(field)
                        ),
                        commit_immediately=True,
                        skip_auth_checks=True
                    ))
            ret['secondary_res'].append(other_student_data.set_data(
                column_uuid='status',
                data='inactive',
                commit_immediately=True,
                skip_auth_checks=True
            ))
        # return
        ret['primary_now'] = self.config
        ret['secondary_now'] = other_student_data.config
        ret['success_count'] = success_count
        ret['failure_count'] = failure_count
        return ret

