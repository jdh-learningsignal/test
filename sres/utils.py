from flask import request, session, Markup
from uuid import uuid4
import json
import re
from datetime import datetime
from dateutil import parser, tz
import cexprtk
from Crypto.Util.Padding import pad, unpad
from Crypto.Cipher import AES
import os
from hashlib import sha512
import logging
import mimetypes
from natsort import natsorted, ns
import statistics
from urllib.parse import urlparse
from base64 import b64encode, b64decode
from collections import Counter
import bleach
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, getcontext

from sres.files import get_file_access_url, GridFile
from sres.config import _get_config

DELIMITED_FIELD_REFERENCE_PATTERN =  '\$[A-Z0-9a-z_\.]+\$'
BASE_COLUMN_REFERENCE_PATTERN =        'COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*'
DELIMITED_COLUMN_REFERENCE_PATTERN = '\$COL_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*\$'
DELIMITED_SUMMARY_REFERENCE_PATTERN = '\$SMY_[A-F0-9a-f_]{35}\.?[A-Z0-9a-z_\.]*\$'

GLOBAL_MAGIC_FORMATTER_REFERENCE_PATTERN = '\$\{\s*.+\s*\}.*?\{.+\}\$'

BLEACH_ALLOWED_TAGS = ['a', 'abbr', 'acronym', 'b', 'span', 'blockquote', 'code', 'i', 'strong', 'em', 'ul', 'ol', 'li', 'p', 'sub', 'sup', 'table', 'thead', 'tbody', 'tr', 'td', 'th']
BLEACH_ALLOWED_ATTRIBUTES = {
    '*': ['style'],
    'a': ['href', 'title'],
    'abbr': ['title'],
    'acronym': ['title'],
    'table': ['border', 'class']
}
BLEACH_ALLOWED_STYLES = ['text-decoration', 'color', 'font-weight', 'text-align']

def to_b64(data):
    return b64encode(json.dumps(data).encode()).decode()
    
def from_b64(data):
    return json.loads(b64decode(data.encode()).decode())

def create_uuid(sep='_', uppercase=True):
    s = str(uuid4())
    s = s[0:23] + s[24:]
    if sep != '-':
        s = s.replace("-", sep)
    if uppercase:
        s = s.upper()
    return s
    
def clean_uuid(uuid):
    return re.sub("[^A-Z0-9a-z_-]", "", uuid)

def clean_alphanumeric(s):
    return re.sub("[^A-Z0-9a-z]", "", s)

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except:
        return False
    return True

def is_number(s):
    if s is None:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False
    except TypeError:
        return False
    except:
        return False

def round_number(n, dp):
    if not is_number(n):
        return n
    try:
        getcontext().rounding = ROUND_HALF_UP
        _dp = Decimal(10) ** -int(dp)
        return Decimal(n).quantize(_dp)
    except:
        return n

def list_avg(l):
    l = list_nums(l)
    if len(l) > 0:
        return sum(l) / len(l)
    else:
        return ''

def list_median(l):
    l = list_nums(l)
    if len(l) > 0:
        return statistics.median(l)
    else:
        return ''

def list_sum(l):
    return sum(list_nums(l))

def list_nums(l):
    """Returns only the numeric elements in provided list"""
    return [float(e) for e in l if is_number(e)]

def list_mode(l):
    c = Counter(l)
    return c.most_common(1)[0][0]

def titlecase(s, exceptions=[]):
    word_list = re.split(' |\'|\-', s)
    final = [word_list[0].capitalize()]
    for word in word_list[1:]:
        final.append(word if word in exceptions else word.capitalize())
    return ' '.join(final)

def is_datetime(s):
    """Tries to parse s (string). Returns True if probably a datetime."""
    try:
        parser.parse(s)
        return True
    except:
        return False
    
def check_exprtk_expression(expr):
    try:
        # first replace column references with dummy number, 1
        expr = re.sub(DELIMITED_COLUMN_REFERENCE_PATTERN, '1', expr)
        # then check expression
        cexprtk.check_expression(expr)
        return True
    except:
        return False

def clean_exprtk_expression(expr):
    expr = expr.strip()
    expr = re.sub('\s{2,}', ' ', expr)
    return expr

def get_client_ip_address():
    return request.environ.get('HTTP_X_REAL_IP', request.remote_addr)

def get_referrer_hostname(request):
    """Attempts to return the referrer of the request object."""
    if request.referrer and urlparse(request.referrer).hostname:
        return urlparse(request.referrer).hostname
    elif request.headers.get("Referer"):
        return urlparse(request.headers.get("Referer")).hostname
    else:
        return None

def rn_to_br(input):
    return re.sub("\b(\r\n|\r|\n)+\b", "<br>", input)

def clean_delimiter_from_column_references(column_references, delimiter='$'):
    """
        Cleans delimiter from the start and end of provided column references.
        
        column_references (list of strings, or string)
        
        Returns same type as provided for column_references
    """
    input_type = ''
    if isinstance(column_references, str):
        input_type = 'str'
        column_references = [column_references]
    elif isinstance(column_references, list):
        input_type = 'list'
    ret = []
    for column_reference in column_references:
        r = column_reference
        if r.startswith(delimiter):
            r = r[1:]
        if r.endswith(delimiter):
            r = r[:-1]
        ret.append(r)
    if input_type == 'str':
        return ret[0]
    elif input_type == 'list':
        return ret
    else:
        return column_references

def encrypt_to_hex(input, key=None):
    if not key:
        config = _get_config()
        key = config.SRES['ENC_KEY'] # base64 encoded
    key_bytes = b64decode(key)
    input = pad(input.encode(), 16)
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    return cipher.encrypt(input).hex()
    
def decrypt_from_hex(input, key=None):
    if not key:
        config = _get_config()
        key = config.SRES['ENC_KEY'] # base64 encoded
    key_bytes = b64decode(key)
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    input_bytes = bytes.fromhex(input)
    dec = cipher.decrypt(input_bytes)
    return unpad(dec, 16).decode()    

def utc_to_local(dt):
    """Converts dt (datetime) to local; assumes dt is in UTC"""
    dt = dt.replace(tzinfo=tz.tzutc()) # update dt to be utc
    dt = dt.astimezone(tz.tzlocal()) # convert dt to local time
    return dt

def generate_nonce():
    new_nonce = sha512(os.urandom(16)).hexdigest()
    try:
        #logging.debug('x61' + str(session.get('_nonces')))
        session['_nonces'].append(new_nonce)
        #logging.debug('x62' + str(session.get('_nonces')))
        #logging.debug('xx6')
    except:
        #logging.debug('xx7')
        #logging.debug('x71' + str(session.get('_nonces')))
        session['_nonces'] = [new_nonce]
        #logging.debug('x72' + str(session.get('_nonces')))
    return new_nonce

def validate_nonce(nonce=None):
    logging.debug('zz' + str(session.get('_nonces')))
    if nonce is None:
        logging.debug('xx1')
        return False
    if '_nonces' not in session.keys():
        logging.debug('xx2')
        return False
    if session['_nonces'] is None or (isinstance(session['_nonces'], list) and len(session['_nonces']) == 0):
        logging.debug('xx3')
        return False
    if nonce in session['_nonces']:
        # remove
        session['_nonces'].remove(nonce)
        # confirm
        if nonce not in session['_nonces']:
            return True
        logging.debug('xx4')
    return False

def generate_barcode(text):
    import barcode
    from barcode.writer import ImageWriter
    from io import BytesIO
    generator = barcode.get_barcode_class('code128')
    generator.default_writer_options['write_text'] = False
    generator.default_writer_options['module_width'] = 0.05
    generator.default_writer_options['module_height'] = 3
    code = generator(text, writer=ImageWriter()) # png
    #code = generator(text) # svg
    new_filename = '{}.png'.format(create_uuid())
    b = BytesIO()
    code.write(b)
    gf = GridFile('files')
    gf.save_file(b.getvalue(), new_filename, content_type='image/png')
    return {
        'filename': new_filename ,
        'url': get_file_access_url(new_filename, full_path=True)
    }

def generate_qrcode(text):
    import qrcode
    from io import BytesIO
    img = qrcode.make(text, box_size=5)
    filename = '{}.png'.format(create_uuid())
    b = BytesIO()
    img.save(b)
    gf = GridFile('files')
    gf.save_file(b.getvalue(), filename, content_type='image/png')
    return {
        'filename': filename,
        'url': get_file_access_url(filename, full_path=True)
    }
    
def mark_multientry_labels_as_safe(column):
    if column.config['type'] == 'multiEntry':
        for i, option in enumerate(column.config['multi_entry']['options']):
            column.config['multi_entry']['options'][i]['label'] = Markup(column.config['multi_entry']['options'][i]['label'])

def guess_mime_type(full_file_path):
    config = _get_config()
    mimetypes.init(config.SRES.get('EXTRA_MIME_TYPES_FILES', None))
    return mimetypes.guess_type(full_file_path)[0]

def force_interpret_str_to_list(s, l=None, split_at=None, strip=True, sort_list=True):
    """Converts oddly-formed strings to list. For example:
        "Group 1"                   --> ['Group 1']
        ## ""Group 1", "Group 2""   --> ['Group 1', 'Group 2']
        '"Group 3"'                 --> ['Group 3']
        '["Group 4", "Group 5"]'    --> ['Group 4', 'Group 5']
        "2"                         --> ['2']
        "[3, 6]"                    --> ['3', '6']
        "3, 5"                      --> ['3', '5'] if split_at==',' and strip==True
        "Group 1, Group 2"          --> ['Group 1', 'Group 2'] if split_at==',' and strip==True
        
        s (any) input, will be str()'ed
        l (list|None) The running list being generated. If None, then will create new list
        split_at (string|None) Whether to split the input string e.g. comma-delimited
        strip (boolean) Whether to .strip() the elements
    """
    if l is None:
        l = []
    if isinstance(s, list):
        l.extend(s)
    else:
        s = str(s)
        if len(s) > 0:
            # split
            if split_at:
                temp = s.split(split_at)
                l.extend([str(i) for i in temp])
            else:
                try:
                    temp = json.loads(s)
                except:
                    temp = s
                if isinstance(temp, list):
                    l.extend([str(i) for i in temp])
                else:
                    l.append(str(temp))
            # strip
            if strip:
                l = [i.strip() for i in l]
            # remove dupes
            l = list(dict.fromkeys(l))
            # sort
            if sort_list:
                l = list(natsorted(l, alg=ns.IGNORECASE))
    return l

def flatten_list(l):
    _return_list = []
    _return_topology = []
    for item in l:
        if isinstance(item, list):
            _sublist, _subtopology = flatten_list(item)
            _return_topology.append(len(_sublist))
            _return_list.extend(_sublist)
        else:
            _return_list.append(item)
            _return_topology.append(1)
    return _return_list, _return_topology

def replace_mojibake(s):
    if isinstance(s, str):
        s = s.replace("Â", "")
        s = s.replace("â€™", "'")
        s = s.replace("â€œ", '"')
        s = s.replace("â€“", "-")
        s = s.replace("â€", '"')
        s = s.replace("–", "-")
        s = s.replace("—", "-")
        s = s.replace("“", '"')
        s = s.replace("”", '"')
    return s

def replace_newline_characters(s, replacement=' '):
    if isinstance(s, str):
        s = s.replace('\n', replacement)
        s = s.replace('\r', replacement)
    return s

def replace_quote_html_entities(s):
    if isinstance(s, str):
        s = s.replace('\u2018', "'")
        s = s.replace('\u2019', "'")
        s = s.replace('\u275C', "'")
        s = s.replace('\u275c', "'")
        s = s.replace('\u02BC', "'")
        s = s.replace('\u02Bc', "'")
        s = s.replace('\u201D', '"')
        s = s.replace('\u201d', '"')
        s = s.replace('\u201C', '"')
        s = s.replace('\u201c', '"')
        s = s.replace('\u201D', '"')
        s = s.replace('\u201d', '"')
        s = s.replace('\u2033', '"')
        s = s.replace('\u275D', '"')
        s = s.replace('\u275d', '"')
        s = s.replace('\u275E', '"')
        s = s.replace('\u275e', '"')
        s = s.replace('\u301E', '"')
        s = s.replace('\u301e', '"')
    return s

def bleach_user_input_html(x):
    return bleach.clean(
        x,
        tags=BLEACH_ALLOWED_TAGS,
        attributes=BLEACH_ALLOWED_ATTRIBUTES,
        styles=BLEACH_ALLOWED_STYLES
    )

def bleach_multientry_data(data, column, bleach_all_subfields=False):
    """Bleaches data being saved into a multiEntry column, taking into consideration 
        the subfield configuration unless overridden.
        
        data (str or list)
        column (Column instance) Loaded
        bleach_all_subfields (boolean) If True, will bleach everything.
    """
    if column.config['type'] != 'multiEntry':
        return data
    if type(data) is str:
        if not is_json(data):
            return data
        data = json.loads(data)
    if type(data) is not list:
        return data
    # clean
    _data = []
    for i, x in enumerate(data):
        bleach_this_element = False
        if bleach_all_subfields:
            bleach_this_element = True
        if i < len(column.config['multi_entry']['options']) and column.config['multi_entry']['options'][i].get('type') in ['html-simple']:
            bleach_this_element = True
        if bleach_this_element:
            if type(x) is list:
                _data.append( [ bleach_user_input_html(_x) for _x in x ] )
            else:
                _data.append(bleach_user_input_html(x))
        else:
            _data.append(x)
    return json.dumps(_data)
    

#def validate_csrf_token(token, no_pop=True):
#    if no_pop:
#        stored_token = session.get('_csrf_token', None)
#    else:
#        stored_token = session.pop('_csrf_token', None)
#    if stored_token and stored_token == token:
#        return True
#    return False
#
#def generate_csrf_token():
#    if '_csrf_token' not in session.keys():
#        session['_csrf_token'] = sha512(os.urandom(16)).hexdigest()
#    return session['_csrf_token']

