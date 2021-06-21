import time
import os
import string
import random

from sres.users import User, change_admins
from sres.auth import remember_fallback_login
from sres.db import _get_db
from sres.tests import config

from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait

def _create_id():
    return str(time.time()).replace('.', '')

# Utility functions

def generate_random_string(length=12, alphanumeric=False):
    letters = string.ascii_lowercase
    if alphanumeric:
        letters = letters + '1234567890'
    return ''.join(random.choice(letters) for i in range(length))

# Create user

def create_test_user(user_types):
    """Creates a test user of the defined type(s) and returns a dict defining the user.
        
        user_types (list of str) list|filter|super|student
    """
    
    ret = {
        'username': '',
        'password': '',
        'roles': [],
        'id': '',
        'user': None
    }
    
    id = _create_id()
    username = 'testuser_' + id
    password = ''.join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(48))
    user = User()
    user.find_user(
        username=username,
        email='testuser.{}@sres.io'.format(id),
        add_if_not_exists=True
    )
    user.config['given_names'] = 'G' + id
    ret['given_names'] = user.config['given_names']
    user.config['surname'] = 'S' + id
    ret['surname'] = user.config['surname']
    remember_fallback_login(username, password, user)
    user.update()
    
    for user_type in user_types:
        if user_type in ['list', 'filter', 'super']:
            res = change_admins(user_type, add_usernames=[username])
            if res['added'] == 1:
                ret['roles'].append(user_type)
        if user_type == 'student':
            user.config['sid'] = 'SID' + id
            ret['sid'] = user.config['sid']
            if user.update():
                ret['roles'].append(user_type)
    
    ret['username'] = username
    ret['password'] = password
    ret['id'] = id
    ret['user'] = user
    
    return ret    

def deprivilege_user(user_dict):
    for role in user_dict['roles']:
        change_admins(role, remove_usernames=[user_dict['username']])

def add_user_to_list(driver, table_uuid, user):
    
    driver.get(f'{config.URL_BASE}/tables/{table_uuid}/edit')
    add_button_elements = driver.find_elements_by_class_name('sres-select-user-add')
    for add_button_element in add_button_elements:
        if add_button_element.get_attribute('data-sres-user-type') == 'user':
            add_button_element.click()
            time.sleep(0.2)
            driver.find_element_by_id('select_user_find_user_term').send_keys(user.config['username'])
            driver.find_element_by_id('select_user_find_user_use').click()
    
    time.sleep(0.2)
    move_to_click_element_by_id(driver, 'submitButton')
    time.sleep(5)
    assert "Successfully updated list details" in driver.page_source

def add_student_to_list(driver, table_uuid, user):
    
    driver.get(f'{config.URL_BASE}/entry/table/{table_uuid}/student/new')
    assert "Add student" == driver.find_element_by_css_selector('.navbar-brand').text
    
    move_to_send_keys_element_by_id(driver, 'system_preferred_name', user.config['given_names'])
    move_to_send_keys_element_by_id(driver, 'system_given_names', user.config['given_names'])
    move_to_send_keys_element_by_id(driver, 'system_surname', user.config['surname'])
    move_to_send_keys_element_by_id(driver, 'system_sid', user.config['sid'])
    move_to_send_keys_element_by_id(driver, 'system_email', user.config['email'])
    move_to_send_keys_element_by_id(driver, 'system_username', user.config['username'])
    
    move_to_click_element_by_id(driver, 'save_system_column_data')
    
    WebDriverWait(driver, 5).until(lambda x: user.config['sid'] in x.page_source)

# Read SRES DB

def read_db_data_value(table_uuid, column_uuid, sid):
    db = _get_db()
    res = list(db.data.find({
        'table_uuid': table_uuid,
        'sid': sid
    }))
    if len(res):
        return res[0].get(column_uuid, None)
    else:
        print('Zero results returned from db find', table_uuid, column_uuid, sid)
        return None

# Move to and click

def move_to_click_element_by_id(driver, id):
    element = driver.find_element_by_id(id)
    ActionChains(driver).move_to_element(element).click(element).perform()
    
def move_to_click_element(driver, element):
    ActionChains(driver).move_to_element(element).click(element).perform()
    
def move_to_click_element_by_css_selector(driver, selector):
    element = driver.find_element_by_css_selector(selector)
    ActionChains(driver).move_to_element(element).click(element).perform()
    return element

# Move to and send keys

def move_to_send_keys_element_by_id(driver, id, keys):
    element = driver.find_element_by_id(id)
    ActionChains(driver).move_to_element(element).perform()
    element.send_keys(keys)

def move_to_send_keys_element_by_css_selector(driver, selector, keys):
    element = driver.find_element_by_css_selector(selector)
    ActionChains(driver).move_to_element(element).perform()
    element.send_keys(keys)

# Move to

def move_to_element_by_id(driver, id):
    element = driver.find_element_by_id(id)
    ActionChains(driver).move_to_element(element).perform()
    
def move_to_element_by_name(driver, name):
    element = driver.find_element_by_name(name)
    ActionChains(driver).move_to_element(element).perform()

def move_to_element(driver, element):
    ActionChains(driver).move_to_element(element).perform()

# Screenshot

def save_screenshot(driver, append_text=''):
    if append_text:
        append_text = f'_{append_text}'
    driver.get_screenshot_as_file(f'{os.getcwd()}/screenshot_{_create_id()}{append_text}.png')

def save_element_screenshot(element):
    element.screenshot('{}/screenshot_{}.png'.format(os.getcwd(), _create_id()))

# Select

def select_by_value_in_element_by_id(driver, id, value):
    move_to_element_by_id(driver, id)
    Select(driver.find_element_by_id(id)).select_by_value(value)
    
def select_by_value_in_element_by_name(driver, name, value):
    move_to_element_by_name(driver, name)
    Select(driver.find_element_by_name(name)).select_by_value(value)
    
def select_by_value_in_element_by_css_selector(driver, selector, value):
    element = driver.find_element_by_css_selector(selector)
    move_to_element(driver, element)
    Select(driver.find_element_by_css_selector(selector)).select_by_value(value)

# Checks

def element_by_css_selector_contains_class(driver, selector, class_name):
    element = driver.find_element_by_css_selector(selector)
    return class_name in element.get_attribute('class').split(' ')

def element_by_css_selector_is_enabled(driver, selector):
    element = driver.find_element_by_css_selector(selector)
    return element.is_enabled()

def element_by_css_selector_has_value_for_property(driver, selector, property_name, value):
    element = driver.find_element_by_css_selector(selector)
    return element.get_property(property_name) == value

def element_by_css_selector_has_value_for_attribute(driver, selector, attribute_name, value):
    element = driver.find_element_by_css_selector(selector)
    return element.get_attribute(attribute_name) == value

# DOM traversal

def get_parent_element(element):
    return element.find_element_by_xpath('..')




