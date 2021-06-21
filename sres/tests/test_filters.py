import unittest
import time
from datetime import datetime
import os
import re
import json
import random

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.remote.command import Command

from sres.tests import config
from sres.tests import utils as test_utils

from sres.tables import Table
from sres.columns import Column
from sres.studentdata import StudentData
from sres.anonymiser import get_random_firstname, get_random_surname

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('window-size=1600,900')

class TestBase(unittest.TestCase):
    
    def setUp(self):
        self.driver = webdriver.Chrome(config.CHROMEDRIVER_PATH, chrome_options=options)
        
        # Create test users
        self.list_admin = test_utils.create_test_user(['list', 'filter'])
        assert self.list_admin['username'] and self.list_admin['password']
        self.list_student = test_utils.create_test_user(['student'])
        assert self.list_student['username'] and self.list_student['password']
        
        self.driver.get(config.URL_BASE)
        self.driver.find_element_by_id('loginUsername').send_keys(self.list_admin['username'])
        self.driver.find_element_by_id('loginPassword').send_keys(self.list_admin['password'])
        self.driver.find_element_by_id('login_button').click()
        assert "SRES" in self.driver.title
        assert self.list_admin['username'] in self.driver.find_element_by_id('logged_in_username').text
        time.sleep(2)
        
    def tearDown(self):
        self.driver.quit()
    
class TestFilters(TestBase):
    
    def test_filter(self):
        
        from sres.tests import test_list_crud
        _id, new_list_uuid = test_list_crud.TestListCRUD._create_list(self, add_enrolments=False)
        assert new_list_uuid
        print('Created new list ' + new_list_uuid)
        print('list admin', self.list_admin)
        
        print('Adding students to list programmatically')
        table = Table()
        assert table.load(new_list_uuid)
        students = []
        try:
            real_target_emails = config.REAL_TARGET_EMAILS
        except:
            real_target_emails = []
        real_target_identifiers = []
        for i in range(0,10):
            firstname = get_random_firstname()
            sid = test_utils.generate_random_string(alphanumeric=True)
            try:
                email = config.REAL_TARGET_EMAILS[i]
                real_target_identifiers.append(sid)
            except:
                email = test_utils.generate_random_string() + '@sres.io'
            students.append({
                'sid': sid,
                'given_names': firstname,
                'preferred_name': firstname,
                'surname': get_random_surname(),
                'email': email
            })
        table._update_enrollments(
            df=students,
            mapping={
                'sid': {'field': 'sid'},
                'given_names': {'field': 'given_names'},
                'preferred_name': {'field': 'preferred_name'},
                'surname': {'field': 'surname'},
                'email': {'field': 'email'}
            }
        )
        print('Students added.')
        
        print('Adding columns programmatically')
        columns = []
        for i in range(1,6):
            column = Column(table)
            column_uuid = column.create(table_uuid=table.config['uuid'], override_username=self.list_admin['username'])
            column.config['name'] = f'test column {i}'
            column.config['type'] = 'mark'
            column.config['active']['from'] = datetime.now()
            column.config['active']['to'] = datetime.now()
            assert column.update(override_username=self.list_admin['username'])
            columns.append(column)
        print('Columns added.')
        
        print('Adding data programmatically')
        student_data = StudentData(table)
        for i, student in enumerate(students):
            student_data._reset()
            assert student_data.find_student(student['sid'])
            for j, column in enumerate(columns): # TODO need to set specific data for early students
                if j == 2 and i < len(real_target_emails):
                    data = random.randrange(0, 15)
                else:
                    data = random.randrange(0, 50)
                res = student_data.set_data(
                    column_uuid=column.config['uuid'],
                    data=data,
                    preloaded_column=column,
                    skip_auth_checks=True,
                    auth_user_override=self.list_admin['username']
                )
        print('Data added.')
        
        print('Making filter...')
        # set strings
        string1 = test_utils.generate_random_string()
        string2 = test_utils.generate_random_string()
        string3 = test_utils.generate_random_string()
        filter_name = f'Test filter {_id}'
        try:
            sender_name = config.REAL_SENDER_EMAIL_NAME
        except:
            sender_name = 'Test Suite'
        try:
            sender_email = config.REAL_SENDER_EMAIL_ADDRESS
        except:
            sender_email = f'testsuite_{test_utils.generate_random_string()}@sres.io'
        # load up new filter page
        self.driver.get(config.URL_BASE + '/filters/new?source_table_uuid=' + new_list_uuid)
        assert "Adding columns" in self.driver.page_source
        # filter name
        test_utils.move_to_send_keys_element_by_id(self.driver, 'filter_name', filter_name)
        # set primary conditions
        test_utils.move_to_click_element_by_css_selector(self.driver, '#primary_conditions_rule_0 .rule-filter-container')
        ActionChains(self.driver).send_keys('test column 3').send_keys(Keys.ENTER).perform()
        ActionChains(self.driver).send_keys(Keys.TAB).send_keys('less').send_keys(Keys.ENTER).send_keys(Keys.TAB).send_keys('20').send_keys(Keys.TAB).perform()
        # try toggle email
        element = self.driver.find_element_by_css_selector("[data-sres-contact-type='email']")
        element = test_utils.get_parent_element(element)
        element.click()
        time.sleep(3)
        assert not self.driver.find_element_by_id('sender_name').is_displayed()
        element.click()
        time.sleep(3)
        # add addresses
        test_utils.move_to_send_keys_element_by_id(self.driver, 'sender_name', sender_name)
        test_utils.move_to_send_keys_element_by_id(self.driver, 'sender_email', sender_email)
        test_utils.move_to_click_element_by_css_selector(self.driver, "[data-sres-insert-column-target='email_subject']")
        time.sleep(1)
        test_utils.move_to_click_element_by_css_selector(self.driver, "#column_chooser_tab_student_details button[value='PREFERREDNAME']")
        time.sleep(1)
        test_utils.move_to_click_element_by_id(self.driver, 'email_subject')
        ActionChains(self.driver).send_keys(Keys.END).send_keys(', hi from the test suite, please ignore this test email').perform()
        # body first
        test_utils.move_to_click_element_by_id(self.driver, 'email_body_first')
        ActionChains(self.driver).send_keys('Hi $PREFERREDNAME$, please ignore this email. It is a test message.').send_keys(Keys.ENTER).send_keys(Keys.ENTER).perform()
        ActionChains(self.driver).send_keys('Col 3: $').send_keys(columns[2].config['uuid']).send_keys('$').send_keys(Keys.ENTER).perform()
        ActionChains(self.driver).send_keys(string1).perform()
        # body last
        test_utils.move_to_click_element_by_id(self.driver, 'email_body_last')
        ActionChains(self.driver).send_keys(string2).send_keys(Keys.ENTER).send_keys('Kind regards,').key_down(Keys.SHIFT).send_keys(Keys.ENTER).key_up(Keys.SHIFT).send_keys('SRES Test Suite').perform()
        # save
        test_utils.move_to_click_element_by_id(self.driver, 'btn_save')
        time.sleep(2)
        assert "Filter configuration successfully updated" in self.driver.page_source
        # edit a bit more
        test_utils.move_to_click_element_by_id(self.driver, 'email_body_last')
        ActionChains(self.driver).key_down(Keys.CONTROL).send_keys(Keys.END).key_up(Keys.CONTROL).send_keys(Keys.ENTER).send_keys(string3).perform()
        print('Saving and previewing filter...')
        # save and preview
        test_utils.move_to_click_element_by_id(self.driver, 'btn_save_and_preview')
        # wait for preview screen
        WebDriverWait(self.driver, 15).until(lambda x: f"Preview and run filter {filter_name}" in x.page_source)
        test_utils.save_screenshot(self.driver, 'filter preview')
        WebDriverWait(self.driver, 5).until(lambda x: x.find_element_by_id('preview_email_from').is_displayed())
        # check preview has all the bits
        assert sender_name in self.driver.find_element_by_id('preview_email_from').text
        assert sender_email in self.driver.find_element_by_id('preview_email_from').text
        assert string1 in self.driver.find_element_by_id('preview_email_body').text
        assert string2 in self.driver.find_element_by_id('preview_email_body').text
        assert string3 in self.driver.find_element_by_id('preview_email_body').text
        # unselect some targets as appropriate
        test_utils.move_to_click_element_by_css_selector(self.driver, "[href='#results_table']")
        time.sleep(0.5)
        for element in self.driver.find_elements_by_css_selector("input[name='confirm_send_to']"):
            if element.get_attribute('value') not in real_target_identifiers:
                element.click()
        test_utils.save_screenshot(self.driver, 'filter preview deselecting targets')
        # send an email
        print(f'Attempting to send email from {sender_email} to {real_target_emails}...')
        test_utils.move_to_click_element_by_id(self.driver, 'send_message_button')
        time.sleep(1)
        test_utils.move_to_click_element_by_id(self.driver, 'modal_send_message_confirmation_send')
        
        print('Filter testing complete.')
        
        # Delete list
        #test_list_crud.TestListCRUD._delete_list(self, _id)

if __name__ == '__main__':
    unittest.main()

