import unittest
import time
import datetime
import os

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from sres.tests import config
from sres.tests import utils as test_utils

class TestBase(unittest.TestCase):
    
    def setUp(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('window-size=1600,900')
        self.driver = webdriver.Chrome(config.CHROMEDRIVER_PATH, chrome_options=options)
        
        # Create test users
        self.super_admin = test_utils.create_test_user(['super'])
        assert self.super_admin['username'] and self.super_admin['password']
        self.list_admin = test_utils.create_test_user(['list'])
        assert self.list_admin['username'] and self.list_admin['password']
        self.list_admin_2 = test_utils.create_test_user(['list'])
        assert self.list_admin_2['username'] and self.list_admin_2['password']
        
        # Log in
        self.driver.get(config.URL_BASE)
        self.driver.find_element_by_id('loginUsername').send_keys(self.list_admin['username'])
        self.driver.find_element_by_id('loginPassword').send_keys(self.list_admin['password'])
        self.driver.find_element_by_id('login_button').click()
        assert "SRES" in self.driver.title
        assert self.list_admin['username'] in self.driver.find_element_by_id('logged_in_username').text
        
    def tearDown(self):
        # Close the driver
        self.driver.quit()
        # Deprivilege the users
        test_utils.deprivilege_user(self.super_admin)
        test_utils.deprivilege_user(self.list_admin)

class TestListCRUD(TestBase):
    
    def _delete_list(self, _id, table_uuid):
        
        # Delete the list
        print("Deleting list " + _id)
        self.driver.get(config.URL_BASE + '/login/logout')
        time.sleep(2)
        
        self.driver.get(config.URL_BASE)
        self.driver.find_element_by_id('loginUsername').clear()
        self.driver.find_element_by_id('loginPassword').clear()
        self.driver.find_element_by_id('loginUsername').send_keys(self.super_admin['username'])
        self.driver.find_element_by_id('loginPassword').send_keys(self.super_admin['password'])
        self.driver.find_element_by_id('login_button').click()
        time.sleep(2)
        
        assert self.super_admin['username'] in self.driver.find_element_by_id('logged_in_username').text
        
        time.sleep(2)
        test_utils.move_to_click_element_by_id(self.driver, 'lists-tab')
        time.sleep(4) # give time for the list of lists to load
        test_utils.move_to_send_keys_element_by_css_selector(self.driver, '#table_lists_filter input', _id)
        test_utils.move_to_click_element_by_css_selector(self.driver, f".sres-list-delete[data-sres-tableuuid='{table_uuid}']")
        self.driver.switch_to.alert.accept()
        time.sleep(2)
    
    def _create_list(self, _id=None, add_enrolments=True):
        
        if _id is None:
            _id = str(int(time.time()))
        
        # Create the new list using standard upload
        self.driver.get(config.URL_BASE + '/tables/new')
        print("Creating new list " + _id)
        self.driver.find_element_by_id('uoscode').send_keys('TESTING_' + _id)
        self.driver.find_element_by_id('uosname').send_keys('Test list from testing suite ' + _id)
        self.driver.find_element_by_id('theyear').send_keys(str(datetime.datetime.now().year))
        self.driver.find_element_by_id('thesemester').send_keys('0')
        self.driver.find_element_by_id('staffEmailName').send_keys('Test User')
        self.driver.find_element_by_id('staffEmailAddress').send_keys('test.user@sres.io')
        if add_enrolments:
            test_utils.select_by_value_in_element_by_id(self.driver, 'populate_student_list_from', 'autoList')
            self.driver.find_element_by_id('autoListFiles').send_keys(os.getcwd() + '/enrolments_DEMO1234_v2.csv')
            time.sleep(2)
            self.driver.find_element_by_id('modal_autoList_mapping_confirm').click()
        else:
            test_utils.select_by_value_in_element_by_id(self.driver, 'populate_student_list_from', 'none')
        time.sleep(0.5)
        self.driver.find_element_by_id('submitButton').click()
        
        assert "Successfully updated list details" in self.driver.page_source
        assert self.driver.find_element_by_link_text('View list')
        
        new_list_uuid = self.driver.find_element_by_link_text('View list').get_attribute('href').split('/')[-1]
        
        return (_id, new_list_uuid)
    
    def test_list_crud(self):
        
        # Create the new list
        _id = str(int(time.time()))
        _id, new_list_uuid = self._create_list(_id)
        
        # Navigate to the newly created list
        print("Navigating to new list " + new_list_uuid)
        self.driver.get(config.URL_BASE + '/tables/' + new_list_uuid)
        time.sleep(1)
        assert ('TESTING_' + _id) in self.driver.title
        assert "Showing 0 of 0 columns" in self.driver.page_source
        
        # Update a few things
        print("Updating the list")
        test_utils.add_user_to_list(self.driver, new_list_uuid, self.list_admin_2['user'])
        
        # Delete the list
        self._delete_list(_id, new_list_uuid)
        assert _id not in self.driver.page_source

if __name__ == '__main__':
    unittest.main()

