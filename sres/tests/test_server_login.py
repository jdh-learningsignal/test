import unittest
import time
import datetime
import os

#from flask_testing import TestCase
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from sres.tests import config
from sres.tests import utils as test_utils

class TestBase(unittest.TestCase):
    
    def setUp(self):
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('window-size=1600,900')
        self.driver = webdriver.Chrome(config.CHROMEDRIVER_PATH, chrome_options=options)
        
    def tearDown(self):
        self.driver.quit()

class TestLogin(TestBase):
    
    def test_log_in(self):
        
        # Try an unauthorised login
        print("Attempting unauthorised login")
        self.driver.get(config.URL_BASE)
        self.driver.find_element_by_id('loginUsername').send_keys('fake username')
        self.driver.find_element_by_id('loginPassword').send_keys('fake password')
        self.driver.find_element_by_id('login_button').click()
        
        assert "Please check your username and password and try again" in self.driver.page_source
        
        # Generate test user
        print ("Generating new user")
        new_user = test_utils.create_test_user(['list'])
        assert new_user['username'] and new_user['password']
        
        # Try an authorised login
        print("Attempting authorised login")
        self.driver.get(config.URL_BASE)
        self.driver.find_element_by_id('loginUsername').send_keys(new_user['username'])
        self.driver.find_element_by_id('loginPassword').send_keys(new_user['password'])
        self.driver.find_element_by_id('login_button').click()
        
        assert "SRES" in self.driver.title
        assert new_user['username'] in self.driver.find_element_by_id('logged_in_username').text

if __name__ == '__main__':
    unittest.main()

