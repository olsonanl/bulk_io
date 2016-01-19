# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import NoAlertPresentException
import unittest, time, re

class UploadContigsWebdriver(unittest.TestCase):
    def setUp(self):
        self.driver = webdriver.Firefox()
        self.driver.implicitly_wait(30)
        self.base_url = "https://narrative-next.kbase.us/"
        self.verificationErrors = []
        self.accept_next_alert = True
    
    def test_upload_contigs_webdriver(self):
        driver = self.driver
        # ERROR: Caught exception [ERROR: Unsupported command [openWindow | /functional-site/#/dashboard | ]]
        driver.find_element_by_link_text("New Narrative").click()
        driver.find_element_by_css_selector("button.kb-data-list-add-data-text-button").click()
        driver.find_element_by_xpath("//div[8]/div/div/div[5]").click()
        driver.find_element_by_id("undefined-next").click()
        driver.find_element_by_css_selector("td > button.kb-primary-btn").click()
        driver.find_element_by_css_selector("input[type=\"file\"]").clear()
        driver.find_element_by_css_selector("input[type=\"file\"]").send_keys("/Users/dolson/test.fa")
        driver.find_element_by_id("undefined-run").click()
    
    def is_element_present(self, how, what):
        try: self.driver.find_element(by=how, value=what)
        except NoSuchElementException as e: return False
        return True
    
    def is_alert_present(self):
        try: self.driver.switch_to_alert()
        except NoAlertPresentException as e: return False
        return True
    
    def close_alert_and_get_its_text(self):
        try:
            alert = self.driver.switch_to_alert()
            alert_text = alert.text
            if self.accept_next_alert:
                alert.accept()
            else:
                alert.dismiss()
            return alert_text
        finally: self.accept_next_alert = True
    
    def tearDown(self):
        self.driver.quit()
        self.assertEqual([], self.verificationErrors)

if __name__ == "__main__":
    unittest.main()
