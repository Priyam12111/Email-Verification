from lib.browser import BrowserManager
from selenium.webdriver.common.by import By
from configs.config import Config
import logging
import time
import pdb

log = logging.getLogger(__name__)

class GoogleAccountChecker(BrowserManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pos = {
            'username' : (1037, 560),
            'submit' : (1439, 758)
        }

        self.selectors = {
            'username' : '//*[@id="identifierId"]',
            'response' : '//*[@id="yDmH0d"]/c-wiz/div/div[2]/div/div/div[1]/form/span/section/div/div/div[1]/div/div[2]/div[2]/div'
        }


    def validate(self, driver, email_input_list):
        try:
            self.navigate_to_url(driver, "https://accounts.google.com")

            for email in email_input_list:
                input_box = self.find_element(driver, By.XPATH, self.selectors['username'])
                if input_box:
                    self.type_abs_position(self.pos['username'], email)
                    time.sleep(1)
                    self.click_abs_position(self.pos['submit'])
                    time.sleep(3)
                    response_element = self.find_element_quick(driver, By.XPATH, [self.selectors['response']])
                    if response_element:
                        log.info(f"Email pattern - {email} is Not valid")
                        driver.refresh()
                        continue
                    else:
                        log.info(f"Email pattern - {email} is valid")
                        return "Valid"
                else:
                    pass
                
            return "Maybe"

        except Exception as e:
            log.error(str(e))
            return "Maybe"


    def create_email_patterns(self, first_name, last_name, domain):
        try:
            email_list = []
            for pattern in Config.PATTERNS:
                email = pattern.format(
                    first=first_name.lower(),
                    last=last_name.lower(),
                    first_initial=first_name.lower()[0],
                    last_initial=last_name.lower()[0],
                    domain=domain
                )
                email_list.append(email)
            return email_list

        except Exception as e:
            log.error(str(e))
            return []
