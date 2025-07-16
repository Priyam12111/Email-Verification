import pika
import json
import time
from bson import ObjectId
from lib.browser import BrowserManager
from lib.validator import AccountChecker
from logging.handlers import RotatingFileHandler
from lib.helpers import *
from lib.objects import *
import logging
import time
import pdb

browser_manager = BrowserManager()
account_checker = AccountChecker()

def browser_based_valid(email, provider):
    try:
        driver = browser_manager.open_browser()
        validation_status = account_checker.validate(driver, [email], provider)
        browser_manager.close_browser(driver)
        
        logging.info(f"validation status: {validation_status}")
        return validation_status
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    browser_based_valid('avnit.chopra@petronetlng.in', 'microsoft')
