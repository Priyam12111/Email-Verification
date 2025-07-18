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

# def browser_based_valid(email, provider):
#     try:
#         driver = browser_manager.open_browser()
#         validation_status = account_checker.validate(driver, [email], provider)
#         browser_manager.close_browser(driver)
        
#         logging.info(f"validation status: {validation_status}")
#         return validation_status
#     except KeyboardInterrupt:
#         pass

# def browser_based_valid(driver, email, provider):
#     try:
#         validation_status = account_checker.validate(driver, [email], provider)
#         logging.info(f"validation status: {validation_status}")
#         return validation_status
#     except KeyboardInterrupt:
#         pass
    
def browser_based_valid(driver, email, provider):
    try:
        logging.info(f"Starting browser-based validation for {email} on provider {provider}")
        validation_status = account_checker.validate(driver, [email], provider)
        logging.info(f"validation status: {validation_status}")
        if isinstance(validation_status, tuple):
            return False
        return validation_status
    except Exception as e:
        logging.error(f"Browser-based validation exception: {e}")
        return False

if __name__ == '__main__':
    browser_based_valid('avnit.chopra@petronetlng.in', 'microsoft')
