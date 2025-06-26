from lib.browser import BrowserManager
from lib.email_digger import check_email_status
from selenium.webdriver.common.by import By
from configs.config import Config
# import logging
from configs.logger import log
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# log = logging.getLogger(__name__)

class AccountChecker(BrowserManager):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settings = Config.SETTINGS

    def validate(self, driver, email_input_list, provider):
        try:
            email = email_input_list[0]
            log.info('Checking whether email domain exists or not...')

            if provider == 'google':
                is_valid_email = self.validate_google_account(driver, email)

            elif provider == 'microsoft':        
                is_valid_email = self.validate_microsoft_account(driver, email)

            else:
                log.info("Domain not found for browser verify: %s" % email)
                pass

            driver.close()

            # log.info(f"Email pattern - {email} not found on Google or Microsoft")
            # log.info(f"Is valid: {is_valid_email}")
            log.info(f"[{email}] Validity checked for provider: {provider} - {is_valid_email}")
            return is_valid_email
        
        except Exception as e:
            log.error(str(e))
            return "Error", None


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
            unique_emails = list(set(email_list))
            return unique_emails

        except Exception as e:
            log.error(str(e))
            return []
        

    # def validate_microsoft_account(self, driver, email):
    #     try:
    #         self.navigate_to_url(driver, self.settings['microsoft']['url'])
    #         input_box = self.find_element(driver, By.XPATH, self.settings['microsoft']['selectors']['username'])

    #         if input_box:
    #             self.type_abs_position(self.settings['microsoft']['pos']['username'], email)
    #             time.sleep(1)
    #             self.click_abs_position(self.settings['microsoft']['pos']['submit'], 'microsoft')
    #             time.sleep(5)  # increase delay to allow full redirect

    #             # Check if redirected to external org SSO login page
    #             current_url = driver.current_url
    #             log.info(f"[{email}] Current URL: {current_url}")

    #             if "microsoftonline.com" not in current_url:
    #                 log.info(f"[{email}] Redirected to org login - likely valid.")
    #                 return True

    #             # Fallback: detect intermediate redirect screen
    #             redirect_text = self.find_element_quick(
    #                 driver, By.XPATH, "//div[contains(text(), \"Taking you to your organisation\")]"
    #             )
    #             if redirect_text:
    #                 log.info(f"[{email}] 'Taking you to your organisation' screen detected.")
    #                 return True

    #             # Error handling
    #             response_element = self.find_element_quick(driver, By.XPATH, [self.settings['microsoft']['selectors']['response']])
    #             if response_element:
    #                 log.info(f"[{email}] Invalid email pattern (error found).")
    #                 return False

    #             log.info(f"[{email}] Valid email pattern (no error, no redirect).")
    #             return True

    #         log.info(f"[{email}] Input box not found. Possibly blocked or slow page.")
    #         return False

    #     except Exception as e:
    #         log.error(f"[{email}] Exception during Microsoft validation: {e}")
    #         return False

    # def validate_microsoft_account(self, driver, email):
    #     try:
    #         self.navigate_to_url(driver, self.settings['microsoft']['url'])
    #         input_box = self.find_element(driver, By.XPATH, self.settings['microsoft']['selectors']['username'])
            
    #         if input_box:
    #             self.type_abs_position(self.settings['microsoft']['pos']['username'], email)
    #             time.sleep(1)
    #             self.click_abs_position(self.settings['microsoft']['pos']['submit'], 'microsoft')
    #             time.sleep(3)
                
    #             response_element = self.find_element_quick(driver, By.XPATH, [self.settings['microsoft']['selectors']['response']])
    #             log.info(f"response {response_element}")
    #             if response_element:
    #                 log.info(f"Email pattern - {email} is Not valid on Microsoft")
    #                 return False
    #             else:
    #                 log.info(f"Email pattern - {email} is valid on Microsoft")
    #                 return True
                                    
    #         return False
            
    #     except Exception as e:
    #         log.error(str(e))
    #         return False
    
    # def validate_microsoft_account(self, driver, email):
    #     try:
    #         self.navigate_to_url(driver, self.settings['microsoft']['url'])
    #         input_box = self.find_element(driver, By.XPATH, self.settings['microsoft']['selectors']['username'])
            
    #         if input_box:
    #             self.type_abs_position(self.settings['microsoft']['pos']['username'], email)
    #             time.sleep(1)
    #             self.click_abs_position(self.settings['microsoft']['pos']['submit'], 'microsoft')
    #             time.sleep(3)

    #             current_url = driver.current_url
    #             log.info(f"Current URL after submit: {current_url}")

    #             # Check if redirected to external domain (org SSO) = valid
    #             org_redirect_patterns = ["okta", "adfs", "extranet", "sso", "idp", "auth", "signin"]
    #             if any(p in current_url.lower() for p in org_redirect_patterns):
    #                 log.info(f"Email pattern - {email} is valid (SSO redirect detected)")
    #                 return True

    #             # Look for "Taking you to your organisation" message
    #             redirect_text = self.find_element_quick(driver, By.XPATH, "//div[contains(text(), \"Taking you to your organisation\")]")
    #             if redirect_text:
    #                 log.info(f"Email pattern - {email} is valid (org redirect screen detected)")
    #                 return True

    #             # Look for error response
    #             # response_element = self.find_element_quick(driver, By.XPATH, [self.settings['microsoft']['selectors']['response']])
    #             response_element = self.find_element_quick(driver, By.XPATH, "//div[@id='usernameError' or contains(text(), 'This username may be incorrect')]")
    #             if response_element:
    #                 log.info(f"Email pattern - {email} is Not valid on Microsoft (error found)")
    #                 return False
    #             else:
    #                 log.info(f"Email pattern - {email} is valid on Microsoft (no error)")
    #                 return True

    #         return False

    #     except Exception as e:
    #         log.error(f"Validation error for {email}: {e}")
    #         return False
   
   
    # def validate_microsoft_account(self, driver, email):
    #     try:
    #         self.navigate_to_url(driver, self.settings['microsoft']['url'])
    #         input_box = self.find_element(driver, By.XPATH, self.settings['microsoft']['selectors']['username'])
            
    #         if input_box:
    #             self.type_abs_position(self.settings['microsoft']['pos']['username'], email)
    #             time.sleep(1)
    #             self.click_abs_position(self.settings['microsoft']['pos']['submit'], 'microsoft')
    #             time.sleep(3)

    #             current_url = driver.current_url
    #             log.info(f"Current URL after submit: {current_url}")

    #             # ✅ Only trust these two cases as valid:
    #             if any(p in current_url.lower() for p in ["okta", "adfs", "extranet", "sso", "idp", "auth", "signin"]):
    #                 log.info(f"Email pattern - {email} is valid (SSO redirect detected)")
    #                 return True

    #             redirect_text = self.find_element_quick(
    #                 driver,
    #                 By.XPATH,
    #                 "//div[contains(text(), 'Taking you to your organisation')]"
    #             )
    #             if redirect_text:
    #                 log.info(f"Email pattern - {email} is valid (org redirect screen detected)")
    #                 return True

    #             # ❌ Default fallback: treat all other results as invalid
    #             log.info(f"Email pattern - {email} is Not valid on Microsoft (no redirect or org message)")
    #             return False

    #         return False

    #     except Exception as e:
    #         log.error(f"Validation error for {email}: {e}")
    #         return False
            

    # def validate_microsoft_account(self, driver, email):
    #     try:
    #         self.navigate_to_url(driver, self.settings['microsoft']['url'])

    #         input_box = self.find_element(driver, By.XPATH, self.settings['microsoft']['selectors']['username'])
    #         if input_box:
    #             self.type_abs_position(self.settings['microsoft']['pos']['username'], email)
    #             time.sleep(1)
    #             self.click_abs_position(self.settings['microsoft']['pos']['submit'], 'microsoft')

    #             # Wait briefly for the page to respond
    #             time.sleep(1.5)

    #             # ✅ Check for redirect patterns (SSO)
    #             current_url = driver.current_url
    #             log.info(f"[{email}] URL after click: {current_url}")
    #             if any(p in current_url.lower() for p in ["okta", "adfs", "extranet", "sso", "idp", "auth", "signin"]):
    #                 log.info(f"[{email}] ✅ Valid via SSO redirect")
    #                 return True

    #             # ✅ Check for org redirect screen
    #             org_text = self.find_element_quick(driver, By.XPATH, "//div[contains(text(), 'Taking you to your organisation')]")
    #             if org_text:
    #                 log.info(f"[{email}] ✅ Valid via org redirect screen")
    #                 return True

    #             # ✅ Wait explicitly for red error to appear
    #             try:
    #                 WebDriverWait(driver, 5).until(
    #                     EC.presence_of_element_located((
    #                         By.XPATH,
    #                         "//div[@id='usernameError' or contains(text(), 'This username may be incorrect')]"
    #                     ))
    #                 )
    #                 log.info(f"[{email}] ❌ Invalid - Red error detected")
    #                 return False
    #             except Exception:
    #                 log.info(f"[{email}] ❌ Invalid - No redirect and no error after wait")
    #                 return False

    #         log.info(f"[{email}] ❌ Input box not found")
    #         return False

    #     except Exception as e:
    #         log.error(f"[{email}] ❌ Exception during validation: {e}")
    #         return False

    def validate_google_account(self, driver, email):
        log.info('here123456')
        try:
            self.navigate_to_url(driver, self.settings['google']['url'])
            input_box = self.find_element(driver, By.XPATH, self.settings['google']['selectors']['username'])
            
            if input_box:
                self.type_abs_position(self.settings['google']['pos']['username'], email)
                time.sleep(1)
                self.click_abs_position(self.settings['google']['pos']['submit'])
                time.sleep(5)
                
                response_element = self.find_element_quick(driver, By.XPATH, [self.settings['google']['selectors']['response']])
                if response_element:
                    log.info(f"Email pattern - {email} is Not valid")
                    # driver.refresh()
                    return False
                else:
                    time.sleep(3)
                    confirm_element = self.find_element_quick(driver, By.XPATH, [self.settings['google']['selectors']['confirm']])
                    if confirm_element:
                        if ("Welcome" in confirm_element.text) or ("Verify" in confirm_element.text):
                            log.info(f"Email pattern - {email} is valid")
                            return True
                        else:
                            workspace_confirm_element = self.find_element_quick(driver, By.XPATH, [self.settings['google']['selectors']['workspace_confirm']])
                            if workspace_confirm_element:
                                if "Connecting to" in workspace_confirm_element.text:
                                    log.info(f"Email pattern - {email} is valid")
                                    return True
                                else:
                                    return False
                            else:
                                return False
                            
                    return False
                    
            return False
            
        except Exception as e:
            log.error(str(e))
            return False


    def validate_microsoft_account(self, driver, email):
        try:
            self.navigate_to_url(driver, self.settings['microsoft']['url'])

            input_box = self.find_element(driver, By.XPATH, self.settings['microsoft']['selectors']['username'])
            if input_box:
                self.type_abs_position(self.settings['microsoft']['pos']['username'], email)
                time.sleep(1)
                self.click_abs_position(self.settings['microsoft']['pos']['submit'], 'microsoft')

                # Wait up to 5s for the error to show
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            "//div[@id='usernameError' or contains(text(), 'This username may be incorrect')]"
                        ))
                    )
                    log.info(f"[{email}] ❌ Invalid - Red error detected")
                    return False
                except:
                    log.info(f"[{email}] ✅ No error detected within timeout, checking for redirect...")

                # Now check redirect conditions
                current_url = driver.current_url
                log.info(f"[{email}] URL after click: {current_url}")
                if any(p in current_url.lower() for p in ["okta", "adfs", "extranet", "sso", "idp", "auth", "signin"]):
                    log.info(f"[{email}] ✅ Valid via SSO redirect")
                    return True

                org_text = self.find_element_quick(driver, By.XPATH, "//div[contains(text(), 'Taking you to your organisation')]")
                if org_text:
                    log.info(f"[{email}] ✅ Valid via org redirect screen")
                    return True

                # No redirect either
                log.info(f"[{email}] ❌ Invalid - No error, no redirect")
                return False

            log.info(f"[{email}] ❌ Input box not found")
            return False

        except Exception as e:
            log.error(f"[{email}] ❌ Exception during validation: {e}")
            return False

    # def identify_email_pattern(self, email):
    #     try:
    #         print(email)
    #         for pattern in Config.PATTERNS:
    #             formatted_pattern = pattern.format(
    #                 first='',
    #                 last='',
    #                 first_initial='',
    #                 last_initial='',
    #                 domain=''
    #             )
    #             if formatted_pattern in email:
    #                 return pattern
    #         return None
    #     except Exception as e:
    #         log.error(str(e))
    #         return None
        