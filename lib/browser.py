import logging
import random
import time
import pyautogui
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager


log = logging.getLogger(__name__)

class BrowserManager:
    def __init__(self, browser='chrome', pool_size=5):
        self.browser = browser
        self.pool_size = pool_size
        self.browsers = []

        # Initialize logging
        # logging.basicConfig(level=logging.INFO)
        # self.logger = logging.getLogger(__name__)


    def open_browser(self, headless=False):
        if len(self.browsers) < self.pool_size:
            try:
                if self.browser.lower() == 'chrome':
                    chrome_options = ChromeOptions()
                    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
                    # chrome_options.add_argument('--ignore-certificate-errors')
                    # chrome_options.add_argument('--ignore-ssl-errors')
                    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                    if headless:
                        chrome_options.add_argument('--headless')

                    driver = webdriver.Chrome(service=ChromeService('C:\\Users\\user\\Downloads\\chromedriver-win64\\chromedriver-win64\\chromedriver.exe'), options=chrome_options)
                    driver.maximize_window()
                    self.browsers.append(driver)

                    return driver
                
                else:
                    logging.info('Browser is not supported')
                    return None
            
            except WebDriverException as e:
                log.info(f"Error opening browser: {e}")
                return None
        else:
            logging.info("Maximum pool size reached.")
            return None


    def close_browser(self, driver):
        if driver in self.browsers:
            driver.quit()
            self.browsers.remove(driver)
            logging.info("Browser closed successfully.")
        else:
            log.info("Invalid browser instance.")


    def navigate_to_url(self, driver, url):
        try:
            driver.get(url)
            logging.info(f"Opened the given url in browser.")
            return True
        except WebDriverException as e:
            log.info(f"Error navigating to {url}: {e}")
            return False


    def get_page_source(self, driver):
        return driver.page_source


    def wait_random(self, min_seconds=1, max_seconds=5):
        wait_time = random.uniform(min_seconds, max_seconds)
        time.sleep(wait_time)

    
    def find_element_quick(self, driver, by, values):
        try:
            for value in values:
                element = driver.find_element(by, value)
                if element:
                    logging.info(f"Found element using {by}: {value}")
                    return element
            
            log.info(f"None of the elements with {by} found in the list: {values}.")
            return None
            
        except NoSuchElementException:
            log.info(f"No elements with {by} found.")
            return None
        except WebDriverException as e:
            log.info(f"Error finding element: {e}")
            return None


    def find_element(self, driver, by, value, name="default", timeout=10):
        try:
            element_locator = (by, value)
            
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located(element_locator)
            )
            logging.info(f"Found element using {by}: {value}")
            return element
        except TimeoutException:
            log.info(f"Element for: {name} with {by}: {value} not found within {timeout} seconds.")
            return None
        except WebDriverException as e:
            log.info(f"Error finding element: {e}")
            return None


    def execute_script(self, driver, script):
        try:
            driver.execute_script(script)
            logging.info("Executed script.")
            return True
        except WebDriverException as e:
            log.info(f"Error executing script: {e}")
            return False


    def get_element_location(self, driver, by, value):
        try:
            element = self.find_element(driver, by, value)
            if element:
                location = element.location
                logging.info(f"Location of element ({by}: {value}): ({location['x']}, {location['y']})")
                return location['x'], location['y']
            else:
                self.logger.warning(f"Element ({by}: {value}) not found.")
                return None
        except WebDriverException as e:
            log.info(f"Error getting location of element ({by}: {value}): {e}")
            return None
        
    
    def scroll(self, driver, amount):
        try:
            actions = ActionChains(driver)
            actions.move_to_element(driver.find_element(By.TAG_NAME, 'body')).perform()  # Move to the body element
            actions.send_keys(Keys.PAGE_DOWN * amount).perform()  # Perform scrolling
            logging.info(f"Scrolled down by {amount} times.")
            return True
        except WebDriverException as e:
            log.info(f"Error scrolling: {e}")
            return False

    
    def click_with_action_chains(self, driver, element):
        try:
            actions = ActionChains(driver)
            actions.move_to_element(element).click().perform()  # Move to the element before clicking
            logging.info("Clicked on element using Action Chains.")
            return True
        except WebDriverException as e:
            log.info(f"Error clicking with Action Chains: {e}")
            return False

    
    def type_keys_with_action_chains(self, driver, element, keys):
        try:
            actions = ActionChains(driver)
            actions.move_to_element(element).click().send_keys(keys).perform()  # Move to the element before typing keys

            logging.info(f"Typed keys '{keys}' into element using Action Chains.")
            return True
        except WebDriverException as e:
            log.info(f"Error typing keys with Action Chains: {e}")
            return False
        
    
    def get_position_on_ui(self, asset):
        max_retry = 5
        retry_count = 0
        while retry_count < max_retry:
            try:
                location = pyautogui.locateCenterOnScreen(asset)
                return location
            except Exception as e:
                log.info(f"Error locating element with PyAutoGUI: {e}")
                logging.info(f"At Current retry count: {str(retry_count)}")
            retry_count += 1
        return None
        
    
    def click_with_gui(self, asset):
        max_retry = 5
        retry_count = 0
        while retry_count < max_retry:
            try:
                location = pyautogui.locateCenterOnScreen(asset)
                if location:
                    pyautogui.moveTo(location)
                    pyautogui.click()
                    logging.info(f"Clicked on element using PyAutoGUI. No of tries - {str(retry_count)}")
                    return True
                else:
                    log.info("Asset not found on the screen.")
            except Exception as e:
                log.info(f"Error clicking with PyAutoGUI: {e}")
                logging.info(f"At Current retry count: {str(retry_count)}")
            retry_count += 1
            # time.sleep(0.5)
        log.info("Max retry count reached. Failed to click on element.")
        return False

    
    def type_keys_gui(self, asset, keys):
        max_retry = 5
        retry_count = 0
        while retry_count < max_retry:
            try:
                location = pyautogui.locateCenterOnScreen(asset)
                if location:
                    pyautogui.moveTo(location)
                    pyautogui.click()
                    pyautogui.typewrite(keys, interval=random.uniform(0.05, 0.1))  # Simulate typing with random delays
                    logging.info(f"Typed keys '{keys}' into element using PyAutoGUI. No of tries - {str(retry_count)}")
                    return True
                else:
                    log.info("Asset not found on the screen.")
            except Exception as e:
                log.info(f"Error typing keys with PyAutoGUI: {e}")
                logging.info(f"At Current retry count: {str(retry_count)}")
            retry_count += 1
            # time.sleep(0.5)  # Wait for 1 second before retrying
        log.info("Max retry count reached. Failed to type keys into element.")
        return False
    
        

    def click_abs_position(self, pos, provider='google'):
        x, y = pos
        # pyautogui.moveTo(x, y, duration=random.uniform(0.3, 0.5))
        if provider == 'google':
            for _ in range(3):  
                pyautogui.press('tab')
                time.sleep(0.1)
        
        if provider == 'microsoft':
            for _ in range(2):  
                pyautogui.press('tab')
                time.sleep(0.1)
        # pyautogui.click(duration=random.uniform(0.1, 0.3))
        pyautogui.press('enter')
        logging.info("Clicked element at position.")
        return True


    def type_abs_position(self, pos, keys):
        x, y = pos
        # pyautogui.moveTo(x, y, duration=random.uniform(0.3, 0.5))
        # pyautogui.click(duration=random.uniform(0.1, 0.3))

        pyautogui.typewrite(keys, interval=random.uniform(0.08, 0.15))
        logging.info(f"Typed keys '{keys}' into element using PyAutoGUI.")
        return True
