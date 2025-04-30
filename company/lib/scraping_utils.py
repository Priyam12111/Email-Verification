from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from urllib.parse import quote
from datetime import datetime
from lib.configs import envs
from lib.exceptions import *
import random
import time
import pytz
import re

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/95.0.1020.30",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/94.0.992.47",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/93.0.961.47",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36"
]

def validate_email(email):
    raw_mails = ['email_not_unlocked@domain.com']
    if email in raw_mails:
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    match = re.match(pattern, email)
    return bool(match)


def generate_duckduckgo_url(linkedin_url):
    base_url = "https://duckduckgo.com/?t=h_&q="
    encoded_url = quote(linkedin_url, safe='')
    duckduckgo_url = f"{base_url}{encoded_url}&ia=web"
    return duckduckgo_url


def create_engine():
    try:
        headless = envs['HEADLESS']
        dockerised = envs['DOCKER_APP']
        
        options = webdriver.ChromeOptions()
        options.add_argument("start-maximized")
        # options.add_argument(random.choice(user_agents))

        # if headless and dockerised:
        #     options.add_argument("--window-size=1920,1080")
        #     options.add_argument("--headless")
        #     options.add_argument("--disable-gpu")
        #     options.add_argument("--no-sandbox")
        
        # if headless and (not dockerised):
        #     options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
        #     options.add_argument("--window-size=1920,1080")
        #     options.add_argument("--headless")
        #     options.add_argument("--disable-gpu")
        #     options.add_argument("--no-sandbox")

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=options)
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )

        return driver

    except Exception as e:
        print(f"Error: {e}")
        raise EngineCreationFailed(f"Error : {str(e)}")
    

def wait_for_element_generous(driver, by, value, timeout=10):
    print("Generously finding element")
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except TimeoutException as e:
        print(f"Timed out waiting for element")
        time.sleep(1)
        return None
        

def wait_for_element(driver, by, value, timeout=10, max_attempts=2):
    attempts = 0
    while attempts < max_attempts:
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException as e:
            print(f"Attempt {attempts+1}: Timed out waiting for element")
            attempts += 1
            time.sleep(1)
    print(f"Element not found after {max_attempts} attempts")
    return None
    

def wait_for_element_strict(driver, by, value, timeout=15, max_attempts=3):
    attempts = 0
    while attempts < max_attempts:
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except TimeoutException as e:
            print(f"Attempt {attempts+1}: Timed out waiting for element")
            attempts += 1
            if attempts < max_attempts:
                print("Refreshing page and retrying...")
                driver.refresh()
            else:
                print(f"Element not found after {max_attempts} attempts")
                break
    return None
    

def login_linkedin(engine, email, password):
    try:
        engine.get("https://www.linkedin.com/?sign_in")

        email_input = wait_for_element(engine, By.ID, "session_key")
        email_input.send_keys(email)
        time.sleep(1)

        pwd_input = wait_for_element(engine, By.ID, "session_password")
        pwd_input.send_keys(password)
        time.sleep(2)

        login_submit = wait_for_element(engine, By.XPATH, '//button[@type="submit"]')
        login_submit.click()
        time.sleep(3)
    
    except Exception as e:
        msg = f"Error: Error Trying to login to linkedin ...{e}"
        print(f"ERROR : {msg}")
        raise AbortProcess(msg)
    

def verify_login(engine):
    try:
        home_element = wait_for_element(engine, By.XPATH, '//*[@id="global-nav"]/div/nav/ul/li[1]/a')
        if home_element:
            print("Login verified")
            return True
        else:
            return False
    except ElementNotFound:
        print(f"Login not verified")
        return False




def find_and_get_info_from_element(engine, by, value, name="default"):
    try:
        print(f"INFO : Find and get info from - {name}")
        element = wait_for_element(engine, by, value)
        return element.text
    except ElementNotFound:
        print(f"INFO : Skipping {name} element")
        return ""
    

def find_and_click_element(engine, by, value, name="default"):
    try:
        print(f"INFO : Find and click - {name}")
        element = wait_for_element(engine, by, value)
        element.click()
    except ElementNotFound:
        msg = "Element - {name} not found"
        raise AbortProcess(msg)
    

def find_and_get_element_attribute(engine, by, value, attr, name="default"):
    try:
        print(f"INFO : Getting attribute of - {name}")

        element = wait_for_element(engine, by, value)
        if attr == "href":
            return (element.get_attribute(attr)).strip('/'), element
        else:
            return (element.get_attribute(attr)).strip(''), element
    except ElementNotFound:
        msg = "Element - {name} not found"
        raise AbortProcess(msg)
    

def check_login_required(engine):
    try:
        name = "login_form"
        login_form_identifier = wait_for_element(engine, By.XPATH, '//*[@id="main-content"]/div/form/h1', timeout=7)
        if login_form_identifier:
            msg = 'Login required for getting profile data...Aborting.'
            raise AbortProcess(msg)

    except ElementNotFound:
        msg = "Element - {name} not found"
        print("INFO : Login Not Required immediately, proceed to get information.")


def find_company_url_with_id(engine):
    try:
        expected_counter = 3

        while expected_counter <= 7:
            value = f'//*[@id="profile-content"]/div/div[2]/div/div/main/section[{str(expected_counter)}]/div[3]/ul/li[1]/div/div[1]/a'
            
            print(f"Checking at - {value}")
            
            element = wait_for_element_generous(engine, By.XPATH, value)
            if element:
                break
            else:
                expected_counter+=1

        company_url = element.get_attribute("href").strip('/')
        return company_url

    except Exception as e:
        print(f"ERROR : {str(e)}")
        return None


