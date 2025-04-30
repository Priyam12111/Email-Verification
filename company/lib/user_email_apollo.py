# This is a sample Python script.
import pyperclip
from botocore.exceptions import HTTPClientError
from bson import ObjectId

from mongo_connection import mg_list, mg_insert, mg_update, mg_aggregate
from lib.u_date import encoded_string, formatted_time

import re
from urllib.parse import urlsplit, urlunparse, urlparse


def url_remove_query(url):
    parsed_url = urlparse(url)

    # Remove the query string
    parsed_url = parsed_url._replace(query=None)

    # Convert the modified URL back to a string
    return urlunparse(parsed_url)


def main_domain(url):
    # url = "https://linkedin.com/company/wurth-italia"
    parsed_url = urlsplit(url)
    return parsed_url.netloc


country_lang_arr = {'kw': 'ar', 'in': 'in', 'sg': 'sg', 'au': 'au', 'uk': 'uk', 'ph': 'tl', 'vn': 'vi', 'mt': 'mt',
                    'hk': 'hk', 'gr': 'gr', 'bg': 'bg', 'si': 'si', 'ng': 'ng', 'mv': 'it', 'ch': 'de', 'be': 'be',
                    'pk': 'pk', 'th': 'th', 'kr': 'ko', 'ca': 'ca', 'ae': 'ar'}


def get_source_lang(link):
    match = re.search(r'https://([a-zA-Z-]+)\.linkedin\.com', link)
    if match:
        subdomain = match.group(1)
        my_list = ['www', 'linkedin', 'it']
        if subdomain in my_list:
            return 'it'
        else:
            return country_lang_arr[subdomain]
    else:
        return 'it'


def sub_domain(url):
    # Regular expression pattern to extract the subdomain
    pattern = r"https?://([^/.]+)\."

    # Use the regular expression to find the subdomain
    match = re.search(pattern, url)
    my_list = ['www', 'linkedin']
    # Check if a match was found and extract the subdomain
    if match:
        subdomain = match.group(1)
        if subdomain in my_list:
            return url
        else:
            return url.replace(subdomain, 'in')
    else:
        return url


def excerpt_string(text):
    parts = text.split('  ', 2)  # Split into at most 3 parts

    if len(parts) >= 2:
        excerpt_str = text.replace(parts[len(parts) - 1], '')
    else:
        excerpt_str = text  # If there are not enough spaces, use the entire string

    return excerpt_str.strip()


from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException

from time import sleep

import difflib

filename = formatted_time("%Y%m%d%H%M%S")


def Write(filenam,cont):  # Updating the vars and Slots
    with open(f'{filenam}.csv', 'a', encoding='utf-8') as f:
        try:
            f.write(f'{cont}')
        except:
            pass
        f.close()


def match_string_percentage(string1, string2):
    # Calculate the matching percentage
    matcher = difflib.SequenceMatcher(None, string1, string2)
    match_percentage = round(matcher.ratio() * 100, 2)

    return match_percentage


username: str = "shubhamtomar033@gmail.com"


def login(engine, email, password):
    # password: str = "Acadecraft@12"

    engine.get("https://www.linkedin.com/?sign_in")
    sleep(5)

    engine.find_element(By.ID, "session_key").send_keys(email)
    sleep(1)

    engine.find_element(By.ID, "session_password").send_keys(password)
    sleep(2)

    engine.find_element(By.XPATH, '//button[@type="submit"]').click()
    sleep(40)
    print("Connected linkedin..")


def google_login(engine, email, password):
    google_login_link = 'https://accounts.google.com/ServiceLogin?hl=en&passive=true&continue=https://www.google.com/&ec=GAZAmgQ'
    google_login_next = '/html/body/div[1]/div[1]/div[2]/div/c-wiz/div/div[2]/div/div[2]/div/div[1]/div/div/button/span'

    engine.get(google_login_link)
    sleep(5)
    # password: str = "Shubham@12"
    # username: str = "shubham@acadecraft.com"
    engine.find_element(By.ID, "identifierId").send_keys(email)
    sleep(1)
    engine.find_element(By.XPATH, google_login_next).click()
    sleep(5)

    engine.find_element(By.XPATH,
                        "/html/body/div[1]/div[1]/div[2]/div/c-wiz/div/div[2]/div/div[1]/div/form/span/section[2]/div/div/div[1]/div[1]/div/div/div/div/div[1]/div/div[1]/input").send_keys(
        password)
    sleep(2)
    print("Connected google..")
    engine.find_element(By.XPATH, google_login_next).click()
    sleep(10)


def ap_login(engine, email, password):
    # password: str = "Shubham@12"
    # username: str = "shubham@acadecraft.com"

    engine.get("https://app.apollo.io/#/login")
    sleep(5)

    engine.find_element(By.XPATH, "/html/body/div[2]/div/div[2]/div[1]/div/div[2]/div/div[2]/div/form/div[5]/div/div/input").send_keys(email)
    sleep(1)

    engine.find_element(By.XPATH, "/html/body/div[2]/div/div[2]/div[1]/div/div[2]/div/div[2]/div/form/div[6]/div/div[1]/div/input").send_keys(password)
    sleep(2)

    engine.find_element(By.XPATH, '//button[@type="submit"]').click()
    sleep(10)
    print("Connected apollo..")


def validate_email(email):
    raw_mails = ['email_not_unlocked@domain.com']
    if email in raw_mails:
        return False
    # Define the regular expression pattern for a basic email validation
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    # Use re.match() to check if the email matches the pattern
    match = re.match(pattern, email)

    # If there is a match, the email is valid
    return bool(match)


if __name__ == "__main__":
    # https://www.linkedin.com/in/ACwAAArdQW8Boj6K6_oVCKT6U5oWRqGc-h1iXUA  656840deaa64482879974d7b
    # https://www.linkedin.com/sales/lead/ACwAAAEuCYQB9CERSeS-LF2MyJogGy5IaJDGC2w,NAME_SEARCH,iyeB
    file_path = 'user-email-apollo.txt'
    extension_path = ''
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Read the first two lines
            extension_path = file.readline().strip().replace('extension_path:', '')
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        exit()
    except Exception as e:
        print(f"An error occurred: {e}")
        exit()

    limit_apollo_hours = 100
    limit_apollo_daily = 500
    credentials = [
        {'email': 'shubham@acadecraft.com', 'g_pass': 'Shubham@12', 'a_pass': 'Shubham@12'},
        {'email': 'rishabh.wasan@acadecraft.com', 'g_pass': 'acade010101', 'a_pass': 'acade010101'},
        {'email': 'sumeet.sawant@acadecraft.com', 'g_pass': 'Shubham@12', 'a_pass': 'Shubham@12'}
    ]
    # linkedin
    limit_linked_hours = 100
    limit_linked_daily = 500
    cred_linked = [
        {'email': 'shubhamtomar033@gmail.com', 'pass': 'Acadecraft@12'},
        {'email': 'manojtomar326@gmail.com', 'pass': 'Tomar@@##123'},
        # {'email': 'priyamtomar012@gmail.com', 'pass': 'Tomar@@##123'}
    ]
    for i in range(1, 11):
        for cred in credentials:
            api_key = cred['email']
            current_date = formatted_time("%Y-%m-%d")
            cond = [
                {
                    '$match': {
                        'apollo_status': True,
                        'api_key': api_key,
                        'modifiedAt': {
                            '$regex': re.compile(f"^{current_date}")
                        }
                    }
                }, {
                    '$group': {
                        '_id': {
                            '$hour': {
                                '$toDate': '$modifiedAt'
                            }
                        },
                        'count': {
                            '$sum': 1
                        }
                    }
                }
            ]
            hour_counter = mg_aggregate('users', cond)
            # print(len(hour_counter), api_key)
            current_hours: int = 0
            current_daily: int = 0
            for hour in hour_counter:
                # print('Document:', document)
                if int(formatted_time("%H")) == hour['_id']:
                    current_hours += hour['count']
                current_daily += hour['count']
            print("Apollo: ", current_date, formatted_time("%H"), "hours=", str(current_hours).zfill(3), "daily=", str(current_daily).zfill(3), api_key)
            linked_api_key = None
            linked_api_pas = None
            current_linked_hours: int = 0
            current_linked_daily: int = 0
            for cred_u in cred_linked:
                current_linked_hours = 0
                current_linked_daily = 0
                linked_api_key = cred_u['email']
                linked_cond = [
                    {
                        '$match': {
                            'apollo_status': True,
                            'linked_api_key': linked_api_key,
                            'modifiedAt': {
                                '$regex': re.compile(f"^{current_date}")
                            }
                        }
                    }, {
                        '$group': {
                            '_id': {
                                '$hour': {
                                    '$toDate': '$modifiedAt'
                                }
                            },
                            'count': {
                                '$sum': 1
                            }
                        }
                    }
                ]
                linked_hours_counter = mg_aggregate('users', linked_cond)
                # print(len(linked_hours_counter), linked_api_key)

                for hour in linked_hours_counter:
                    # print('Document:', document)
                    if int(formatted_time("%H")) == hour['_id']:
                        current_linked_hours += hour['count']
                    current_linked_daily += hour['count']

                if limit_linked_daily > current_linked_daily and limit_linked_hours > current_linked_hours:
                    linked_api_pas = cred_u['pass']
                    break
                # elif limit_linked_hours > current_linked_hours + 1:
                #     linked_api_pas = cred_u['pass']
                #     break
                # else:
                #     continue

            print("Linked: ", current_date, formatted_time("%H"), "hours=", str(current_linked_hours).zfill(3), "daily=", str(current_linked_daily).zfill(3), linked_api_key)
            if linked_api_pas is None:
                print('Linkedin limit finished.')
                exit()
            # exit()
            if current_daily >= limit_apollo_daily:
                continue
            elif current_hours >= limit_apollo_hours:
                continue
            # print('Apollo Running', api_key)

            cond = {
                "uStatus": False,
                # "leadUrl": {"$exists": True},
                "leadUrl": {
                    "$regex": "^https?://(?:www\\.)?[a-zA-Z0-9-]+\\.[a-zA-Z]{2,}(?:/[^\\s]*)?$",
                    "$options": "i"
                },
                "apollo_status": {"$exists": False}
            }
            cursor = mg_list('users', cond, 'createdAt', -1)
            print('Total:', len(cursor))
            if len(cursor):
                # extension_path = 'C:/Users/ABC/AppData/Local/Google/Chrome/User Data/Default/Extensions/alhgpfoeiimagjlnfekdhkjlkiomcapa/7.3.0_0'
                options = webdriver.ChromeOptions()

                # Set the language to US English
                options.add_argument('--lang=en-US')
                options.add_argument(f'--load-extension={extension_path}')
                # options.add_argument('--headless')

                service = Service(ChromeDriverManager().install())

                engine = webdriver.Chrome(service=service, options=options)
                engine.maximize_window()

                sleep(2)
                # Switch to the main tab
                main_tab = engine.window_handles[0]
                engine.switch_to.window(main_tab)

                google_login(engine, api_key, cred['g_pass'])
                ap_login(engine, api_key, cred['a_pass'])
                login(engine, linked_api_key, linked_api_pas)

                for index, document in enumerate(cursor):
                    lk_url = (document['leadUrl'].replace(
                        'https://www.linkedin.com/in', 'https://www.linkedin.com/sales/lead')) + ',NAME_SEARCH,iyeB'
                    serial = str(index + 1).zfill(3)
                    # for testing purpose
                    # lk_url = 'https://www.linkedin.com/sales/lead/ACwAAAANcIUBPCif-6hVz-E7CBKdF2emG82VIxI,NAME_SEARCH,iyeB'
                    print(f"URL:", serial, lk_url)
                    engine.get(lk_url)
                    sleep(10)
                    # unlock profile
                    unlock_x = '/html/body/main/div[1]/div[3]/div/div/div/div/section[1]/section[1]/div[2]/section/div[1]/button/span'
                    try:
                        sleep(2)
                        found_unblock_ele = engine.find_element(by=By.XPATH, value=unlock_x)
                        if found_unblock_ele:
                            found_unblock_ele.click()
                            # Refresh the page
                            engine.refresh()
                            sleep(10)
                    except NoSuchElementException:
                        pass  # Continue to the next XPath expression
                    sleep(1)
                    # ignore like Linkedin member
                    ignore_x_list = [
                        '/html/body/main/div[1]/div[3]/div/div[1]/div/div/section/section[1]/div[1]/div[2]/h1'
                    ]
                    ignore_list = [
                        'linkedin member'
                    ]
                    ignore_try = 0
                    for ignore_xpath in ignore_x_list:
                        try:
                            sleep(2)
                            found_ignore_ele = engine.find_element(by=By.XPATH, value=ignore_xpath)
                            if found_ignore_ele:
                                ignore_txt = found_ignore_ele.get_attribute('innerText').strip().lower()
                                if ignore_txt in ignore_list:
                                    print('break loop::')
                                    ignore_try = 1
                                    break  # If found, break out of the loop
                            sleep(1)
                        except NoSuchElementException:
                            pass  # Continue to the next XPath expression

                    e_text = {'apollo_status': True, 'modifiedAt': str(formatted_time()), 'api_key': api_key,
                              'linked_api_key': linked_api_key}

                    if ignore_try == 0:

                        apollo_icon_x = [
                            '/html/body/div[1]/div/div[1]/div[1]/img',
                            '/html/body/div[1]/div/div[1]/div[1]/img',
                            '/html/body/div[1]/div/div[1]/div[1]',
                        ]
                        for ap_i_xp in apollo_icon_x:
                            try:
                                sleep(2)
                                found_apollo_icon_ele = engine.find_element(by=By.XPATH, value=ap_i_xp)
                                if found_apollo_icon_ele:
                                    found_apollo_icon_ele.click()
                                    break  # If found, break out of the loop
                                sleep(5)
                            except NoSuchElementException:
                                pass  # Continue to the next XPath expression

                        # try:
                        #     apollo_icon = engine.find_element(by=By.XPATH,
                        #                                       value='/html/body/div[1]/div/div[1]/div[1]')
                        #     if apollo_icon:
                        #         apollo_icon.click()
                        # except NoSuchElementException:
                        #     sleep(10)
                        #     apollo_icon = engine.find_element(by=By.XPATH,
                        #                                       value='/html/body/div[1]/div/div[1]/div[1]')
                        #     if apollo_icon:
                        #         apollo_icon.click()
                        sleep(5)

                        emails_vw_xp = [
                            '/html/body/div[19]/div/div[2]/div/div/div[4]/div/div[1]/div[1]/div[3]/div[1]/div[2]/span'
                        ]

                        for e_vw_xp in emails_vw_xp:
                            try:
                                found_vw_element = engine.find_element(by=By.XPATH, value=e_vw_xp)
                                if found_vw_element:
                                    found_vw_element.click()
                                    sleep(3)
                                    break  # If found, break out of the loop
                            except NoSuchElementException:
                                pass  # Continue to the next XPath expression

                        emails_xp = {
                            'business_email': '/html/body/div[19]/div/div[2]/div/div/div[4]/div/div[1]/div[1]/div[3]/div[1]/div/div[1]/div/div[2]/div/div/span/div',
                            'personal_email': '/html/body/div[19]/div/div[2]/div/div/div[4]/div/div[1]/div[1]/div[3]/div[2]/div/div[1]/div[2]/div/div/span/div'
                        }

                        for idx, e_xp in emails_xp.items():
                            try:
                                found_element = engine.find_element(by=By.XPATH, value=e_xp)
                                if found_element:
                                    email_txt = found_element.get_attribute('innerText').strip()
                                    if validate_email(email_txt):
                                        e_text[idx] = email_txt
                                # break  # If found, break out of the loop
                            except NoSuchElementException:
                                pass  # Continue to the next XPath expression
                        # /html/body/main/div[1]/div[3]/div/div[1]/div/div/section[1]/section[1]/div[1]/div[2]/h1
                        if len(e_text) > 2:
                            e_text['uStatus'] = True

                        # click 3 dot ... public url
                        try:
                            xpath_ele = '/html/body/main/div[1]/div[3]/div/div[1]/div/div/section[1]/section[1]/div[2]/section/div[2]/button/span[1]'
                            #            /html/body/main/div[1]/div[3]/div/div[1]/div/div/section[1]/section[1]/div[2]/section/div[2]/button/span[1]
                            element_dot = engine.find_element(by=By.XPATH,
                                                              value=xpath_ele)
                            element_dot.click()
                            sleep(1)
                            # /html/body/div[2]/div[2]/ul/li[3]/button/div/div

                            details_xpath_ele = '/html/body/div[2]/div[2]/ul/li[3]/button/div/div'
                            # /html/body/div[1]/div[2]
                            details_element = engine.find_element(by=By.XPATH, value=details_xpath_ele)
                            details_element.click()
                            sleep(1)

                            copied_public_url = pyperclip.paste()
                            e_text['publicUrl'] = copied_public_url.strip()
                        except NoSuchElementException:
                            print('public url not visible')

                    # updating email id with status
                    upd = mg_update('users',
                                    {'_id': ObjectId(document['_id'])},
                                    e_text)
                    print('MOD:', serial, document['_id'], e_text)
                    current_daily += 1
                    current_hours += 1
                    current_linked_daily += 1
                    current_linked_hours += 1
                    # limit_apollo_hours

                    if current_daily >= limit_apollo_daily:
                        engine.quit()
                        break
                    elif current_hours >= limit_apollo_hours:
                        engine.quit()
                        break
                    elif limit_linked_daily <= current_linked_daily:
                        engine.quit()
                        break
                    elif limit_linked_hours <= current_linked_hours:
                        engine.quit()
                        break
                # sleep(20)
                lk_urls = [
                    'https://www.linkedin.com/sales/lead/ACwAAAEuCYQB9CERSeS-LF2MyJogGy5IaJDGC2w,NAME_SEARCH,iyeB?_ntb=FJNUerSvT9yMRjxRv2KpgA%3D%3D',
                    'https://www.linkedin.com/sales/lead/ACwAAABUhpIBYrwYIRyKENv69Oog5LS9NkpCCOo,NAME_SEARCH,wu20x',
                    'https://www.linkedin.com/sales/lead/ACwAAAT-_1kBUq8zSboKrMmN_EoSiF26dPPKOh4,NAME_SEARCH,JgzX',
                    'https://www.linkedin.com/sales/lead/ACwAABsA-ZUBYJ6XZOZ61Ef9bHfsITb2aN0UUnk,NAME_SEARCH,F5Gg?_ntb=FJNUerSvT9yMRjxRv2KpgA%3D%3D',
                    'https://www.linkedin.com/sales/lead/ACwAAAgDM10BTNZwFbTDszNrVO7Cw9YrZ_NCjzw,NAME_SEARCH,c00o?_ntb=FJNUerSvT9yMRjxRv2KpgA%3D%3D',
                    'https://www.linkedin.com/sales/lead/ACwAAALkGLwBjqFS8OqpTo6ibIoewbGaz4K_T0I,NAME_SEARCH,uNI8?_ntb=FJNUerSvT9yMRjxRv2KpgA%3D%3D',
                    'https://www.linkedin.com/sales/lead/ACwAABLaAEQB81oJXTYeXCzOn5b68Bgl9n844cM,NAME_SEARCH,7ndf',
                    'https://www.linkedin.com/sales/lead/ACwAABXzqIMBrBpUQZ7lk7gtnZqTPq07G3xtExE,NAME_SEARCH,3Leu',
                    'https://www.linkedin.com/sales/lead/ACwAABmT3sIBfQtKByqM23A1nQ28moO0sPxcQXk,NAME_SEARCH,kgRu',
                    'https://www.linkedin.com/sales/lead/ACwAAA6C0UsBVFf_ujhdhIFiSn2ReJIZIPmFW_Y,NAME_SEARCH,DVXp',
                    'https://www.linkedin.com/sales/lead/ACwAAAhg9PcBG6FWic8UxvJN9CG6_TFgiPs9hu8,NAME_SEARCH,F6BZ',
                    'https://www.linkedin.com/sales/lead/ACwAAAU3Ek0B_iAUg6n_utSWHi5IGWhWNBPHuT8,NAME_SEARCH,k3pF',
                    'https://www.linkedin.com/sales/lead/ACwAAADqZ7EB6-YJspHvY1M3pwpxyqvf1qvxX-4,NAME_SEARCH,F9Q_'
                ]
                # for lk_url in lk_urls:
                # Close the browser window
                engine.quit()
            else:
                print('No data.')

    # exit()

