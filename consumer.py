import csv

from bson import ObjectId

from lib.u_date import formatted_time, encoded_string, source_lang, remove_keyword, extract_url, get_mac_address

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException

from time import sleep

import difflib
import pika
import json
import time
import re

import os
import random

from urllib.parse import urlsplit, urlunparse, urlparse

from lib.constants import COLLECTION_COMPANY, COLLECTION_SYSTEM
# from lib.mongo_connection import mg_list, mg_one, mg_update, mg_aggregate
# from lib.helpers import get_company_to_verify

filename = formatted_time("%Y%m%d%H%M%S")

cloudamqp_url = 'amqps://ehwegmmg:ueyUmQ9kgBB8B5UkWjFaPZBW2xsqleBt@puffin.rmq2.cloudamqp.com/ehwegmmg'
queue_name = "company_details"


def Write(filenam, cont):  # Updating the vars and Slots
    with open(f'{filenam}.csv', 'a', encoding='utf-8') as f:
        try:
            f.write(f'{cont}')
        except:
            pass
        f.close()


def url_remove_query(url):
    parsed_url = urlparse(url)
    parsed_url = parsed_url._replace(query=None)
    return urlunparse(parsed_url)


def main_domain(url):
    parsed_url = urlsplit(url)
    return parsed_url.netloc


def verify_domain2(url):
    if not url.startswith("https://") and not url.startswith("http://"):
        url = "https://" + url
    parsed_url = urlsplit(url)
    domain_url = parsed_url.netloc.lower()
    if domain_url.startswith('www.'):
        domain_url = domain_url[4:]

    domain_postfix = ['gov.au', 'com.au', 'org.au', 'net.au', 'asn.au', 'com.py', 'com.sg', 'gov.in',
                      'com.sg', 'edu.au', 'co.uk', 'co.in']
    d_br = domain_url.split('.')
    if len(domain_url.split('.')) == 3 and (d_br[len(d_br) - 2] + "." + d_br[len(d_br) - 1]) in domain_postfix:
        domain_url = domain_url
    elif len(domain_url.split('.')) >= 3:
        domain_url = False
    # return domain_url.strip()
    if isinstance(domain_url, str):
        return domain_url.strip()
    else:
        # Handle the case where domain_url is not a string
        return None  # or some appropriate value


def verify_domain(url):
    # Ensure the URL starts with a valid scheme (http or https)
    if not url.startswith("https://") and not url.startswith("http://"):
        url = "https://" + url

    # Parse the URL and extract the domain
    parsed_url = urlsplit(url)
    domain_url = parsed_url.netloc.lower()

    # Remove 'www.' if present
    if domain_url.startswith('www.'):
        domain_url = domain_url[4:]

    # Split the domain into parts and return the domain including subdomains
    domain_parts = domain_url.split('.')
    
    if len(domain_parts) >= 2:
        # Return the domain and subdomains
        return '.'.join(domain_parts)
    else:
        # If the domain is invalid
        return None
    

def sub_domain(url):
    pattern = r"https?://([^/.]+)\."
    match = re.search(pattern, url)
    my_list = ['www', 'linkedin']
    if match:
        subdomain = match.group(1)
        if subdomain in my_list:
            return url
        else:
            return url.replace(subdomain, 'in')
    else:
        return url


def excerpt_string(text):
    parts = text.split('  ', 2)
    if len(parts) >= 2:
        excerpt_str = text.replace(parts[len(parts) - 1], '')
    else:
        excerpt_str = text
    return excerpt_str.strip()


def match_string_percentage(string1, string2):
    matcher = difflib.SequenceMatcher(None, string1.lower(), string2.lower())
    match_percentage = round(matcher.ratio() * 100, 2)
    return match_percentage


# List of user agents
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

def create_driver(proxy=None):
    opt = webdriver.ChromeOptions()

    opt.add_experimental_option("debuggerAddress", "localhost:8989")
    opt.add_argument('--disable-blink-features=AutomationControlled')
    
    # Enhanced fingerprint protection
    opt.add_argument("--disable-webgl")  # WebGL fingerprint protection
    opt.add_argument("--disable-site-isolation-trials")
    opt.add_argument("--disable-features=IsolateOrigins,site-per-process")
    opt.add_argument("--disable-3d-apis")
    opt.add_argument("--disable-web-security")
    opt.add_argument("--disable-notifications")
    opt.add_argument(f"--user-data-dir={os.path.expanduser('~')}/chrome_profiles/profile_{random.randint(1,100)}")
    # Set realistic user agent
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    opt.add_argument(f'--user-agent={user_agent}')

    # Disable automation flags
    # opt.add_experimental_option("excludeSwitches", ["enable-automation", "load-extension"])
    # opt.add_experimental_option("useAutomationExtension", False)

    # Optional: Use existing browser profile (create one manually first)
    # opt.add_argument("--user-data-dir=/path/to/your/chrome/profile")

    # Optional: For headless mode (uncomment if needed)
    # opt.add_argument('--headless=new')  # New headless mode in Chrome 109+
    # opt.add_argument('--window-size=1920,1080')  # Set resolution when headless
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), 
        options=opt
    )

    driver.implicitly_wait(5)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        }
    )
    return driver



def process(data):
    sleep_time_arr = [1, 2, 3, 4, 5, 6, 7]
    data_add = 0
    data_exist = 0
    for index, row in enumerate(data):
        chk_company = {
            'status': False,}
        if not chk_company['status']:
            engine = create_driver()
            engine.maximize_window()
            company_name = row['name']
            try:
                try:
                    com_name = encoded_string(company_name.replace(' | ', ' '))
                    random_sleep_time = random.choice(sleep_time_arr)
                    sleep(random_sleep_time)
                    g_url = f'https://html.duckduckgo.com/html?q=%27{com_name}%27+linkedin.com%2Fcompany'
                    engine.get(g_url)
                    sleep(5)
                    
                    # Extract and print top 10 links
                    search_results = engine.find_elements('css selector', '.result__title')
                    values = {}
                    # Check if "company/school" is present in the lowercase URL
                    check_keywords = [
                        "linkedin.com/company",
                        "linkedin.com/school"
                    ]
                    for s_index, result in enumerate(search_results[:20]):
                        link = extract_url(result.find_element('css selector', 'a').get_attribute('href'))
                        h3 = result.find_element('css selector', 'a').get_attribute('innerText')

                        # print(h3, link)
                        pattern = re.compile("|".join(check_keywords))
                        match_keyword = pattern.search(link.lower())
                        if match_keyword:
                            values[link] = match_string_percentage(company_name, remove_keyword(h3))

                    sleep(1)
                    extra_data = {'status': True, 'dt_status': True, 'modifiedAt': str(formatted_time())}
                    if len(values):
                        # Find the highest value and its key
                        link = max(values, key=values.get)
                        highest_value = values[link]
                        print("Highest value:", str(formatted_time()), highest_value, link)

                        if highest_value > 93:
                            # add data
                            link = url_remove_query(link)
                            replace_txt = main_domain(link).replace('.', '-')
                            final_domain = link.replace(main_domain(link), f'{replace_txt}.translate.goog')
                            if 'linkedin.com/school' in link:
                                translate_url = link
                            else:
                                translate_url = f'{final_domain}?_x_tr_sl={source_lang(link)}&_x_tr_tl=en&_x_tr_hl=en&_x_tr_pto=sc'

                            print(f'urls 1: {final_domain} == {translate_url} = {link}')

                            engine.get(translate_url)
                            sleep(5)
                            engine.implicitly_wait(4)
                            # from selenium.webdriver.common.action_chains import ActionChains
                            actions = ActionChains(engine)
                            actions.move_by_offset(20, 200).click().perform()

                            sleep(2)
                            engine.implicitly_wait(2)
                            extra_data['publicUrl'] = link
                            # search_results2 = engine.find_elements('css selector', '.tF2Cxc')
                            elements = engine.find_elements('css selector',
                                                            '.mb-2.flex.papabear\\:mr-3.mamabear\\:mr-3.babybear\\:flex-wrap')
                            sleep(2)
                            industries_arr = ['Sector', 'Professional field', 'industry']
                            types_arr = ['Guy', 'type', 'Art', 'category']
                            founded_arr = ['Founding date', 'Established', 'establish', 'Foundedwhen', 'Foundation',
                                            'Foundedin']
                            specialties_arr = ['Specializations', 'Sectors of expertise', 'bailiwick', 'field', 'Areas',
                                                'Specialization', 'Specialty']
                            headquarters_arr = ['Head office', 'Site', 'Thirst']
                            website_arr = ['website']
                            companysize_arr = ['scale', 'Sizeofthecompany']
                            # print(f'we advance data{enumerate(elements)}')
                            for index2, result2 in enumerate(elements):
                                dt = result2.find_element('css selector', 'div dt')
                                dd = result2.find_element('css selector', 'div dd')
                                key_db = dt.text
                                if key_db in industries_arr:
                                    arr_key = 'Industry'
                                elif key_db in types_arr:
                                    arr_key = 'Type'
                                elif key_db in founded_arr:
                                    arr_key = 'Founded'
                                elif key_db in headquarters_arr:
                                    arr_key = 'Headquarters'
                                elif key_db in website_arr:
                                    arr_key = 'Website'
                                elif key_db in specialties_arr:
                                    arr_key = 'Specialties'
                                elif key_db in companysize_arr:
                                    arr_key = 'Companysize'
                                else:
                                    arr_key = key_db.replace(' ', '')
                                # print(f"Link2w3 {index2 + 1}: {dt.text}====={dd.text}")
                                if arr_key:
                                    extra_data[arr_key] = dd.text
                                # print(f'{dd.text}=={dt.text}')
                            # exit()

                            try:
                                location = engine.find_element(by=By.XPATH,
                                                                value=f'/html/body/main/section[1]/section/div/div[2]/div[1]/h3')
                                extra_data['location'] = excerpt_string(location.get_attribute('innerText'))
                                err = ''
                            except NoSuchElementException:
                                # Handle the case where the element does not exist
                                err = "Element location: /html/body/main/section[1]/section/div/div[2]/div[1]/h3"
                            website_txt = extra_data.get('Website', None)
                            if website_txt is not None:
                                extra_data['email_domain_verify'] = True
                                try:
                                    if verify_domain(extra_data['Website']):
                                        extra_data['email_domain'] = verify_domain(extra_data['Website'])
                                except Exception as e:
                                    print(f"Error: {e}")

                            extra_data['dt_reason'] = ''
                            industry_txt = extra_data.get('Industry', None)

                            if industry_txt is None:
                                # //update not found on search engine
                                extra_data['dt_reason'] = 'Not company details in linkedin'
                                print(f"320The word 'company' is not present in the URL. {extra_data['dt_reason']}")
                        else:
                            # //update not found on google
                            extra_data['dt_reason'] = f'Not match {highest_value}% in google'
                            print(f"324The word 'company' is not present in the URL. {extra_data['dt_reason']}")
                    else:
                        extra_data['dt_reason'] = 'Not found in google'
                        print(f"333The word 'company' is not present in the URL. {extra_data['dt_reason']}")
                    # print('done', extra_data)
                    try:
                        print('Adding data: ', extra_data)
                        return extra_data['email_domain']
                    except Exception as e:
                        print(f"Error: {e}")
                        return None
                    # print('Adding data: ', extra_data, row['_id'])
                    # mg_update(COLLECTION_COMPANY, {'_id': ObjectId(row['_id'])}, extra_data)
                    # print(industry_txt)
                    # exit()
                except WebDriverException as e:
                    # Handle the WebDriverException (connection timeout error)
                    print("Error:", e)
                    print("The connection timed out. Check your internet connection or the target website.")
                    engine.quit()
            
            except IndexError:
                # show error
                print('Index does NOT exist')
            finally:
                engine.quit()
    
    print(f"Added: {data_add}, Exists: {data_exist}")



def main():
    try:
        while True:
            try:
                print("INFO: Polling to RMQ...")
                parameters = pika.URLParameters(cloudamqp_url)
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()

                channel.queue_declare(queue=queue_name, durable=True)

                method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)

                if method_frame:
                    detail_obj = json.loads(body.decode('utf-8'))

                    print("INFO : Picked data from queue...starting the process")
                    process([detail_obj])

                    print("INFO : Completed the process for one company")
                
                else:
                    print("No messages in the queue.")

                try:
                    connection.close()
                    print("INFO : Polling Connection closed")
                except Exception as e:
                    pass

                print("INFO : Checking the queue again after 5s...")
                time.sleep(5)
            
            except Exception as e:
                pass

    except KeyboardInterrupt:
        try:
            connection.close()
        except Exception as e:
            pass


if __name__ == "__main__":
    main()

    

    

    

