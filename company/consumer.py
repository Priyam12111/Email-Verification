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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from lib.helpers import db

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
from lib.mongo_connection import mg_list, mg_one, mg_update, mg_aggregate
# from lib.helpers import get_company_to_verify

filename = formatted_time("%Y%m%d%H%M%S")

# cloudamqp_url = 'amqps://ehwegmmg:ueyUmQ9kgBB8B5UkWjFaPZBW2xsqleBt@puffin.rmq2.cloudamqp.com/ehwegmmg'
cloudamqp_url = 'amqps://ailtamhb:U9IxT1znd6_wjBhXaAx-AO0YE44FqRMJ@gorilla.lmq.cloudamqp.com/ailtamhb'
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
    if isinstance(domain_url, str):
        return domain_url.strip()
    else:
        return None


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
        return '.'.join(domain_parts)
    else:
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

# def create_driver(proxy=None):
#     opt = webdriver.ChromeOptions()

#     opt.add_experimental_option("debuggerAddress", "localhost:8989")
#     opt.add_argument('--disable-blink-features=AutomationControlled')

#     # Enhanced fingerprint protection
#     opt.add_argument("--disable-webgl")  # WebGL fingerprint protection
#     opt.add_argument("--disable-site-isolation-trials")
#     opt.add_argument("--disable-features=IsolateOrigins,site-per-process")
#     opt.add_argument("--disable-3d-apis")
#     opt.add_argument("--disable-web-security")
#     opt.add_argument("--disable-notifications")
#     opt.add_argument(f"--user-data-dir={os.path.expanduser('~')}/chrome_profiles/profile_{random.randint(1,100)}")
#     # Set realistic user agent
#     user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
#     opt.add_argument(f'--user-agent={user_agent}')

#     driver = webdriver.Chrome(
#         service=Service(ChromeDriverManager().install()),
#         options=opt
#     )

#     driver.implicitly_wait(5)
#     driver.execute_cdp_cmd(
#         "Page.addScriptToEvaluateOnNewDocument", {
#             "source": """
#             Object.defineProperty(navigator, 'webdriver', {
#                 get: () => undefined
#             });
#             """
#         }
#     )
#     return driver

CHROME_PATH = ChromeDriverManager().install()

def create_driver(proxy=None):
    opt = webdriver.ChromeOptions()

    # REMOVE this unless you manually launch Chrome with --remote-debugging-port=8989
    # opt.add_experimental_option("debuggerAddress", "localhost:8989")

    opt.add_argument('--disable-blink-features=AutomationControlled')
    opt.add_argument("--disable-site-isolation-trials")
    opt.add_argument("--disable-features=IsolateOrigins,site-per-process")
    opt.add_argument("--disable-3d-apis")
    opt.add_argument("--disable-web-security")
    opt.add_argument("--disable-notifications")
    # opt.add_argument("--headless=new")

    # Use one stable profile (or headless)
    opt.add_argument(f"--user-data-dir={os.path.expanduser('~')}/chrome_profiles/selenium_profile")
    # opt.add_argument("--headless=new")  # uncomment if UI not needed
    opt.add_argument("--disable-gpu")

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    opt.add_argument(f'--user-agent={user_agent}')

    driver = webdriver.Chrome(service=Service(CHROME_PATH), options=opt)
    driver.implicitly_wait(3)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    })
    try:
        driver.maximize_window()
    except Exception:
        pass
    return driver


def is_directory_or_social(url: str) -> bool:
    """Filter out directories and social sites that are not official websites."""
    bad_hosts = [
        "linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com",
        "youtube.com", "crunchbase.com", "angel.co", "glassdoor.com",
        "indeed.com", "g2.com", "capterra.com", "wikipedia.org",
        "ycombinator.com", "github.com", "play.google.com", "apps.apple.com"
    ]
    u = url.lower()
    return any(host in u for host in bad_hosts)


def prefer_root_like(url: str) -> int:
    """
    Small heuristic to prefer likely homepages:
      - penalize deep paths and query
      - reward pure root or short paths like '/' or '/home'
    Returns a score to add to similarity (0..20).
    """
    try:
        p = urlsplit(url)
        path = p.path or "/"
        bonus = 0
        if path in ["/", "/home", "/en", "/index.html", "/index.htm"]:
            bonus += 12
        # penalize long paths
        depth = len([seg for seg in path.split("/") if seg])
        if depth == 0:
            bonus += 6
        elif depth == 1:
            bonus += 3
        # penalize heavy querystrings
        if p.query:
            bonus -= 4
        # de-bonus for obvious blog/news subpaths
        if any(seg in path.lower() for seg in ["blog", "news", "about", "careers"]):
            bonus -= 2
        return max(0, bonus)
    except:
        return 0


def hostname_contains_company(host: str, company_name: str) -> int:
    """
    Reward if hostname contains normalized company tokens (adds up to ~10).
    """
    try:
        host = host.lower()
        # basic normalization of company name
        tokens = re.sub(r"[^a-z0-9 ]", " ", company_name.lower()).split()
        tokens = [t for t in tokens if len(t) > 2]  # ignore tiny bits
        score = 0
        for t in tokens[:3]:
            if t in host:
                score += 4
        return min(score, 12)
    except:
        return 0


def process(data, engine):
    # sleep_time_arr = [1, 2, 3, 4, 5, 6, 7]
    sleep_time_arr = [0.5, 1, 1.5]
    data_add = 0
    data_exist = 0
    for index, row in enumerate(data):
        chk_company = {'status': False}
        if not chk_company['status']:
            print("here1")
            # engine = create_driver()
            # engine.maximize_window()
            company_name = row['name']
            try:
                try:
                    # --- DuckDuckGo search for company domain (instead of LinkedIn) ---
                    com_name = encoded_string(company_name.replace(' | ', ' '))
                    random_sleep_time = random.choice(sleep_time_arr)
                    sleep(random_sleep_time)

                    # Use DuckDuckGo HTML endpoint (works well without heavy JS)
                    # Avoid social results; bias towards official website.
                    g_url = (
                        f'https://html.duckduckgo.com/html'
                        f'?q={com_name}+official+website+-linkedin+-facebook+-twitter+-instagram+-crunchbase'
                    )
                    print(f"🔍 Searching DuckDuckGo: {g_url}")
                    engine.get(g_url)
                    # sleep(5)
                    try:
                        WebDriverWait(engine, 6).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.result__title a'))
                        )
                    except Exception:
                        pass
                    # search_results = engine.find_elements(By.CSS_SELECTOR, '.result__title a')

                    # Collect top results
                    # Selector for DDG HTML version results
                    search_results = engine.find_elements(By.CSS_SELECTOR, '.result__title a')
                    values = {}
                    scored_meta = {}

                    for s_index, a in enumerate(search_results[:20]):
                        raw_href = a.get_attribute('href') or ''
                        link = extract_url(raw_href).strip()
                        title = (a.text or '').strip()

                        if not link:
                            continue
                        if is_directory_or_social(link):
                            continue

                        # Normalize URL - keep only root for fair comparison
                        try:
                            parsed = urlsplit(link)
                            host = parsed.netloc or ''
                            root_url = f"{parsed.scheme}://{host}/".rstrip('/')
                        except Exception:
                            root_url = link
                            host = ''

                        # Similarity on title and company name
                        sim_score = match_string_percentage(company_name, title)

                        # Heuristics: prefer clean root/homepage URLs, or where hostname contains company tokens
                        bonus = prefer_root_like(link) + hostname_contains_company(host, company_name)

                        # Add small extra weight if path seems like a homepage
                        if parsed.path in ('', '/', '/home', '/index.html'):
                            bonus += 5

                        final_score = sim_score + bonus

                        # Accumulate max score per root domain (so subpages don’t override homepage)
                        if root_url not in values or final_score > values[root_url]:
                            values[root_url] = final_score
                            scored_meta[root_url] = {
                                "title": title,
                                "sim_score": sim_score,
                                "bonus": bonus,
                                "raw_link": link
                            }

                    extra_data = {'status': True, 'dt_status': True, 'modifiedAt': str(formatted_time())}

                    if len(values):
                        # Pick best-scoring root URL
                        best_url = max(values, key=values.get)
                        best_score = values[best_url]
                        meta = scored_meta.get(best_url, {})

                        print(
                            f"🏆 Best match: {best_url} | final={best_score} "
                            f"(title_sim={meta.get('sim_score')} bonus={meta.get('bonus')})"
                        )

                        # Use the original link for domain verification
                        best_url_clean = url_remove_query(meta.get('raw_link', best_url))
                        extra_data['publicUrl'] = best_url_clean

                        try:
                            dom = verify_domain(best_url_clean)
                            if dom:
                                extra_data['email_domain'] = dom
                                extra_data['email_domain_verify'] = True
                                print(f"✅ Verified domain: {dom}")
                            else:
                                extra_data['dt_reason'] = 'Unable to extract domain from best URL'
                                print("❌ Unable to extract domain properly")
                        except Exception as e:
                            extra_data['dt_reason'] = f'Verify domain error: {e}'
                            print(f"Error verifying domain: {e}")

                        # Optional: light mouse movement to appear human
                        try:
                            actions = ActionChains(engine)
                            actions.move_by_offset(20, 200).click().perform()
                            sleep(1)
                        except Exception:
                            pass

                    else:
                        extra_data['dt_reason'] = 'Not found in DuckDuckGo'
                        print("⚠️ No valid results found on DuckDuckGo.")

                    # try:
                    #     print('Adding data: ', extra_data)
                    #     return extra_data.get('email_domain')
                    # except Exception as e:
                    #     print(f"Error: {e}")
                    #     return None
                    try:
                        print('Adding data: ', extra_data, row['_id'])
                        mg_update(COLLECTION_COMPANY, {'_id': ObjectId(row['_id'])}, db, extra_data)
                    except Exception as e:
                        print("Mongo update failed:", e)
                        raise  # lets main() nack/requeue
    
                    # print(industry_txt)
                    # exit()
    
                except WebDriverException as e:
                    # Handle the WebDriverException (connection timeout error)
                    print("Error:", e)
                    print("The connection timed out. Check your internet connection or the target website.")
                    raise

            except IndexError:
                # show error
                print('Index does NOT exist')

    print(f"Added: {data_add}, Exists: {data_exist}")


def main():
    engine = create_driver()
    print('engine created')

    parameters = pika.URLParameters(cloudamqp_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_qos(prefetch_count=1)

    try:
        while True:
            print("INFO: Polling to RMQ...")
            # IMPORTANT: auto_ack=False
            method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=False)

            if method_frame and body:
                detail_obj = json.loads(body.decode('utf-8'))
                print("INFO : Picked data from queue...starting the process")
                try:
                    process([detail_obj], engine)
                    channel.basic_ack(method_frame.delivery_tag)   # ack only on success
                    print("INFO : Completed the process for one company")
                except WebDriverException:
                    print("⚠️ Browser crashed, restarting driver…")
                    try: engine.quit()
                    except: pass
                    engine = create_driver()
                    channel.basic_nack(method_frame.delivery_tag, requeue=True)  # requeue the message
                except Exception as e:
                    print("❌ Processing error:", e)
                    channel.basic_nack(method_frame.delivery_tag, requeue=True)
            else:
                print("No messages in the queue.")
                time.sleep(1)  # short backoff

    except KeyboardInterrupt:
        pass
    finally:
        try: engine.quit()
        except: pass
        try: channel.close()
        except: pass
        try: connection.close()
        except: pass


if __name__ == "__main__":
    main()
