from datetime import datetime
from urllib.parse import urlparse, parse_qs

import pytz
import re
import urllib.parse
import uuid
import random

desired_time_zone = pytz.timezone('Asia/Kolkata')  # Indian Standard Time


def mailsend_sleep_time():
    return random.randint(20, 30)


def formatted_time(format_type: str = '%Y-%m-%d %H:%M:%S'):
    # Get the current time in the desired time zone
    current_time_in_desired_zone = datetime.now(desired_time_zone)
    return current_time_in_desired_zone.strftime(format_type)


def encoded_string(original_string):
    encoded_str = urllib.parse.quote(original_string.strip())
    return encoded_str.replace('%20', '+')


def source_lang(url):
    country_lang_arr = {'kw': 'ar', 'in': 'in', 'sg': 'sg', 'au': 'au', 'uk': 'uk', 'ph': 'tl', 'vn': 'vi', 'mt': 'mt',
                        'hk': 'auto', 'gr': 'gr', 'bg': 'bg', 'si': 'si', 'ng': 'ng', 'mv': 'it', 'ch': 'de', 'be': 'be',
                        'pk': 'pk', 'th': 'th', 'kr': 'ko', 'ca': 'ca', 'ae': 'ar', 'cn': 'auto', 'za': 'it', 'dz': 'ar',
                        'jp': 'ja'}
    my_list = ['www', 'linkedin', 'it']
    match = re.search(r'https://([a-zA-Z-]+)\.linkedin\.com', url)
    if match:
        subdomain = match.group(1)
        if subdomain in my_list:
            return 'it'
        elif subdomain in country_lang_arr:
            return country_lang_arr[subdomain]
        else:
            return 'auto'
    else:
        return 'it'


def remove_keyword(modified_h3):
    values_to_remove = [" - LinkedIn", " | LinkedIn"]
    for value in values_to_remove:
        if modified_h3.endswith(value):
            modified_h3 = modified_h3.replace(value, "")
            break
    return modified_h3


def extract_url(b_url):
    parsed_url = urlparse(b_url)
    try:

        # Get the query string parameters as a dictionary
        query_params = parse_qs(parsed_url.query)

        # Print the query string parameters
        return query_params['uddg'][0]
    except KeyError:
        return b_url


def filter_name(name):
    name = name.lower()
    name_br = name.split(' ')

    return list(filter(lambda x: x != '', name_br))


def get_mac_address():
    mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) for elements in range(5, -1, -1)])
    return mac
