from datetime import datetime
from bson import ObjectId
from deep import main,logging
from pymongo import MongoClient
from urllib.parse import urlparse
import re
import traceback
import csv
import tldextract

main_url = [
    "docs.google.com",
    "sites.google.com",
    "play.google.com",
    "api.whatsapp.com",
]
email_domains = [
    "wixsite.com",
    "start.page",
    "site123.me",
    "notion.site",
    "blogspot.com",
]

def get_email_domain(url):
    if any(domain in url for domain in main_url):
        return ''
        
    replacements = {
        "///": "//",
        "http//:": "",
        "https//:": "",
        "http;//": "",
        "https;//": "",
        "http//": "",
        "https//": "",
        "http;/": "",
        "https;/": "",
        ";": ":",
        ",": ".",
    }

    for old, new in replacements.items():
        url = url.replace(old, new)
    # url = url.replace("http//", "")
    # print("url",url)
    url = url.lower().strip()

    # Fix malformed schemes
    if url.startswith("http:/") and not url.startswith("http://"):
        url = url.replace("http:/", "http://", 1)
    elif url.startswith("https:/") and not url.startswith("https://"):
        url = url.replace("https:/", "https://", 1)
    elif not url.startswith(("http://", "https://")):
        url = "https://" + url.lstrip('/')

    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path

        # Fallback to regex if no usable domain
        if '.' not in domain or domain.endswith('.'):
            match = re.search(r'([a-z0-9\-]+\.[a-z]{2,})', url)
            domain = match.group(1) if match else ''

        # Extract base domain (strips subdomain like www2)
        ext = tldextract.extract(domain)
        if ext.domain !='www' and ext.domain and ext.suffix:
            email_domain = f"{ext.domain}.{ext.suffix}"
            if any(domain in email_domain for domain in email_domains):
                return ''
            return email_domain
        else:
            return ''
    except:
        return ''  
            
# client = MongoClient('mongodb://developer:ah6M6vIz52YYJzy1@3.109.96.163:27017/e-finder?authSource=e-finder&readPreference=primary&serverSelectionTimeoutMS=20000&appname=mongosh%201.6.1&directConnection=true&ssl=false')
# logging.info("Connected to MongoDB")

# db = client["e-finder"]
# users = db["users"]
# company = db["company"]

# result = company.find({
#     'Website': {
#         '$exists': True
#     }, 
#     '$or': [
#         {
#             'email_domain': {
#                 '$exists': False
#             }
#         }, {
#             'email_domain': ''
#         }
#     ]
# })

# docs = list(result)  # convert cursor to list


# # Open CSV file for writing
# with open('company.csv', mode='w', newline='', encoding='utf-8') as file:
#     writer = csv.writer(file)
#     writer.writerow(['Company ID', 'Website', 'Extracted Email Domain'])  # Header

#     # Loop through MongoDB results and write to CSV
#     for doc in docs:
#         website = doc.get('Website')
#         domain = get_email_domain(website)

#         writer.writerow([str(doc.get('_id')), website, domain])

#         # Optional: Print to console
#         print(f"Company ID: {doc.get('_id')}")
#         print(f"Website: {website}")
#         print(f"Extracted Email Domain: {domain}")
#         print("-" * 40)
        
#         if domain:
#             # Update v6 field for processed users
#             company.update_many(
#                 {"_id": ObjectId(doc.get('_id'))},
#                 {"$set": {"email_domain": domain, "email_domain_verify": True}}
#             )
#             # exit()
    
# print(f"company= {len(docs)}")

  
    
# Example usage
urls = [
    "https;//www.dilipbuildcon.co.in",
    "http://http;/www.makhophilaacademy.co.za",
    "http://www.grupokalinangan,org",
    "http://https;//www.sefam.com",
    "http://http//:www.theroyal.co.za",
    "http://http;//portal.microsoftonline.com",
    "https:///libyanairlines.aero",
    "http://http;//www.americanvisionwindows.com",
    "http://www.nyc/gov/hra",
    "http://www.com",
    "http://http//www.ma-india.com",
    "http://www.optimozit.co.uk",
    "http://www2.admiralmetals.com",
    "http://",
    "http://n/a",
    "http://almech-facades",
    "http://www/tkietwarana.org",
    "http:/www./savioceramica.com",
    "http://www.",
    "www.",
    "http://Nil",
    "http:/www.pravara.com",
    "https://www.garg-associates.com"
]

# for url in urls:
#     print(url, "=>", get_email_domain(url))