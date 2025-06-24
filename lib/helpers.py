from lib.objects import *
from imapclient import IMAPClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from configs.config import Config
import dns.resolver
import smtplib
import logging
import smtplib
import email
import re
import time
from bson import ObjectId

log = logging.getLogger(__name__)


def current_time():
    current_datetime = datetime.now()
    formatted_time = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time



def get_mail_template_by_url_id(url_id):
    try:
        pipeline = [
            {
                "$match": {
                    "_id": url_id
                }
            },
            {
                "$lookup": {
                    "from": "mail-templates",
                    "localField": "mailTemplateId",
                    "foreignField": "_id",
                    "as": "mail_template"
                }
            },
            {
                "$unwind": "$mail_template"
            },
            {
                "$project": {
                    "_id": 0,
                    "mail_template": 1
                }
            }
        ]

        result = list(url_collection.aggregate(pipeline))
        
        if result:
            return result[0]["mail_template"]
        else:
            return "No data found"
    
    except Exception as e:
        log.error(str(e))
        return ""



def get_users_to_verify_email(system_offset, instance_offset, limit=5):
    try:
        users = []

        pipeline = [
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'refCompanyId',
                    'as': 'joinedUsers'
                }
            },
            {
                '$unwind': '$joinedUsers'
            },
            {
                '$match': {
                    'email_domain': {'$nin': ['', False], '$exists': True},
                    # 'joinedUsers.patternStatus': False,
                    'joinedUsers.uStatus': False,
                    # 'joinedUsers.match_pattern_level': {'$exists': False},
                    'joinedUsers.business_email': {'$nin': ['', False], '$exists': False},
                    'joinedUsers.fullName': {'$regex': re.compile(f"^[a-zA-Z]+ [a-zA-Z]+$")},
                    # 'joinedUsers.send_email_to_verify': {'$ne': True},
                    'joinedUsers.inQueue': {'$ne': True}
                }
            },
            {
                '$project': {
                    'email_domain': 1,
                    'joinedUsers': 1
                }
            },
            {
                '$skip': system_offset * limit + instance_offset
            },
            {
                '$limit': limit
            }
        ]

        result = company_collection.aggregate(pipeline)

        for doc in result:
            users.append(doc)

        return users
    
    except Exception as e:
        log.error(str(e))
        return []

def get_users_to_verify_email_urgent(company_id, system_offset, instance_offset, limit=5):
    try:
        users = []

        pipeline = [
            {
                '$match': {
                    '_id': company_id
                }
            },
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'refCompanyId',
                    'as': 'joinedUsers'
                }
            },
            {
                '$unwind': '$joinedUsers'
            },
            {
                '$match': {
                    'email_domain': {'$exists': True, '$ne' : ""},
                    'joinedUsers.uStatus': False,
                    'joinedUsers.fullName': {'$regex': re.compile(f"^[a-zA-Z]+ [a-zA-Z]+$")},
                    'joinedUsers.send_email_to_verify': {'$ne': True},
                    'joinedUsers.inQueue': {'$ne': True},
                    # 'joinedUsers.business_email': {'$exists': False, '$ne':""},
                    "$or": [
                        { 'joinedUsers.business_email': {"$exists":False} }, 
                        { 'joinedUsers.business_email': "" }  
                    ]
                }
            },
            
            {
                '$project': {
                    'email_domain': 1,
                    'joinedUsers': 1
                }
            },
            {
                '$skip': system_offset * limit + instance_offset
            },
            {
                '$limit': limit
            }
        ]

        result = company_collection.aggregate(pipeline)

        for doc in result:
            users.append(doc)

        return users
    
    except Exception as e:
        log.error(str(e))
        return []


def get_users_to_verify_email_l3(system_offset, instance_offset, limit=5):
    try:
        users = []

        pipeline = [
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'refCompanyId',
                    'as': 'joinedUsers'
                }
            },
            {
                '$unwind': '$joinedUsers'
            },
            {
                '$match': {
                    'email_domain': {'$exists': True, '$ne' : ""},
                    # 'joinedUsers.patternStatus': False,
                    'joinedUsers.uStatus': False,
                    # 'joinedUsers.match_pattern_level': {'$exists': False},
                    'joinedUsers.business_email': {'$exists': False},
                    'joinedUsers.fullName': {'$regex': re.compile(f"^[a-zA-Z]+ [a-zA-Z]+$")},
                    'joinedUsers.send_email_to_verify': True,
                    'joinedUsers.checked_pattern' : {'$exists': False},
                    'joinedUsers.inQueue': {'$ne': True}
                }
            },
            {
                '$project': {
                    'email_domain': 1,
                    'joinedUsers': 1
                }
            },
            {
                '$skip': system_offset * limit + instance_offset
            },
            {
                '$limit': limit
            }
        ]

        result = company_collection.aggregate(pipeline)

        for doc in result:
            users.append(doc)

        return users
    
    except Exception as e:
        log.error(str(e))
        return []


def get_users_to_verify_email_lnd(system_offset, instance_offset, limit=5):
    try:
        users = []

        pipeline = [
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'refCompanyId',
                    'as': 'joinedUsers'
                }
            },
            {
                '$unwind': '$joinedUsers'
            },
            {
                '$match': {
                    # 'email_domain': {'$exists': True, '$ne' : ""},
                    'email_domain': {
                        '$exists': True, 
                        '$nin': [
                            '', False
                        ]
                    },
                    'joinedUsers.uStatus': False,
                    'joinedUsers.business_email': {'$exists': False, '$nin':['']},
                    'joinedUsers.fullName': {'$regex': re.compile(f"^[a-zA-Z]+ [a-zA-Z]+$")},
                    'joinedUsers.send_email_to_verify': {'$ne': True},
                    'joinedUsers.checked_pattern' : {'$exists': False},
                    'joinedUsers.inQueue': {'$ne': True}
                }
            },
            {
                '$lookup': {
                    'from': 'urls',
                    'localField': 'joinedUsers.url_id',
                    'foreignField': '_id',
                    'as': 'url'
                }
            },
            {
                '$match': {
                    'url.ReferenceName': 'L&D'
                }
            },
            {
                '$project': {
                    'email_domain': 1,
                    'joinedUsers': 1
                }
            },
            {
                '$skip': system_offset * limit + instance_offset
            },
            {
                '$limit': limit
            }
        ]

        result = company_collection.aggregate(pipeline)

        for doc in result:
            users.append(doc)

        return users
    
    except Exception as e:
        log.error(str(e))
        return []
    

def get_users_to_verify_email_lnd_l3(system_offset, instance_offset, limit=5):
    try:
        users = []

        pipeline = [
            {
                '$lookup': {
                    'from': 'users',
                    'localField': '_id',
                    'foreignField': 'refCompanyId',
                    'as': 'joinedUsers'
                }
            },
            {
                '$unwind': '$joinedUsers'
            },
            {
                '$match': {
                    'email_domain': {'$exists': True, '$ne' : ""},
                    'joinedUsers.uStatus': False,
                    'joinedUsers.business_email': {'$exists': False},
                    'joinedUsers.fullName': {'$regex': re.compile(f"^[a-zA-Z]+ [a-zA-Z]+$")},
                    'joinedUsers.send_email_to_verify': True,
                    'joinedUsers.checked_pattern' : {'$exists': False},
                    'joinedUsers.inQueue': {'$ne': True}
                }
            },
            {
                '$lookup': {
                    'from': 'urls',
                    'localField': 'joinedUsers.url_id',
                    'foreignField': '_id',
                    'as': 'url'
                }
            },
            {
                '$match': {
                    'url.ReferenceName': 'L&D'
                }
            },
            {
                '$project': {
                    'email_domain': 1,
                    'joinedUsers': 1
                }
            },
            {
                '$skip': system_offset * limit + instance_offset
            },
            {
                '$limit': limit
            }
        ]

        result = company_collection.aggregate(pipeline)

        for doc in result:
            users.append(doc)

        return users
    
    except Exception as e:
        log.error(str(e))
        return []


def send_email(receiver_email, subject, message, username, password):
    try:
        sender_email = username
        smtp_server = 'smtp.gmail.com'
        smtp_port = 465
        smtp_username = username
        smtp_password = password

        # Build the email message
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText(message, 'html'))
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = receiver_email

        # Connect to the SMTP server and send the email
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        return True
    
    except Exception as e:
        log.error(str(e))
        return False



def verify_genuine_email(email_to_test, username, password, num_emails=100):
    try:
        imap_server = 'smtp.gmail.com'

        with IMAPClient(imap_server) as client:
            client.login(username, password)

            folder_name = "INBOX"

            if folder_name not in [name[2] for name in client.list_folders()]:
                print(f"INFO : Folder '{folder_name}' not found.")
                return []
            
            print(f"INFO : Checking in folder {folder_name}")

            client.select_folder(folder_name)

            messages = client.search(['FROM', 'mailer-daemon@googlemail.com'])

            latest_messages = messages[-min(num_emails, len(messages)):]

            emails = []

            for msg_id, msg_data in client.fetch(latest_messages, ['RFC822']).items():
                raw_email = msg_data[b'RFC822']
                email_message = email.message_from_bytes(raw_email)
                emails.append({
                    'From': email_message['From'],
                    'To': email_message['To'],
                    'Subject': email_message['Subject'],
                    'Date': email_message['Date'],
                    'Body': email_message.get_payload(),
                })

            verification_text = f"Your message wasn't delivered to {email_to_test}"

            for email_item in emails:
                body_parts = email_item['Body']
                for part in body_parts:
                    full_payload = part.get_payload()
                    if isinstance(full_payload, list):
                        for subpart in full_payload:
                            minipart = subpart.get_payload()
                            if isinstance(minipart, list):
                                for micropart in minipart:
                                    possible_text = micropart.get_payload()
                                    if verification_text in possible_text:
                                        return False
                                    else:
                                        continue
                            else:
                                pass
                    else:
                        pass

            client.logout()
            return True
    
    except Exception as e:
        log.error(str(e))
        return False



def validate_email_format(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(pattern, email):
        return True
    else:
        return False



def update_user(user_id, update_data):
    try:
        users_collection.update_one(
            {"_id": user_id},
            {"$set": update_data}
        )
        print("User updated successfully")
    except Exception as e:
        print("Error updating user:", str(e))


def update_duplicate_users(leadUrl, first_valid_id, update_data):
    try:
        users_collection.update_many(
            {"leadUrl": leadUrl, "_id":{"$ne": ObjectId(first_valid_id)}},
            {"$set": update_data}
        )
        print("User updated successfully")
    except Exception as e:
        print("Error updating user:", str(e))
        
        
def update_users(url_id, update_data):
    try:
        users_collection.update_many(
            {"url_id": url_id},
            {"$set": update_data}
        )
        print("User updated successfully")
    except Exception as e:
        print("Error updating user:", str(e))

def get_urls(cond):
    try:
        url_list = []
        pipeline = [
            {
                "$match": cond
            },
        ]
        result = url_collection.aggregate(pipeline)

        for doc in result:
            url_list.append(doc)

        return url_list
    
    except Exception as e:
        print("Error url:", str(e))


def get_duplicate():
    try:
        data = []
        pipeline = [
            {
                "$group": {
                    "_id": "$leadUrl",  # Group by the 'leadUrl' field
                    "business_email": {"$first": "$business_email"},
                    "_id1": {"$first": "$_id"},
                    "allIds": {"$push": {
                        "_id": "$_id",
                        "email": "$business_email"
                    }},
                    # "allIds2": {"$push": "$business_email"},
                    "count": {"$sum": 1}  # Count the number of documents for each 'leadUrl'
                }
            },
            # Match groups where 'count' is greater than 1
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            # Sort by 'count' in descending order
            # {
            #     "$sort": {
            #         "count": -1
            #     }
            # }
            { "$sort": { "_id": 1 } },
            # { $skip: 52000 },
            # { "$limit": 1 }
        ]
        result = users_collection.aggregate(pipeline)

        for doc in result:
            data.append(doc)

        return data
    
    except Exception as e:
        print("Error url:", str(e))


def get_duplicate_lead():
    try:
        data = []
        pipeline = [
            {
                "$group": {
                    "_id": "$leadUrl",  # Group by the 'leadUrl' field
                    "business_email": {"$first": "$business_email"},
                    "_id1": {"$first": "$_id"},
                    # "allIds": {"$push": "$_id"},
                    # "allIds2": {"$push": "$business_email"},
                    "count": {"$sum": 1}  # Count the number of documents for each 'leadUrl'
                }
            },
            # Match groups where 'count' is greater than 1
            {
                "$match": {
                    "count": {"$gt": 1}
                }
            },
            # Sort by 'count' in descending order
            # {
            #     "$sort": {
            #         "count": -1
            #     }
            # }
        ]
        result = users_collection.aggregate(pipeline)

        for doc in result:
            data.append(doc)

        return data
    
    except Exception as e:
        print("Error url:", str(e))



def get_app_senders(app_name='eFinderV2'):
    try:
        sender_list = cred_collection.find({'App Name' : app_name})
        return sender_list
    
    except Exception as e:
        print("Error updating user:", str(e))



def get_smtp_server(domain):
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        smtp_servers = [str(record.exchange)[:-1] for record in mx_records]
        return smtp_servers
    except dns.resolver.NoAnswer:
        return None
    except dns.resolver.NXDOMAIN:
        return None
    except dns.exception.Timeout:
        return None

def verify_email_address(smtp_server, email_address):
    try:
        server = smtplib.SMTP(smtp_server)
        code, message = server.verify(email_address)
        server.quit()
        return code, message
    except smtplib.SMTPException as e:
        return None, str(e)

def check_email_status(email_address):
    domain = email_address.split('@')[-1]
    smtp_servers = get_smtp_server(domain)
    if not smtp_servers:
        return 'not_valid'
    for smtp_server in smtp_servers:
        code, _ = verify_email_address(smtp_server, email_address)
        if code in [250, 251]:
            return 'may_be_valid'
        elif code in [252]:
            return 'may_be_valid'  # Considered as 'may_be_valid' as per request
        elif code in [450, 550, 551, 553]:
            return 'not_valid'
    return 'not_valid'


def identify_email_pattern(email):
    try:
        print(email)
        for pattern in Config.SENDING_PATTERNS:
            formatted_pattern = pattern.format(
                first='',
                last='',
                first_initial='',
                last_initial='',
                domain=''
            )
            if formatted_pattern in email:
                return pattern
        return None
    except Exception as e:
        log.error(str(e))
        return None
    

def create_email_patterns(first_name, last_name, domain):
    try:
        email_list = []
        for pattern in Config.SENDING_PATTERNS:
            email = pattern.format(
                first=first_name.lower(),
                last=last_name.lower(),
                first_initial=first_name.lower()[0],
                last_initial=last_name.lower()[0],
                domain=domain
            )
            email_list.append(email)
        return email_list

    except Exception as e:
        log.error(str(e))
        return []
    

def cooldown_sleep(duration, interval=60):
    remaining_duration = duration
    while remaining_duration > 0:
        print(f"Cooldown sleep remaining: {remaining_duration} seconds...")
        time.sleep(min(remaining_duration, interval))
        remaining_duration -= interval