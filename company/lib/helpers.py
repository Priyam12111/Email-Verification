from datetime import datetime
from pymongo import MongoClient
from lib.constants import COLLECTION_COMPANY
from lib.mongo_connection import mg_aggregate
from lib.configs import envs
from bson import ObjectId
import pytz

client = MongoClient(envs['MONGO_STRING'])
db_name = envs['DATABASE_NAME']
db = client[db_name]
company_collection = db.company
users_collection = db.users
credentials_collection = db.credentials

def current_datetime_to_string():
    current_datetime_utc = datetime.utcnow()
    ist_timezone = pytz.timezone('Asia/Kolkata')
    current_datetime_ist = current_datetime_utc.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
    formatted_datetime = current_datetime_ist.strftime("%Y-%m-%dT%H:%M:%S")
    return formatted_datetime


def add_user_data(user_obj):
    try:
        if 'user_public_url' in user_obj:
            existing_user = users_collection.find_one({"user_public_url": user_obj["user_public_url"]})
            if existing_user is None:
                resp = users_collection.insert_one(user_obj)
                return resp.inserted_id
            else:
                print(f"User with URL '{user_obj['user_public_url']}' already exists.")
                return None
        else:
            print("Skipping insertion: 'user_public_url' field is not present in the document.")
            return None
    except Exception as e:
        print(f"Error adding user data: {e}")
        return None


def update_user_data(user_public_url, update_data):
    try:
        query = {"user_public_url": user_public_url}
        update = {"$set": update_data}
        result = users_collection.update_one(query, update)

        if result.matched_count == 1:
            print("User data updated successfully.")
        else:
            print("No matching document found for update.")
    except Exception as e:
        print(f"Error updating user data: {e}")


def add_company_data(company_obj):
    try:
        if 'publicUrl' in company_obj:
            existing_company = company_collection.find_one({"publicUrl": company_obj["publicUrl"]})
            if existing_company is None:
                result = company_collection.insert_one(company_obj)
                return result.inserted_id
            else:
                print(f"Company with publicUrl '{company_obj['publicUrl']}' already exists.")
                return None
        else:
            print("Skipping insertion: 'publicUrl' field is not present in the document.")
            return None
    except Exception as e:
        print(f"Error adding company data: {e}")
        return None
    

def update_company_data(company_linkedin_url, update_data):
    try:
        query = {"publicUrl": company_linkedin_url}
        update = {"$set": update_data}
        result = company_collection.update_one(query, update)

        if result.matched_count == 1:
            print("Company data updated successfully.")
        else:
            print("No matching document found for update.")
    except Exception as e:
        print(f"Error updating company data: {e}")


def get_user_by_url(user_public_url):
    try:
        query = {"user_public_url": user_public_url}
        user_data = users_collection.find_one(query)
        return user_data
    except Exception as e:
        print(f"Error retrieving user data: {e}")
        return None


def get_company_by_url(company_linkedin_url):
    try:
        query = {"publicUrl": company_linkedin_url}
        company_data = company_collection.find_one(query)
        return company_data
    except Exception as e:
        print(f"Error retrieving company data: {e}")
        return None


def get_company_by_id(id):
    try:
        query = {"_id": ObjectId(id)}
        company_data = company_collection.find_one(query)
        return company_data
    except Exception as e:
        print(f"Error retrieving company data: {e}")
        return None
    

def get_credentials_list():
    try:
        credentials_list = list(credentials_collection.find())
        return credentials_list
    except Exception as e:
        print(f"Error retrieving credentials list: {e}")
        return None
    

def get_company_to_verify(offset, limit=10):
    try:
        cond = [
            {
                '$match': {
                    'status': False,
                    # 'company_system': system_no
                }
            },
            {
                '$skip': offset
            },
            {
                '$limit': limit
            }
        ]

        data = mg_aggregate(COLLECTION_COMPANY, cond)

        return data

    except Exception as e:
        print(f"Error retrieving company")
        return []