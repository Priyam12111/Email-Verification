from datetime import datetime
import traceback

from bson import ObjectId
from deep import main,logging
from pymongo import MongoClient, UpdateOne
from sanatize import clean_and_extract
from company import get_email_domain
import re

PATTERNS = [
    "{first}.{last}@{domain}",
    "{first_initial}{last}@{domain}",
    "{first}@{domain}",
    "{first}.{last_initial}@{domain}",
    "{last}.{first}@{domain}",
    "{last_initial}.{first}@{domain}",
    "{first_initial}.{last}@{domain}",
    "{last_initial}{first_initial}@{domain}",
    "{last}@{domain}",
    "{first}{last_initial}@{domain}",
    "{first}{last}@{domain}",
    "{first}_{last}@{domain}",
    "{first}-{last}@{domain}",
    "{last}{first_initial}@{domain}",
    "{first_initial}{last_initial}@{domain}",
    "{last_initial}{first}@{domain}",
    "{last}{first}@{domain}",
]

client = MongoClient('mongodb://developer:ah6M6vIz52YYJzy1@3.109.96.163:27017/e-finder?authSource=e-finder&readPreference=primary&serverSelectionTimeoutMS=20000&appname=mongosh%201.6.1&directConnection=true&ssl=false')
logging.info("Connected to MongoDB")

db = client["e-finder"]
users = db["users"]
company = db["company"]

def generate_email_patterns(firstName, lastName, domain, index, user_id):
    patterns = []
    try:
        pattern = PATTERNS[index]
        email = pattern.format(
            first=firstName,
            last=lastName,
            domain=domain,
            first_initial=firstName[0] if firstName else '',
            last_initial=lastName[0] if lastName else ''
        ).lower().replace('"', '').replace("(", "").replace(")", "")
        # Clean up invalid dot placements
        # email = re.sub(r'\.+', '.', email)           # Replace multiple dots with one
        # email = re.sub(r'\.@', '@', email)           # Remove trailing dot before @
        # email = email.strip('.')                     # Remove leading/trailing dots
        # Normalize multiple dots, dashes, or underscores
        email = re.sub(r'[.\-_]+', lambda m: m.group(0)[0], email)  # Collapse repeats, keep only one

        # Remove any of . - _ before @
        email = re.sub(r'[.\-_]+@', '@', email)

        # Strip special characters (., -, _) from beginning and end
        email = email.strip('.-_')
        patterns.append((email, str(user_id)))
    except Exception as e:
        logging.info(f"Error generating pattern: {e}")
    return patterns

def process_users_dataset(dataset, index):
    email_user_pairs = []
    for data in dataset:
        # fullName = data.get("fullName", "").split(" ")
        fullName = clean_and_extract(data.get("fullName"))
        fullName = fullName.replace('.', ' ').replace('_', ' ').split()
        # print("fullName", fullName)
        firstName = fullName[0] if len(fullName) > 0 else ""
        if firstName:
            lastName = fullName[-1].rsplit('-', 1)[-1] if len(fullName) > 1 else ""
            # refCompanyId = data.get("refCompanyId")
            
            # comp = company.find_one({"_id": refCompanyId}) if refCompanyId else None
            # companyDomain = comp.get("email_domain") if comp else None
            companyDomain = data.get("email_domain") if data else None
            companyDomain = get_email_domain(companyDomain)
            
            if companyDomain:
                pairs = generate_email_patterns(
                    firstName, lastName, companyDomain, 
                    index, data.get("_id")
                )
                
    
                # print("dataset=pairs=", pairs)

                email_user_pairs.extend(pairs)
    return email_user_pairs

for index in range(len(PATTERNS)):
    # Decide the 'users.v6' condition dynamically
    # index = 6
    v6_condition = { '$exists': False } if index == 0 else index
    company_data = company.aggregate([
                                        {
                                            '$match': {
                                                'email_domain': {
                                                    '$nin': [
                                                        '', False
                                                    ], 
                                                    '$exists': True
                                                }
                                            }
                                        }, {
                                            '$lookup': {
                                                'from': 'users', 
                                                'localField': '_id', 
                                                'foreignField': 'refCompanyId', 
                                                'as': 'users'
                                            }
                                        }, {
                                            '$unwind': {
                                                'path': '$users', 
                                                'preserveNullAndEmptyArrays': True
                                            }
                                        }, {
                                            '$match': {
                                                '$or': [
                                                    {
                                                        'users.business_email': {
                                                            '$exists': False
                                                        }
                                                    }, {
                                                        'users.business_email': {
                                                            '$in': [
                                                                '', None
                                                            ]
                                                        }
                                                    }
                                                ], 
                                                'users.fullName': {
                                                    '$exists': True, 
                                                    '$ne': ''
                                                }, 
                                                'users.v6': v6_condition
                                            }
                                        }, {
                                            '$project': {
                                                '_id': 0, 
                                                '_id': '$users._id', 
                                                'email_domain': 1, 
                                                'users': 1, 
                                                'fullName': '$users.fullName', 
                                                'mail': '$users.business_email', 
                                                'v6': '$users.v6', 
                                                'refCompanyId': '$users.refCompanyId'
                                            }
                                        }
                                    ])

    # print("length=====", len(list(company_data)))
    company_data = list(company_data)  # Convert cursor to list
    chunk_size = 500
    
    if (index == (len(PATTERNS) - 1)) and (len(company_data) == 0):
        logging.info("Congratulations, verification data finished.")
        exit()
        
    print(f"Processing pattern {index} ({PATTERNS[index]}) {len(company_data)}")
    
    for i in range(0, len(company_data), chunk_size):
        data = company_data[i:i + chunk_size]
        print(f"Processing chunk {i // chunk_size + 1} with {len(data)} items")
    
    # total_docs = users.count_documents({"$and": [
    #     {"$or": [{"business_email": {"$exists": False}}, {"business_email": {"$in": ["", None]}}]},
    #     {"$or": [{"v6": {"$exists": False}}, {"v6": index}]}
    # ]})
    
    # logging.info(f"Processing pattern {index} ({PATTERNS[index]}) {total_docs}")
    # for skip in range(0, total_docs, 500):
        try:
            # data = list(users.aggregate([
            #     {"$match": {
            #         "$and": [
            #             {"$or": [{"business_email": {"$exists": False}}, {"business_email": {"$in": ["", None]}}]},
            #             {"$or": [{"v6": {"$exists": False}}, {"v6": index}]}
            #         ]
            #     }},
            #     {"$sample": {"size": 500}}
            # ]))

            if not data:
                logging.info("No more users to process.")
                break

            # Generate email patterns in memory
            email_user_pairs = process_users_dataset(data, index)
            
            if not email_user_pairs:
                continue

            # Split into separate lists for processing
            emails_list = [pair[0] for pair in email_user_pairs]
            user_ids_list = [pair[1] for pair in email_user_pairs]
            
            # Process emails through verification system
            verification_results = main(emails_list, user_ids_list)
            # Prepare bulk operations
            updates = []
            valid_user_ids = []
            for result in verification_results:
                user_id = result['id']
                valid_email = result["email"]
                if user_id and valid_email and result["valid"]:
                    updates.append(
                        UpdateOne(
                            {"_id": ObjectId(user_id)},
                            {"$set": {
                                "business_email": valid_email,
                                "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }}
                        )
                    )
                    if user_id in user_ids_list:
                        user_ids_list.remove(user_id)
                    valid_user_ids.append(user_id)

            # Execute bulk updates
            if updates:
                db["users"].bulk_write(updates)
                logging.info(f"Updated {len(updates)} users with valid emails")

            # Update v6 field for processed users
            update_v6_result = users.update_many(
                {"_id": {"$in": [ObjectId(uid) for uid in user_ids_list]}},
                {"$inc": {"v6": 1},"$set": {"v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "modifiedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
            )
            logging.info(f"Updated v6 field for {update_v6_result.modified_count} users")
            # exit()

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            traceback.print_exc()
            logging.error(traceback.format_exc())  # shows line number and stack trace