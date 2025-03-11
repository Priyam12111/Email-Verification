import csv
from datetime import datetime

from bson import ObjectId
from ve6 import main
from pymongo import MongoClient, UpdateOne

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
print("Connected to MongoDB")
db = client["e-finder"]
users = db["users"]
company = db["company"]


def create_patterns(firstName,lastName,domain,index,user_id):
    patterns = []
    f = open('emails.csv', 'a', newline='',encoding='utf-8')
    pattern =  PATTERNS[index]
    try:
        implied = pattern.format(first=firstName,last=lastName,domain=domain,first_initial=firstName[0],last_initial=lastName[0]).lower().replace('"','').replace("(","").replace(")","")
    except Exception:
        implied = ["None"]
    patterns.append(implied)
    f.write(f'{implied},{user_id}\n')

    return patterns

def get_pattern_email(dataset,index=0):
    for data in dataset:
        fullName = data.get("fullName").split(" ")
        if len(fullName) < 1:
            continue  # Skip invalid names
        firstName = fullName[0]
        LastName = fullName[-1]  if len(fullName) > 1 else ""
        refCompanyId = data.get("refCompanyId")
        comp = company.find_one({"_id": refCompanyId}) if refCompanyId else None
        companyDomain = comp.get("email_domain") if comp else None
        if companyDomain:
            create_patterns(firstName, LastName, companyDomain, index,data.get("_id"))


for index in range(2, len(PATTERNS)):
    total_docs = 400000
    for skip in range(0, total_docs, 100):
        try:
            open('emails.csv', 'w').close() #clear the file
            data = list(users.aggregate([
                {"$match": {
                    "$or": [
                        { "business_email": { "$exists": False } },
                        { "business_email": { "$exists": True, "$in": ["", None] } }
                    ],
                    "$or": [
                        {"v6": {"$exists": False}},
                        {"v6": index}
                    ]
                }},
                {"$sample": {"size": 100}}
            ]))
            if data:
                get_pattern_email(data,index)
                emails = main()
                user_ids = [user["_id"] for user in data]
                status = [False] * len(PATTERNS)
                status[index] = True
                updates = []
                for i, user in enumerate(data):
                    ids = list(emails.keys())
                    if i < len(ids) and emails[ids[i]][0]:  # Check if email is valid and exists
                        print(f"Updating user {ObjectId(ids[i])} with email {emails[ids[i]][0]}")
                        updates.append(
                            UpdateOne(
                                {"_id": ObjectId(ids[i])},
                                {"$set": {"business_email": emails[ids[i]][0], "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
                            )
                        )
                    elif i < len(ids) and not emails[ids[i]][0]:
                        updates.append(
                            UpdateOne(
                                {"_id": ObjectId(ids[i])},
                                {"$set": {"modifiedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
                            )
                        )
                # Execute bulk updates
                if updates:
                    db["users"].bulk_write(updates)

                # Update v6 field correctly
                try:
                    users.update_many(
                        {
                            "$or": [
                                {"v6": {"$exists": False}},
                                {"v6": index}
                            ],
                            "_id": {"$in": [ObjectId(user_id) for user_id in user_ids]}
                        },
                        {
                            "$inc": {"v6": 1},  # Increments v6
                            "$set": {"v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                        }
                )
                except Exception as e:
                    print(f"An error occurred: {e}")
            else:
                print("No more users to process.")
                break
        except Exception as e:
            print(f"An error occurred: {e}")