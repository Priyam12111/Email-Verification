from datetime import datetime
import traceback
from bson import ObjectId
from deep import main, log
from pymongo import MongoClient, UpdateOne
from configs.db import users, company, catch_all_patterns, db
from configs.logger import log

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
        patterns.append((email, str(user_id)))
    except Exception as e:
        log.info(f"Error generating pattern: {e}")
    return patterns


def is_pattern_blocked(domain, index):
    entry = catch_all_patterns.find_one({"domain": domain, "invalid_patterns": index})
    return entry is not None

def block_pattern_for_domain(domain, index):
    catch_all_patterns.update_one(
        {"domain": domain},
        {"$addToSet": {"invalid_patterns": index}},
        upsert=True
    )

def process_users_dataset(dataset, index):
    email_user_pairs = []
    already_verified_updates = []

    for data in dataset:
        fullName = data.get("fullName", "").split(" ")
        firstName = fullName[0] if len(fullName) > 0 else ""
        lastName = fullName[-1] if len(fullName) > 1 else ""
        refCompanyId = data.get("refCompanyId")

        comp = company.find_one({"_id": refCompanyId}) if refCompanyId else None
        if not comp:
            continue

        companyDomain = comp.get("email_domain")
        company_pattern_index = comp.get("verified_pattern_index", index)

        if not companyDomain:
            continue

        # Case 1: Company already has verified pattern → skip validation
        if "verified_pattern_index" in comp:
            try:
                email = PATTERNS[company_pattern_index].format(
                    first=firstName,
                    last=lastName,
                    domain=companyDomain,
                    first_initial=firstName[0] if firstName else '',
                    last_initial=lastName[0] if lastName else ''
                ).lower().replace('"', '').replace("(", "").replace(")", "")

                already_verified_updates.append(
                    UpdateOne(
                        {"_id": data.get("_id")},
                        {"$set": {
                            "business_email": email,
                            "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "email_pattern_source": "company_verified"
                        }}
                    )
                )
            except Exception as e:
                log.info(f"Failed to format email for user {data.get('_id')}: {e}")
            continue

        # Case 2: Pattern not yet verified, proceed only if not blocked
        if not is_pattern_blocked(companyDomain, company_pattern_index):
            pairs = generate_email_patterns(
                firstName, lastName, companyDomain,
                company_pattern_index, data.get("_id")
            )
            email_user_pairs.extend(pairs)

    return email_user_pairs, already_verified_updates

BATCH_SIZE = 100
# MAX_PATTERNS = len(PATTERNS)  # or limit to e.g. 5
MAX_PATTERNS = 17  # or limit to e.g. 5

# query = {
#     "$or": [{"business_email": {"$exists": False}}, {"business_email": {"$in": ["", None]}}],
#     "createdAt": {"$lt": "2025-06-17", "$gte": "2025-06-10"}, "fullName": "Aisling McGowan"
# }

query = {
  "url_id": ObjectId("685a6b65e97944fca387317c"),
  "business_email": {
    "$in": ["", None, False]
  },
  "allChecked": { "$exists": False }
}

cursor = users.find(query).limit(BATCH_SIZE)

bulk_updates = []
company_pattern_updates = []

for user in cursor:
    fullName = user.get("fullName", "").split()
    firstName = fullName[0] if len(fullName) > 0 else ""
    lastName = fullName[-1] if len(fullName) > 1 else ""
    user_id = str(user["_id"])
    company_id = user.get("refCompanyId")

    company_doc = company.find_one({"_id": company_id}) if company_id else None
    domain = company_doc.get("email_domain") if company_doc else None
    if not domain:
        continue

    email_variants = []
    index_map = {}
    for idx in range(MAX_PATTERNS):
        if is_pattern_blocked(domain, idx):
            continue
        try:
            email = PATTERNS[idx].format(
                first=firstName,
                last=lastName,
                domain=domain,
                first_initial=firstName[0] if firstName else '',
                last_initial=lastName[0] if lastName else ''
            ).lower().replace('"', '').replace("(", "").replace(")", "")
            email_variants.append(email)
            index_map[email] = idx
        except Exception:
            continue

    # verify all at once
    results = main(email_variants, [user_id]*len(email_variants))
    # for idx, email in enumerate(email_variants):
    #     result = await verifier.verify_email(email, user_id)
    #     if result.get("valid"):
    #         # save + break
    #         break
    # print(f"resulst {results}")

    for result in results:
        if result.get("valid") and result.get("id") == user_id:
            valid_email = result.get("email")
            pattern_index = index_map.get(valid_email)

            # bulk_updates.append(UpdateOne(
            #     {"_id": ObjectId(user_id)},
            #     {"$set": {
            #         "business_email": valid_email,
            #         "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #     }}
            # ))
            
            # store in db
            users.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {
                    "business_email": valid_email,
                    "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "email_verified": True,                            # ✅ example field
                    "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # ✅ consistency
                    "v6": pattern_index        
                }}
            )
            log.info(f"[User Updated] {user_id} - {valid_email} using pattern index {pattern_index}")

            if company_id is not None:
                # store in company
                company.update_one(
                    {"_id": company_id},
                    {"$set": {"verified_pattern_index": pattern_index,
                              "verified_patterns": [PATTERNS[pattern_index]],
                              "verifiedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }}
                )
                log.info(f"[Pattern Verified] Domain: {domain} - Pattern index {pattern_index}")
                # company_pattern_updates.append(UpdateOne(
                #     {"_id": company_id},
                #     {"$set": {"verified_pattern_index": pattern_index}}
                # ))
            break  # found one valid → stop checking other patterns
    else:
        # pass
        # optional: mark as tried all and failed
        # update user
        update_data = {
            "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "v6": MAX_PATTERNS
        }

        if MAX_PATTERNS == len(MAX_PATTERNS):
            update_data["allChecked"] = True

        users.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": update_data}
        )