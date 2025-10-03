from datetime import datetime
import traceback
from bson import ObjectId
from deep import main, log
from pymongo import UpdateOne
from configs.db import users, company, catch_all_patterns, db
from configs.logger import log
import asyncio
from deep import EmailVerifier
import random
import time
from lib.browser import BrowserManager
from browserValidation import browser_based_valid
from utils.helpers import now_ist, inc_stats

browser_manager = BrowserManager() 

PATTERNS = [
    "{first}.{last}@{domain}",
    "{first_initial}{last}@{domain}",
    "{first}@{domain}",
    "{first}.{last_initial}@{domain}",
    "{last}.{first}@{domain}",
    "{last_initial}.{first}@{domain}",
    "{first_initial}.{last}@{domain}",
    # "{last_initial}{first_initial}@{domain}",
    # "{last}@{domain}",
    # "{first}{last_initial}@{domain}",
    # "{first}{last}@{domain}",
    # "{first}_{last}@{domain}",
    # "{first}-{last}@{domain}",
    # "{last}{first_initial}@{domain}",
    # "{first_initial}{last_initial}@{domain}",
    # "{last_initial}{first}@{domain}",
    # "{last}{first}@{domain}",
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
MAX_PATTERNS = len(PATTERNS) 

# async def process_user_patterns(driver, user, PATTERNS, verifier, catch_all_domains):
#     fullName = user.get("fullName", "").split()
#     firstName = fullName[0] if len(fullName) > 0 else ""
#     lastName = fullName[-1] if len(fullName) > 1 else ""
#     user_id = str(user["_id"])
#     company_id = user.get("refCompanyId")

#     company_doc = company.find_one({"_id": company_id}) if company_id else None
#     domain = company_doc.get("email_domain") if company_doc else None
#     if not domain:
#         return

#     current_index = user.get("v6", 0)

#     # DOMAIN CHECKS ONCE
#     ok, mx_or_reason = await verifier.domain_precheck(firstName, lastName, domain)
#     if not ok:
#         users.update_one(
#             {"_id": ObjectId(user_id)},
#             {"$set": {
#                 "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                 "pattern_invalid_reason": mx_or_reason,
#                 "allChecked": True,
#                 "v6": len(PATTERNS)
#             }}
#         )
#         log.info(f"[Domain Invalid] {user_id} - {domain} - {mx_or_reason}")
#         return

#     mx_servers = mx_or_reason
#     mx_provider = verifier.get_mx_provider(mx_servers)

#     # ---- Do catch-all test ONCE per domain ----
#     if domain in catch_all_domains:
#         is_catch_all = True
#     else:
#         test_email = f"random_{random.randint(1000,9999)}_{int(time.time())}@{domain}"
#         catch_all_result, _ = await verifier.smtp_check(test_email, mx_servers)
#         is_catch_all = bool(catch_all_result)
#         if is_catch_all:
#             catch_all_domains.add(domain)
#     for idx in range(current_index, len(PATTERNS)):
#         if is_pattern_blocked(domain, idx):
#             continue
#         try:
#             email = PATTERNS[idx].format(
#                 first=firstName,
#                 last=lastName,
#                 domain=domain,
#                 first_initial=firstName[0] if firstName else '',
#                 last_initial=lastName[0] if lastName else ''
#             ).lower().replace('"', '').replace("(", "").replace(")", "")
#         except Exception as e:
#             log.info(f"Pattern formatting failed for {user_id} at index {idx}: {e}")
#             continue

#         # Pass the driver into the validation function!
#         result = await verifier.verify_email_with_mx(
#             email, user_id, mx_servers, mx_provider, is_catch_all, catch_all_domains, driver
#         )

#         if result.get("valid"):
#             users.update_one(
#                 {"_id": ObjectId(user_id)},
#                 {"$set": {
#                     "business_email": email,
#                     "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                     "email_verified": True,
#                     "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                     "v6": idx
#                 }}
#             )
#             log.info(f"[User Updated] {user_id} - {email} using pattern index {idx}")

#             if company_id:
#                 company.update_one(
#                     {"_id": company_id},
#                     {"$set": {
#                         "verified_pattern_index": idx,
#                         "verified_patterns": [PATTERNS[idx]],
#                         "verifiedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                     }}
#                 )
#                 log.info(f"[Pattern Verified] Domain: {domain} - Pattern index {idx}")
#             break
#         else:
#             users.update_one(
#                 {"_id": ObjectId(user_id)},
#                 {"$set": {
#                     "v6": idx + 1,  # try next pattern next time
#                     "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#                 }}
#             )
#             log.info(f"[Pattern Invalid] {email} - next index: {idx + 1}")
#     else:
#         users.update_one(
#             {"_id": ObjectId(user_id)},
#             {"$set": {
#                 "v6": len(PATTERNS),
#                 "allChecked": True,
#                 "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             }}
#         )
#         log.info(f"[All Patterns Tried] {user_id} - domain: {domain}")


import re
from bson import ObjectId
from datetime import datetime

EMAIL_SYNTAX_RE = re.compile(
    r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
    re.IGNORECASE
)

async def process_user_patterns(driver, user, PATTERNS, verifier, catch_all_domains):
    print("here2")
    fullName = user.get("fullName", "").split()
    firstName = fullName[0] if len(fullName) > 0 else ""
    lastName  = fullName[-1] if len(fullName) > 1 else ""
    user_id   = str(user["_id"])
    company_id = user.get("refCompanyId")

    company_doc = company.find_one({"_id": company_id}) if company_id else None
    domain = company_doc.get("email_domain") if company_doc else None
    if not domain:
        return

    current_index = user.get("v6", 0)

    for idx in range(current_index, len(PATTERNS)):
        if is_pattern_blocked(domain, idx):
            continue
        try:
            email = (
                PATTERNS[idx]
                .format(
                    first=firstName,
                    last=lastName,
                    domain=domain,
                    first_initial=firstName[0] if firstName else '',
                    last_initial=lastName[0] if lastName else ''
                )
                .lower()
                .replace('"', '')
                .replace("(", "")
                .replace(")", "")
            )
        except Exception as e:
            log.info(f"Pattern formatting failed for {user_id} at index {idx}: {e}")
            continue

        # ---- Hardcoded provider attempts ----
        valid = False
        used_provider = None
        
        print("here1")

        for provider in ("google", "microsoft"):
            try:
                print("before browser")
                is_browser_valid = browser_based_valid(driver, email, provider)
                print('browser_based_valid', is_browser_valid)
                result = {"valid": is_browser_valid}
            except Exception as e:
                print("exception", str(e))
                result = {"valid": False}
            
            inc_stats(queries=1)

            if result.get("valid"):
                valid = True
                used_provider = provider
                inc_stats(valids=1)
                break

        if valid:
            users.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {
                    "business_email": email,
                    "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "email_verified": True,
                    "email_verified_mode": "provider_assumed",
                    "email_verified_provider_assumed": used_provider,
                    "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "v6": idx
                }}
            )
            log.info(f"[User Updated] {user_id} - {email} using pattern index {idx} (assumed {used_provider})")

            if company_id:
                pass
                company.update_one(
                    {"_id": company_id},
                    {"$set": {
                        "verified_pattern_index": idx,
                        "verified_patterns": [PATTERNS[idx]],
                        "verifiedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "provider_assumed": used_provider
                    }}
                )
            break
        else:
            users.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {
                    "v6": idx + 1,
                    "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }}
            )
            log.info(f"[Pattern Invalid] {email} - next index: {idx + 1}")
    else:
        users.update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {
                "v6": len(PATTERNS),
                "allChecked": True,
                "v6_checked": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }}
        )
        log.info(f"[All Patterns Tried] {user_id} - domain: {domain}")


async def main_loop():
    verifier = EmailVerifier(concurrency=1)
    catch_all_domains = set()
    driver = browser_manager.open_browser()

    try:
        print("here3")
        while True:
            pipeline = [
                {
                    "$match": {
                        "business_email": {"$in": ["", None, False]},
                        "allChecked": {"$exists": False}
                    }
                },
                {
                    "$lookup": {
                        "from": "company-1",
                        "localField": "refCompanyId",
                        "foreignField": "_id",
                        "as": "company"
                    }
                },
                {
                    "$unwind": "$company"
                },
                {
                    "$match": {
                        "company.email_domain": {"$exists": True, "$ne": ""}
                    }
                },
                { "$sort": { "createdAt": 1 }},
                { "$limit": BATCH_SIZE }
            ]

            cursor = users.aggregate(pipeline)
            print('after aggregate pipeline')

            for user in cursor:
                await process_user_patterns(driver, user, PATTERNS, verifier, catch_all_domains)
    except Exception as e:
        log.error(f"Main loop error: {e}")
    finally:
        if driver:
            browser_manager.close_browser(driver)

if __name__ == "__main__":
    asyncio.run(main_loop())