from __future__ import annotations

import os
import re
import time
import random
import socket
import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from pymongo import UpdateOne, ReturnDocument
from configs.db import users, company, catch_all_patterns, db
from configs.logger import log
from deep import EmailVerifier
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

BATCH_IDLE_SLEEP_S = float(os.getenv("IDLE_SLEEP_S", "1.0"))
LEASE_SECS = int(os.getenv("LEASE_SECS", "600"))
RENEW_EVERY_SECS = int(os.getenv("RENEW_EVERY_SECS", "180"))     
MAX_ATTEMPTS_PER_USER = int(os.getenv("MAX_ATTEMPTS", "5"))

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"

EMAIL_SYNTAX_RE = re.compile(
    r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$",
    re.IGNORECASE
)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso_now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ensure_indexes():
    """
    Run once (or harmlessly on startup). Keeps the claim query fast.
    """
    try:
        users.create_index([("business_email", 1), ("allChecked", 1), ("lock.lease_until", 1)])
        users.create_index([("createdAt", 1)])
        users.create_index([("lock.owner", 1)])
        company.create_index([("email_domain", 1)])
        catch_all_patterns.create_index([("domain", 1)], unique=False)
    except Exception as e:
        log.warning(f"ensure_indexes() warning: {e}")

def is_pattern_blocked(domain: str, index: int) -> bool:
    entry = catch_all_patterns.find_one({"domain": domain, "invalid_patterns": index})
    return entry is not None

def block_pattern_for_domain(domain: str, index: int) -> None:
    catch_all_patterns.update_one(
        {"domain": domain},
        {"$addToSet": {"invalid_patterns": index}},
        upsert=True
    )


# def process_users_dataset(dataset, index):
#     email_user_pairs = []
#     already_verified_updates = []

#     for data in dataset:
#         fullName = data.get("fullName", "").split(" ")
#         firstName = fullName[0] if len(fullName) > 0 else ""
#         lastName = fullName[-1] if len(fullName) > 1 else ""
#         refCompanyId = data.get("refCompanyId")

#         comp = company.find_one({"_id": refCompanyId}) if refCompanyId else None
#         if not comp:
#             continue

#         companyDomain = comp.get("email_domain")
#         company_pattern_index = comp.get("verified_pattern_index", index)

#         if not companyDomain:
#             continue

#         # Case 1: Company already has verified pattern → skip validation
#         if "verified_pattern_index" in comp:
#             try:
#                 email = PATTERNS[company_pattern_index].format(
#                     first=firstName,
#                     last=lastName,
#                     domain=companyDomain,
#                     first_initial=firstName[0] if firstName else '',
#                     last_initial=lastName[0] if lastName else ''
#                 ).lower().replace('"', '').replace("(", "").replace(")", "")

#                 already_verified_updates.append(
#                     UpdateOne(
#                         {"_id": data.get("_id")},
#                         {"$set": {
#                             "business_email": email,
#                             "modifiedAt_pattern": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                             "email_pattern_source": "company_verified"
#                         }}
#                     )
#                 )
#             except Exception as e:
#                 log.info(f"Failed to format email for user {data.get('_id')}: {e}")
#             continue

#         # Case 2: Pattern not yet verified, proceed only if not blocked
#         if not is_pattern_blocked(companyDomain, company_pattern_index):
#             pairs = generate_email_patterns(
#                 firstName, lastName, companyDomain,
#                 company_pattern_index, data.get("_id")
#             )
#             email_user_pairs.extend(pairs)

#     return email_user_pairs, already_verified_updates

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


def claim_one_user():
    """
    Atomically claim ONE user for this worker. No $lookup here — keep it light.
    Company/domain checks are done AFTER claiming.
    """
    lease_until = now_utc() + timedelta(seconds=LEASE_SECS)

    doc = users.find_one_and_update(
        {
            # needs work
            "business_email": {"$in": ["", None, False]},
            "allChecked": {"$exists": False},

            # not currently leased or lease expired
            "$or": [
                {"lock": {"$exists": False}},
                {"lock.lease_until": {"$lt": now_utc()}}
            ],

            # cap attempts
            "$expr": {
                "$lt": [
                    {"$ifNull": ["$lock.attempts", 0]},
                    MAX_ATTEMPTS_PER_USER
                ]
            },
        },
        {
            "$set": {
                "lock.owner": WORKER_ID,
                "lock.lease_until": lease_until
            },
            "$inc": {"lock.attempts": 1}
        },
        sort=[("createdAt", 1)],  # deterministic order
        return_document=ReturnDocument.AFTER
    )
    return doc

def renew_lease(user_id: ObjectId):
    users.update_one(
        {"_id": user_id, "lock.owner": WORKER_ID},
        {"$set": {"lock.lease_until": now_utc() + timedelta(seconds=LEASE_SECS)}}
    )

def release_lock(user_id: ObjectId):
    users.update_one(
        {"_id": user_id, "lock.owner": WORKER_ID},
        {"$unset": {"lock": ""}}
    )

async def process_user_patterns(driver, user, PATTERNS, verifier, catch_all_domains: set[str]):
    """
    Uses your existing provider-based browser validation.
    Renews lease periodically so long-running checks don't lose their claim.
    """
    fullName = user.get("fullName", "").split()
    firstName = fullName[0] if len(fullName) > 0 else ""
    lastName  = fullName[-1] if len(fullName) > 1 else ""
    user_id   = ObjectId(user["_id"])
    company_id = user.get("refCompanyId")

    comp = company.find_one({"_id": company_id}) if company_id else None
    domain = comp.get("email_domain") if comp else None
    if not domain:
        # nothing sensible to do; mark checked and exit
        users.update_one(
            {"_id": user_id},
            {"$set": {"allChecked": True, "v6_checked": iso_now_str()}}
        )
        return

    # If company already has a verified pattern, short-circuit
    if comp and "verified_pattern_index" in comp:
        idx = comp.get("verified_pattern_index", 0)
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
            log.info(f"Failed to format fast-path email for user {user_id}: {e}")
            email = None

        if email:
            users.update_one(
                {"_id": user_id},
                {"$set": {
                    "business_email": email,
                    "modifiedAt_pattern": iso_now_str(),
                    "email_pattern_source": "company_verified",
                    "email_verified": True,  # optional
                    "v6_checked": iso_now_str(),
                    "v6": idx
                }}
            )
        return

    current_index = user.get("v6", 0)
    last_renew = time.monotonic()

    for idx in range(current_index, len(PATTERNS)):
        # renew lease heartbeat inside loop (in case this is slow work)
        if time.monotonic() - last_renew > RENEW_EVERY_SECS:
            renew_lease(user_id)
            last_renew = time.monotonic()

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

        # Provider attempts (your existing browser-based flow)
        valid = False
        used_provider = None

        for provider in ("google", "microsoft"):
            try:
                is_browser_valid = browser_based_valid(driver, email, provider)
                result = {"valid": is_browser_valid}
            except Exception as e:
                log.info(f"browser_based_valid exception for {email} ({provider}): {e}")
                result = {"valid": False}

            inc_stats(queries=1)

            if result.get("valid"):
                valid = True
                used_provider = provider
                inc_stats(valids=1)
                break

        if valid:
            users.update_one(
                {"_id": user_id},
                {"$set": {
                    "business_email": email,
                    "modifiedAt_pattern": iso_now_str(),
                    "email_verified": True,
                    "email_verified_mode": "provider_assumed",
                    "email_verified_provider_assumed": used_provider,
                    "v6_checked": iso_now_str(),
                    "v6": idx
                }}
            )
            log.info(f"[User Updated] {user_id} - {email} using pattern index {idx} (assumed {used_provider})")

            if company_id:
                company.update_one(
                    {"_id": company_id},
                    {"$set": {
                        "verified_pattern_index": idx,
                        "verified_patterns": [PATTERNS[idx]],
                        "verifiedAt": iso_now_str(),
                        "provider_assumed": used_provider
                    }}
                )
            break
        else:
            users.update_one(
                {"_id": user_id},
                {"$set": {
                    "v6": idx + 1,
                    "v6_checked": iso_now_str()
                }}
            )
            log.info(f"[Pattern Invalid] {email} - next index: {idx + 1}")
    else:
        users.update_one(
            {"_id": user_id},
            {"$set": {
                "v6": len(PATTERNS),
                "allChecked": True,
                "v6_checked": iso_now_str()
            }}
        )
        log.info(f"[All Patterns Tried] {user_id} - domain: {domain}")

async def main_loop():
    ensure_indexes()

    verifier = EmailVerifier(concurrency=1)
    catch_all_domains: set[str] = set()
    driver = browser_manager.open_browser()

    try:
        log.info(f"Worker {WORKER_ID} started with lease={LEASE_SECS}s renew={RENEW_EVERY_SECS}s")
        while True:
            # Atomically claim one user
            user = claim_one_user()
            if not user:
                time.sleep(BATCH_IDLE_SLEEP_S)
                continue

            uid = user["_id"]

            try:
                await process_user_patterns(driver, user, PATTERNS, verifier, catch_all_domains)
            except Exception as e:
                log.error(f"Worker {WORKER_ID} failed for user {uid}: {e}\n{traceback.format_exc()}")
            finally:
                release_lock(uid)

    finally:
        if driver:
            browser_manager.close_browser(driver)
        log.info(f"Worker {WORKER_ID} stopped.")

if __name__ == "__main__":
    asyncio.run(main_loop())
