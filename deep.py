import asyncio
from datetime import datetime
import aiodns
import re
import time
import sys
from tqdm import tqdm
from email.utils import parseaddr
from collections import defaultdict
import requests
import aiosmtplib
import random
import csv
from browserValidation import browser_based_valid
from configs.db import company
from configs.logger import log
import contextlib

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$')

class EmailVerifier:
    def __init__(self, concurrency=2, timeout=120, smtp_port=25):
        self.resolver = aiodns.DNSResolver()
        self.timeout = timeout
        self.smtp_port = smtp_port
        self.disposable_domains = self.load_disposable_domains()
        self.catch_all_domains = self.load_blocked_domains("catch_all_domains.csv")
        self.blocked_domains = self.load_blocked_domains("blocked_domains.csv")
        self.mx_cache = {}
        self.smtp_semaphore = asyncio.Semaphore(concurrency)
        self.domain_rates = defaultdict(int)
        self.error_desc = ""
        self.validate_emails = defaultdict(list)

    @staticmethod
    def load_disposable_domains():
        try:
            response = requests.get(
                'https://raw.githubusercontent.com/disposable-email-domains/disposable-email-domains/master/disposable_email_blocklist.conf',
                timeout=5
            )
            return set(response.text.splitlines())
        except Exception:
            return set(['tempmail.com', 'mailinator.com'])

    def is_google_mx(self, mx_records):
        google_hosts = {
            'aspmx.l.google.com',
            'alt1.aspmx.l.google.com',
            'alt2.aspmx.l.google.com',
            'alt3.aspmx.l.google.com',
            'alt4.aspmx.l.google.com',
        }
        return any(host.lower().endswith('.google.com') or host.lower() in google_hosts for _, host in mx_records)

    def get_mx_provider(self, mx_records):
        google_hosts = ['google.com', 'googlemail.com']
        microsoft_hosts = [
            'outlook.com', 'hotmail.com', 'office365.com', 'microsoft.com',
            'outlook.office365.com', 'mail.protection.outlook.com'
        ]

        for _, host in mx_records:
            host = host.lower()
            if any(gh in host for gh in google_hosts):
                return "google"
            if any(mh in host for mh in microsoft_hosts):
                return "microsoft"
        return "other"

    async def check_syntax(self, email):
        if not EMAIL_REGEX.match(email):
            return False
        _, address = parseaddr(email)
        if not address:
            return False
        parts = address.split('@')
        if len(parts) != 2:
            return False
        local, domain = parts
        if not local or not domain:
            return False
        if domain.startswith('.') or domain.endswith('.') or '.' not in domain:
            return False
        return True

    async def check_disposable(self, domain):
        return domain.lower() in self.disposable_domains

    async def get_mx_servers(self, domain):
        if domain in self.mx_cache:
            return self.mx_cache[domain]
        try:
            records = await self.resolver.query(domain, 'MX')
            mx_records = sorted([(record.priority, record.host) for record in records])
            self.mx_cache[domain] = mx_records
            return mx_records
        except aiodns.error.DNSError:
            try:
                a_records = await self.resolver.query(domain, 'A')
            except aiodns.error.DNSError:
                a_records = []
            try:
                aaaa_records = await self.resolver.query(domain, 'AAAA')
            except aiodns.error.DNSError:
                aaaa_records = []
            if a_records or aaaa_records:
                self.mx_cache[domain] = [(0, domain)]
                return [(0, domain)]
            else:
                self.mx_cache[domain] = []
                return []

    async def smtp_check(self, email, mx_servers):
        domain = email.split('@')[1]

        if domain in self.catch_all_domains:
            return True, "Domain already marked as catch-all"

        self.domain_rates[domain] += 1
        if self.domain_rates[domain] > 5:
            self.domain_rates[domain] = 0

        for priority, host in mx_servers:
            try:
                async with self.smtp_semaphore:
                    smtp = aiosmtplib.SMTP(hostname=host, port=25, timeout=self.timeout, start_tls=True)
                    await smtp.connect()

                    code, _ = await smtp.ehlo()
                    if code != 250:
                        await smtp.helo()

                    await smtp.mail("verifier@example.com")
                    await asyncio.sleep(1)

                    code, response = await smtp.rcpt(email)
                    await smtp.quit()

                    if code == 250:
                        if self.is_google_mx(mx_servers):
                            return False, "Unverifiable — hosted on Google (ambiguous 250)"
                        return True, "Verified via RCPT"
                    elif code == 550:
                        return False, "Mailbox not found"
                    elif code == 552:
                        return False, "Mailbox full"
                    elif code == 451:
                        return False, "Temporary local problem"
                    elif code == 421:
                        return False, "Service not available"
                    elif code == 252:
                        self.catch_all_domains.add(domain)
                        return True, "Possible catch-all (252)"
                    else:
                        return False, f"Unhandled SMTP code {code}: {response.decode() if hasattr(response, 'decode') else response}"

            except Exception as e:
                self.error_desc = str(e)
                log.error(f"[SMTP ERROR] {email} @ {host}: {e}")

                if "4.2.1" in self.error_desc:
                    self.blocked_domains.add(domain)
                    self.catch_all_domains.add(domain)
                    log.warning(f"[Blocked] Sleeping due to rate limit on {domain}")
                    await asyncio.sleep(random.uniform(15, 45))

                return False, f"SMTP exception: {str(e)}"

        return False, "All MX servers failed"

    async def verify_email(self, email, id):
        parts = email.split('@')
        domain = parts[1] if len(parts) > 1 else ''
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")

        if not await self.check_syntax(email):
            return {
                'id': id,
                'email': email,
                'valid': False,
                'reason': 'Invalid syntax',
                'catch_all': False,
                'timestamp': current_time,
                'mx_provider': 'unknown'
            }

        if await self.check_disposable(domain):
            return {
                'id': id,
                'email': email,
                'valid': False,
                'reason': 'Disposable domain',
                'catch_all': False,
                'timestamp': current_time,
                'mx_provider': 'unknown'
            }

        mx_servers = await self.get_mx_servers(domain)
        if not mx_servers:
            self.validate_emails[id].append(False)
            return {
                'id': id,
                'email': email,
                'valid': False,
                'reason': 'No MX records/servers',
                'catch_all': False,
                'timestamp': current_time,
                'mx_provider': 'none'
            }

        mx_provider = self.get_mx_provider(mx_servers)
        is_google = mx_provider == "google"
        is_microsoft = mx_provider == "microsoft"
        is_known_hosted = is_google or is_microsoft

        smtp_valid, smtp_reason = await self.smtp_check(email, mx_servers)

        if smtp_valid:
            # SMTP passed — check catch-all
            if domain not in self.catch_all_domains:
                test_email = f"random_{random.randint(1000,9999)}_{int(time.time())}@{domain}"
                catch_all_result, _ = await self.smtp_check(test_email, mx_servers)
                if catch_all_result:
                    self.catch_all_domains.add(domain)

            if domain in self.catch_all_domains:
                if is_known_hosted:
                    is_browser_valid = browser_based_valid(email, mx_provider)
                    if is_browser_valid:
                        self.validate_emails[id].append(email)
                        return {
                            'id': id,
                            'email': email,
                            'valid': True,
                            'reason': 'Browser-based validation (catch-all)',
                            'catch_all': True,
                            'timestamp': current_time,
                            'mx_provider': mx_provider
                        }
                    else:
                        self.validate_emails[id].append(False)
                        return {
                            'id': id,
                            'email': email,
                            'valid': False,
                            'reason': 'Catch-all domain, browser check failed',
                            'catch_all': True,
                            'timestamp': current_time,
                            'mx_provider': mx_provider
                        }
                else:
                    self.validate_emails[id].append(False)
                    return {
                        'id': id,
                        'email': email,
                        'valid': False,
                        'reason': 'Catch-all domain, non-browser-verifiable',
                        'catch_all': True,
                        'timestamp': current_time,
                        'mx_provider': mx_provider
                    }

            # No catch-all → success
            self.validate_emails[id].append(email)
            return {
                'id': id,
                'email': email,
                'valid': True,
                'reason': smtp_reason,
                'catch_all': False,
                'timestamp': current_time,
                'mx_provider': mx_provider
            }

        # SMTP failed — check for "block" in error message
        if "block" in smtp_reason.lower():
            if domain in self.catch_all_domains or is_known_hosted:
                is_browser_valid = browser_based_valid(email, mx_provider)
                if is_browser_valid:
                    self.validate_emails[id].append(email)
                    return {
                        'id': id,
                        'email': email,
                        'valid': True,
                        'reason': 'Validated via browser (SMTP blocked)',
                        'catch_all': domain in self.catch_all_domains,
                        'timestamp': current_time,
                        'mx_provider': mx_provider
                    }

        # Final fallback — Invalid
        self.validate_emails[id].append(False)
        return {
            'id': id,
            'email': email,
            'valid': False,
            'reason': smtp_reason,
            'catch_all': domain in self.catch_all_domains,
            'timestamp': current_time,
            'smtp_error': self.error_desc,
            'mx_provider': mx_provider
        }

    async def should_skip_verification(self, email, id):
        log.info('here')
        domain = email.split('@')[-1]
        comp = company.find_one({"email_domain": domain} , sort=[("createdAt", -1)]) if domain else None
        patterns = comp.get("verified_patterns", [])
        
        log.info(f"patterns: {patterns} {comp} {domain}")

        if not patterns:
            return False

        if patterns:
            # return that pattern that is defined for that particualar user it should not go with email generation even
            log.info(f"Skipping verification for {email} — matched known pattern(s) for company {comp.get('name')}")
            return True
        return False
    
    async def process_batch(self, emails, ids):
        tasks = []

        for i, email in enumerate(emails):
            id = ids[i]

            log.info(f"id: {id}")
            if await self.should_skip_verification(email, id):
                result = {
                    'id': id,
                    'email': email,
                    'valid': True,
                    'reason': 'Skipped - known/verified domain',
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                tasks.append(result)
            else:
                tasks.append(self.verify_email(email, id))

        coroutines = [task for task in tasks if asyncio.iscoroutine(task)]
        static_results = [task for task in tasks if not asyncio.iscoroutine(task)]

        verified = await asyncio.gather(*coroutines) if coroutines else []
        return static_results + verified

    # async def process_batch(self, emails, ids):
    #     tasks = [self.verify_email(email, ids[i]) for i, email in enumerate(emails)]
    #     return await asyncio.gather(*tasks)

    def load_blocked_domains(self, filename):
        blocked = set()
        try:
            with open(filename, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if row:
                        blocked.add(row[0].strip().lower())
        except FileNotFoundError:
            print(f"[Warning] File '{filename}' not found. No blocked domains loaded.")
        return blocked
    
# async def verify_first_valid(email_variants, user_id):
#     verifier = EmailVerifier(concurrency=50)
#     tasks = [verifier.verify_email(email, user_id) for email in email_variants]

#     for coro in asyncio.as_completed(tasks):
#         result = await coro
#         if result.get("valid"):
#             return [result]  # return as list to match original return type

#     # If none valid, collect all results (optional fallback)
#     return [await task for task in tasks]

# async def verify_first_valid(email_variants, user_id):
#     verifier = EmailVerifier(concurrency=50)
#     tasks = [asyncio.create_task(verifier.verify_email(email, user_id)) for email in email_variants]

#     try:
#         for coro in asyncio.as_completed(tasks):
#             result = await coro
#             if result.get("valid"):
#                 for t in tasks:
#                     if not t.done():
#                         t.cancel()
#                         with contextlib.suppress(asyncio.CancelledError):
#                             await t
#                 return [result]

#         return [await t for t in tasks if not t.cancelled()]
#     except Exception as e:
#         for t in tasks:
#             if not t.done():
#                 t.cancel()
#         raise e



async def verify_first_valid_sequential(email_variants, user_id):
    verifier = EmailVerifier(concurrency=1)  # 1 ensures no parallel SMTPs

    for email in email_variants:
        result = await verifier.verify_email(email, user_id)
        if result.get("valid"):
            return [result]  # ✅ stop immediately on success
    return []  # ❌ all failed

# def main(sample_emails=[""], sample_ids=[""]):
#     print(len(sample_emails), len(sample_ids))
#     if sample_ids == [""]:
#         sample_ids = [0]*len(sample_emails)
#     if not sample_emails:
#         return []
#     verifier = EmailVerifier(concurrency=50)
#     loop = asyncio.get_event_loop()
#     results = loop.run_until_complete(verifier.process_batch(sample_emails, sample_ids))
#     return results

# def main(sample_emails=[""], sample_ids=[""]):
#     if sample_ids == [""]:
#         sample_ids = [0]*len(sample_emails)
#     if not sample_emails:
#         return []

#     email_variants = sample_emails
#     user_id = sample_ids[0]  # Assuming one user at a time

#     loop = asyncio.get_event_loop()
#     results = loop.run_until_complete(verify_first_valid(email_variants, user_id))
#     return results

def main(sample_emails=[""], sample_ids=[""]):
    if sample_ids == [""]:
        sample_ids = [0] * len(sample_emails)
    if not sample_emails:
        return []

    email_variants = sample_emails
    user_id = sample_ids[0]  # Assuming all variants are for 1 user

    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(verify_first_valid_sequential(email_variants, user_id))
    return results
