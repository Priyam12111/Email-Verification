import asyncio
from datetime import datetime
import logging
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


if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    

logging.basicConfig(filename='email_verifier.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$')

class EmailVerifier:
    def __init__(self, concurrency=2, timeout=120, smtp_port=25):
        self.resolver = aiodns.DNSResolver()
        self.timeout = timeout
        self.smtp_port = smtp_port
        self.disposable_domains = self.load_disposable_domains()
        # self.catch_all_domains = set()
        self.catch_all_domains = self.load_blocked_domains("catch_all_domains.csv")
        # self.blocked_domains = set()
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
            self.domain_rates[domain] = 0  # Optional rate limiting logic

        for priority, host in mx_servers:
            try:
                async with self.smtp_semaphore:
                    smtp = aiosmtplib.SMTP(hostname=host, port=self.smtp_port, timeout=self.timeout)
                    await smtp.connect()

                    code, _ = await smtp.ehlo()
                    if code != 250:
                        await smtp.helo()  # fallback EHLO

                    await smtp.mail("verifier@example.com")  # proper MAIL FROM
                    await asyncio.sleep(1)  # small pause before RCPT

                    code, response = await smtp.rcpt(email)
                    await smtp.quit()

                    if code == 250:
                        return True, "Valid recipient"
                    elif code == 550:
                        return False, "Mailbox not found"
                    elif code == 552:
                        return False, "Mailbox full"
                    elif code == 451:
                        return False, "Temporary local problem"
                    elif code == 421:
                        return False, "Service not available"
                    elif code == 252:
                        # 252 is ambiguous — may mean "cannot verify but will accept"
                        self.catch_all_domains.add(domain)
                        return True, "Possible catch-all (252)"
                    else:
                        return False, f"Unhandled SMTP code {code}: {response.decode() if hasattr(response, 'decode') else response}"

            except Exception as e:
                self.error_desc = str(e)
                logging.error(f"[SMTP ERROR] {email} @ {host}: {e}")

                # Handle blocking
                if "4.2.1" in self.error_desc:
                    self.blocked_domains.add(domain)
                    self.catch_all_domains.add(domain)
                    logging.warning(f"[Blocked] Sleeping due to rate limit on {domain}")
                    await asyncio.sleep(random.uniform(15, 45))  # back-off strategy

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
                'catch_all': domain in self.catch_all_domains,
                'timestamp': current_time
            }

        if await self.check_disposable(domain):
            return {
                'id': id,
                'email': email,
                'valid': False,
                'reason': 'Disposable domain',
                'catch_all': domain in self.catch_all_domains,
                'timestamp': current_time
            }

        mx_servers = await self.get_mx_servers(domain)
        if not mx_servers:
            self.validate_emails[id].append(False)
            return {
                'id': id,
                'email': email,
                'valid': False,
                'reason': 'No MX records/servers',
                'catch_all': domain in self.catch_all_domains,
                'timestamp': current_time
            }

        # Step 1: Check if actual email exists
        smtp_valid = await self.smtp_check(email, mx_servers)
        if smtp_valid:
            self.validate_emails[id].append(email)
            return {
                'id': id,
                'email': email,
                'valid': True,
                'reason': 'Valid email',
                'catch_all': domain in self.catch_all_domains
            }

        # Step 2: Check catch-all behavior only if email not valid and domain not already known
        if domain not in self.catch_all_domains and domain != "gmail.com":
            catch_all_test = f"random_{random.randint(1000,9999)}_{int(time.time())}@{domain}"
            catch_all_result = await self.smtp_check(catch_all_test, mx_servers)

            if catch_all_result:
                self.catch_all_domains.add(domain)
                return {
                    'id': id,
                    'email': email,
                    'valid': False,
                    'reason': 'Invalid email (catch-all)',
                    'catch_all': True,
                    'timestamp': current_time
                }

        self.validate_emails[id].append(False)
        return {
            'id': id,
            'email': email,
            'valid': False,
            'reason': 'Mailbox not found',
            'catch_all': domain in self.catch_all_domains,
            'timestamp': current_time,
            'smtp_error': self.error_desc
        }

    async def process_batch(self, emails, ids):
        tasks = [self.verify_email(email, ids[i]) for i, email in enumerate(emails)]
        return await asyncio.gather(*tasks)

    def load_blocked_domains(self, filename):
        blocked = set()
        try:
            with open(filename, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if row:  # Avoid empty rows
                        blocked.add(row[0].strip().lower())  # Assuming one domain per line
        except FileNotFoundError:
            print(f"[Warning] File '{filename}' not found. No blocked domains loaded.")
        return blocked


def main(sample_emails=[""], sample_ids=[""]):
    """Example usage without CSV file interactions"""
    print(len(sample_emails), len(sample_ids))
    if sample_ids == [""]:
        sample_ids = [0]*len(sample_emails)
    if not sample_emails:
        return []
    verifier = EmailVerifier(concurrency=1000)
    
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(verifier.process_batch(sample_emails, sample_ids))
    return results

# if __name__ == "__main__":
#     main(["priyam133@me.com","shubham@acadecraft.com"], ["123", "456"])