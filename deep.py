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
        self.catch_all_domains = set()
        self.blocked_domains = set()
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
            return True
        self.domain_rates[domain] += 1
        if (self.domain_rates[domain]) > 5:
            self.domain_rates[domain] = 0
        for priority, host in mx_servers:
            try:
                async with self.smtp_semaphore:
                    smtp = aiosmtplib.SMTP(hostname=host, port=self.smtp_port, timeout=self.timeout)
                    await smtp.connect()
                    code, _ = await smtp.ehlo()
                    if code != 250:
                        await smtp.helo()
                    
                    await smtp.mail("") 
                    await asyncio.sleep(1)
                
                    code, _ = await smtp.rcpt(email)
                    await smtp.quit()
                    if code == 250:
                        return True
                    elif code == 252:
                        self.catch_all_domains.add(domain)
                        return True
            except Exception as e:
                logging.error(f"SMTP check error for {email} on {host}: {e}")
                self.error_desc = str(e)
                if "4.2.1" in self.error_desc:
                    self.blocked_domains.add(domain)
                    self.catch_all_domains.add(domain)
                    logging.info(f"Sleeping for 5 minutes due to blocked domain {domain}.")
                    await asyncio.sleep(random.uniform(0.5, 1.5)*30)
                return False
        return False

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

        smtp_valid = await self.smtp_check(email, mx_servers)
        if not smtp_valid:
            self.validate_emails[id].append(False)
            return {
                'id': id,
                'email': email,
                'valid': False,
                'reason': 'Mailbox not found',
                'catch_all': domain in self.catch_all_domains,
                'timestamp': current_time,
                'Reason': self.error_desc
            }
        smtp_valid = await self.smtp_check(f'nonexistent@{domain}', mx_servers)
        if smtp_valid and domain != "gmail.com":
            self.validate_emails[id].append(email)
            self.catch_all_domains.add(domain)
            return {
                'id': id,
                'email': email,
                'valid': True,
                'reason': 'Valid email',
                'catch_all': True
            }
        self.validate_emails[id].append(email)

        return {
            'id': id,
            'email': email,
            'valid': True,
            'reason': 'Valid email',
            'catch_all': domain in self.catch_all_domains
        }

    async def process_batch(self, emails, ids):
        tasks = [self.verify_email(email, ids[i]) for i, email in enumerate(emails)]
        return await asyncio.gather(*tasks)


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

if __name__ == "__main__":
    main(["priyam133@me.com","shubham@acadecraft.com"], ["123", "456"])