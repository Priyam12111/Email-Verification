import asyncio
import csv
import logging
from collections import defaultdict
from datetime import datetime
from email.utils import parseaddr
import re
import aiodns
import aiosmtplib
import requests
from tqdm import tqdm

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$')

class EmailVerifier:
    def __init__(self, concurrency=2, timeout=60, smtp_port=587):  # Changed default port to 587
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
            return {'tempmail.com', 'mailinator.com'}

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
        return not (domain.startswith('.') and not domain.endswith('.') and ('.' in domain))

    async def check_disposable(self, domain):
        return domain.lower() in self.disposable_domains

    async def get_mx_servers(self, domain):
        if domain in self.mx_cache:
            return self.mx_cache[domain]
        try:
            records = await self.resolver.query(domain, 'MX')
            mx_records = sorted((record.priority, record.host) for record in records)
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
            self.mx_cache[domain] = []
            return []

    async def smtp_check(self, email, mx_servers):
        domain = email.split('@')[1]
        if domain in self.catch_all_domains:
            return True
        
        self.domain_rates[domain] += 1
        if self.domain_rates[domain] > 5:
            logging.info(f"Rate limiting for {domain}")
            await asyncio.sleep(1)
            self.domain_rates[domain] = 0

        for priority, host in mx_servers:
            try:
                async with self.smtp_semaphore:
                    smtp = aiosmtplib.SMTP(
                        hostname=host,
                        port=self.smtp_port,
                        timeout=self.timeout
                    )

                    await smtp.connect()
                    code, _ = await smtp.ehlo()
                    if code != 250:
                        await smtp.helo()

                    # Add STARTTLS for port 587
                    if self.smtp_port == 587:
                        await smtp.starttls()

                    await smtp.mail(f"no-reply@{domain}")
                    code, _ = await smtp.rcpt(email)
                    await smtp.quit()

                    if code == 250:
                        return True
                    if code == 252:  # Catch-all server
                        self.catch_all_domains.add(domain)
                        return True
            except Exception as e:
                logging.error(f"SMTP error {host}: {str(e)}")
                self.error_desc = str(e)
                if "421" in self.error_desc:  # Server busy
                    await asyncio.sleep(300)
                return False
        return False

    async def verify_email(self, email, id):
        domain = email.split('@')[1] if '@' in email else ''
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not await self.check_syntax(email):
            return self._result(email, False, 'Invalid syntax', domain)

        if await self.check_disposable(domain):
            return self._result(email, False, 'Disposable domain', domain)

        mx_servers = await self.get_mx_servers(domain)
        if not mx_servers:
            return self._result(email, False, 'No MX records', domain)

        if not await self.smtp_check(email, mx_servers):
            return self._result(email, False, 'Mailbox not found', domain)

        # Catch-all check
        if not await self._catch_all_check(domain, mx_servers):
            return self._result(email, True, 'Valid email', domain, catch_all=False)

        return self._result(email, True, 'Valid email', domain, catch_all=True)

    async def _catch_all_check(self, domain, mx_servers):
        if domain == "gmail.com":
            return False
        return await self.smtp_check(f'nonexistent@{domain}', mx_servers)

    def _result(self, email, valid, reason, domain, catch_all=None):
        return {
            'email': email,
            'valid': valid,
            'reason': reason,
            'catch_all': catch_all if catch_all is not None else (domain in self.catch_all_domains),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'error': self.error_desc
        }

    async def process_batch(self, emails, ids):
        tasks = [self.verify_email(email, ids[i]) for i, email in enumerate(emails)]
        return await asyncio.gather(*tasks)

def main(input_file='emails.csv', output_file='results.csv'):
    verifier = EmailVerifier(
        concurrency=20,  # Reduced concurrency for AWS limits
        smtp_port=587    # Explicitly use port 587
    )

    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = [row for row in reader if row]
        emails = [row[0].strip() for row in rows]
        ids = [row[1] if len(row) > 1 else 0 for row in rows]

    results = []
    batch_size = 50  # Reduced batch size for stability
    loop = asyncio.get_event_loop()

    for i in tqdm(range(0, len(emails), batch_size)):
        batch_emails = emails[i:i+batch_size]
        batch_ids = ids[i:i+batch_size]
        try:
            batch_results = loop.run_until_complete(
                verifier.process_batch(batch_emails, batch_ids)
            )
            results.extend(batch_results)
        except Exception as e:
            logging.error(f"Batch error: {str(e)}")

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'valid', 'reason', 'catch_all', 'timestamp', 'error'])
        writer.writeheader()
        writer.writerows(results)

    return verifier.validate_emails

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()