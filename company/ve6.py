import dns.resolver
from datetime import datetime
import re
import csv
import time
import sys
import requests
import smtplib
import threading
from concurrent.futures import ThreadPoolExecutor,as_completed
from email.utils import parseaddr
from collections import defaultdict

EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$')

class EmailVerifierSync:
    def __init__(self, concurrency=50, timeout=15, smtp_port=25):
        self.resolver = dns.resolver.Resolver()
        self.timeout = timeout
        self.smtp_port = smtp_port
        self.disposable_domains = self.load_disposable_domains()
        self.catch_all_domains = set()
        self.blocked_domains = set()
        self.mx_cache = {}
        self.smtp_semaphore = threading.BoundedSemaphore(concurrency)
        self.domain_rates = defaultdict(int)
        self.error_desc = ""
        self.validate_emails = defaultdict(list)
        self.locks = {
            'mx': threading.Lock(),
            'catch_all': threading.Lock(),
            'blocked': threading.Lock(),
            'rates': threading.Lock(),
            'validation': threading.Lock()
        }

    def load_disposable_domains(self):
        try:
            response = requests.get(
                'https://raw.githubusercontent.com/disposable-email-domains/disposable-email-domains/master/disposable_email_blocklist.conf',
                timeout=5
            )
            return set(response.text.splitlines())
        except Exception:
            return {'tempmail.com', 'mailinator.com'}

    def check_syntax(self, email):
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

    def check_disposable(self, domain):
        return domain.lower() in self.disposable_domains

    def get_mx_servers(self, domain):
        with self.locks['mx']:
            if domain in self.mx_cache:
                return self.mx_cache[domain]

        try:
            answers = self.resolver.resolve(domain, 'MX')
            mx_records = sorted([(record.preference, str(record.exchange).rstrip('.')) for record in answers])
            with self.locks['mx']:
                self.mx_cache[domain] = mx_records
            return mx_records
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.Timeout):
            pass

        # Fallback to A/AAAA records
        try:
            self.resolver.resolve(domain, 'A')
            with self.locks['mx']:
                self.mx_cache[domain] = [(0, domain)]
            return self.mx_cache[domain]
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.Timeout):
            pass

        try:
            self.resolver.resolve(domain, 'AAAA')
            with self.locks['mx']:
                self.mx_cache[domain] = [(0, domain)]
            return self.mx_cache[domain]
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.Timeout):
            pass

        with self.locks['mx']:
            self.mx_cache[domain] = []
        return []

    def smtp_check(self, email, mx_servers):
        domain = email.split('@')[1]
        with self.locks['catch_all']:
            if domain in self.catch_all_domains:
                return True

        with self.locks['rates']:
            self.domain_rates[domain] += 1
            if self.domain_rates[domain] > 5:
                time.sleep(60)
                self.domain_rates[domain] = 0

        for priority, host in mx_servers:
            try:
                with self.smtp_semaphore:
                    with smtplib.SMTP(host=host, port=self.smtp_port, timeout=self.timeout) as smtp:
                        code, _ = smtp.ehlo()
                        if code != 250:
                            smtp.helo()
                        
                        smtp.mail(f"no-reply@{domain}")
                        time.sleep(1)
                        
                        code, _ = smtp.rcpt(email)
                        if code == 250:
                            return True
                        if code == 252:  # Catch-all server
                            with self.locks['catch_all']:
                                self.catch_all_domains.add(domain)
                            return True
            except smtplib.SMTPException as e:
                self.error_desc = str(e)
                if "4.2.1" in self.error_desc:
                    with self.locks['blocked']:
                        self.blocked_domains.add(domain)
                        self.catch_all_domains.add(domain)
                    with open('blocked_domains.csv', 'a') as f:
                        f.write(f"{domain}\n")
                    time.sleep(300)
                return False
        return False

    def verify_email(self, email, id):
        parts = email.split('@')
        domain = parts[1] if len(parts) > 1 else ''
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not self.check_syntax(email):
            return {
                'email': email,
                'valid': False,
                'reason': 'Invalid syntax',
                'catch_all': False,
                'timestamp': current_time
            }

        if self.check_disposable(domain):
            return {
                'email': email,
                'valid': False,
                'reason': 'Disposable domain',
                'catch_all': False,
                'timestamp': current_time
            }

        mx_servers = self.get_mx_servers(domain)
        if not mx_servers:
            with self.locks['validation']:
                self.validate_emails[id].append(False)
            return {
                'email': email,
                'valid': False,
                'reason': 'No MX records',
                'catch_all': False,
                'timestamp': current_time
            }

        if not self.smtp_check(email, mx_servers):
            with self.locks['validation']:
                self.validate_emails[id].append(False)
            return {
                'email': email,
                'valid': False,
                'reason': 'Mailbox not found',
                'catch_all': False,
                'timestamp': current_time,
                'error': self.error_desc
            }

        # Catch-all check
        if self.smtp_check(f'nonexistent@{domain}', mx_servers) and domain != "gmail.com":
            with self.locks['validation']:
                self.validate_emails[id].append(email)
            with self.locks['catch_all']:
                self.catch_all_domains.add(domain)
            with open('catch_all_domains.csv', 'a') as f:
                f.write(f"{domain}\n")
            return {
                'email': email,
                'valid': True,
                'reason': 'Valid (catch-all)',
                'catch_all': True,
                'timestamp': current_time
            }

        with self.locks['validation']:
            self.validate_emails[id].append(email)
        return {
            'email': email,
            'valid': True,
            'reason': 'Valid email',
            'catch_all': False,
            'timestamp': current_time
        }

    def process_batch(self, emails, ids):
        results = []
        with ThreadPoolExecutor(max_workers=self.smtp_semaphore._value) as executor:
            # Create a mapping of future to its email
            future_to_email = {
                executor.submit(self.verify_email, email, ids[i]): email
                for i, email in enumerate(emails)
            }
            for future in as_completed(future_to_email):
                email = future_to_email[future]
                try:
                    results.append(future.result())
                except Exception as e:
                    results.append({
                        'email': email,
                        'valid': False,
                        'reason': 'Verification error',
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
        return results


def main_sync(input_file='emails.csv', output_file='results.csv'):
    verifier = EmailVerifierSync(concurrency=50)
    emails = []
    ids = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            emails.append(row[0].strip())
            ids.append(row[1] if len(row) > 1 else 0)

    results = []
    batch_size = 100
    start_time = time.time()

    for i in range(0, len(emails), batch_size):
        batch = emails[i:i+batch_size]
        batch_ids = ids[i:i+batch_size]
        try:
            batch_results = verifier.process_batch(batch, batch_ids)
            results.extend(batch_results)
        except Exception as e:
            pass
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['email', 'valid', 'reason', 'catch_all', 'timestamp', 'error'])
        writer.writeheader()
        writer.writerows(results)

    return verifier.validate_emails

def check_emails_sync(emails, ids=None, output_file=None):
    verifier = EmailVerifierSync(concurrency=50)
    ids = ids or [0]*len(emails)
    results = verifier.process_batch(emails, ids)
    
    if output_file:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['email', 'valid', 'reason', 'catch_all', 'timestamp', 'error'])
            writer.writeheader()
            writer.writerows(results)
    
    return results

if __name__ == '__main__':
    main_sync()