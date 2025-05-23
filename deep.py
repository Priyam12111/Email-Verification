import asyncio
from datetime import datetime
import logging
import aiodns
import re
import sys
from email.utils import parseaddr
from collections import defaultdict
import requests
import aiosmtplib
import random
from python_socks import ProxyType
from python_socks.async_.asyncio import Proxy

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
logging.basicConfig(filename='email_verifier.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$')

class EmailVerifier:
    def __init__(self, concurrency=10, timeout=120, smtp_port=25):
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
        
        # Configure your proxy here
        self.proxy_list = [
            {
                'proxy_type': ProxyType.SOCKS5,
                'host': '13.203.5.98',
                'port': 1080,
                'username': 'proxyuser',
                'password': 'proxypassd@@12',
                'rdns': True
            }
        ]
        self.current_proxy_index = 0
        self.proxy_failures = defaultdict(int)
        self.max_proxy_failures = 3
        
    def get_next_proxy(self):
        if not self.proxy_list:
            return None
            
        proxy = self.proxy_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        return proxy
        
    async def create_proxy_connection(self, host, port):
        proxy_config = self.get_next_proxy()
        if not proxy_config:
            return None
            
        try:
            proxy = Proxy(
                proxy_type=proxy_config['proxy_type'],
                host=proxy_config['host'],
                port=proxy_config['port'],
                username=proxy_config.get('username'),
                password=proxy_config.get('password'),
                rdns=proxy_config.get('rdns', True)
            )
            
            # Connect through proxy with timeout
            sock = await asyncio.wait_for(
                proxy.connect(dest_host=host, dest_port=port),
                timeout=self.timeout
            )
            return sock
            
        except asyncio.TimeoutError:
            logging.error(f"Proxy connection timed out: {proxy_config}")
        except Exception as e:
            logging.error(f"Proxy connection failed: {proxy_config} - {str(e)}")
        
        self.proxy_failures[str(proxy_config)] += 1
        if self.proxy_failures[str(proxy_config)] >= self.max_proxy_failures:
            self.proxy_list.remove(proxy_config)
        return None
    
    async def create_smtp_connection(self, host):
        if not self.proxy_list:
            # Direct connection without proxy
            smtp = aiosmtplib.SMTP(
                hostname=host,
                port=self.smtp_port,
                timeout=self.timeout
            )
            await smtp.connect()
            return smtp
            
        sock = await self.create_proxy_connection(host, self.smtp_port)
        if sock is None:
            return None
            
        try:
            # Create custom SMTP connection through proxy
            # This is a workaround for aiosmtplib's proxy limitations
            reader, writer = await asyncio.open_connection(sock=sock)
            
            # Create SMTP client manually
            smtp = aiosmtplib.SMTP(
                hostname=host,
                port=self.smtp_port,
                timeout=self.timeout
            )
            
            # Manually initialize the connection
            smtp._reader = reader
            smtp._writer = writer
            # await smtp._ehlo()
            
            return smtp
            
        except Exception as e:
            logging.error(f"SMTP connection failed through proxy: {str(e)}")
            if 'writer' in locals():
                writer.close()
                await writer.wait_closed()
            return None

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
        if random.random() < 0.05:
            self.mx_cache.pop(domain, None)
            
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
        if self.domain_rates[domain] > 5:
            wait_time = random.uniform(1, 5)
            await asyncio.sleep(wait_time)
            
        if domain in self.blocked_domains:
            return False

        for priority, host in mx_servers:
            try:
                async with self.smtp_semaphore:
                    print("host", email, host)
                    smtp = await self.create_smtp_connection(host)
                    if smtp is None:
                        continue
                        
                    code, _ = await smtp.ehlo()
                    if code != 250:
                        await smtp.helo()
                    
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    await smtp.mail("") 
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                
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
                if "4.2.1" in self.error_desc or "421" in self.error_desc:
                    self.blocked_domains.add(domain)
                    logging.info(f"Domain {domain} blocked. Sleeping for 5 minutes.")
                    await asyncio.sleep(300)
                    return False
                elif "connection refused" in str(e).lower():
                    continue
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
                'error': self.error_desc
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
                'catch_all': True,
                'timestamp': current_time
            }
            
        self.validate_emails[id].append(email)
        return {
            'id': id,
            'email': email,
            'valid': True,
            'reason': 'Valid email',
            'catch_all': domain in self.catch_all_domains,
            'timestamp': current_time
        }

    async def process_batch(self, emails, ids):
        results = []
        for i, email in enumerate(emails):
            if i > 0 and i % 10 == 0:
                await asyncio.sleep(random.uniform(1, 3))
                
            result = await self.verify_email(email, ids[i])
            results.append(result)
            
            if (i + 1) % 100 == 0:
                logging.info(f"Processed {i + 1}/{len(emails)} emails")
                
        return results

# async def main():
#     verifier = EmailVerifier(concurrency=100)
#     emails = ["test@example.com", "nonexistent@example.com"]
#     ids = ["123", "456"]
#     results = await verifier.process_batch(emails, ids)
#     print("Results:", results)

# if __name__ == "__main__":
#     asyncio.run(main())

def main(sample_emails=[""], sample_ids=[""]):
    """Example usage without CSV file interactions"""
    print(len(sample_emails), len(sample_ids))
    if sample_ids == [""]:
        sample_ids = [0]*len(sample_emails)
    if not sample_emails:
        return []
    verifier = EmailVerifier(concurrency=100)
    
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(verifier.process_batch(sample_emails, sample_ids))
    return results

if __name__ == "__main__":
    emails = ["test@example.com", "support@hubstaff.com", "developer@acadecraft.com"]
    ids = list(range(len(emails))) # ["232","123", "456", '34']
    result = main(emails, ids)
    print("result", result)