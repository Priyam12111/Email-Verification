import asyncio
import time
from datetime import datetime
from python_socks import ProxyType
from python_socks.async_.asyncio import Proxy
import aiosmtplib
import aiodns
from collections import defaultdict
import sys
import re

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
class ProxyManager:
    def __init__(self, proxy_configs):
        self.proxy_configs = proxy_configs or []
        self.proxy_status = {cfg['host']: {'last_failure': 0, 'retries': 0} for cfg in proxy_configs}
        self.current_index = 0
        
    def get_next_proxy(self):
        if not self.proxy_configs:
            return None
            
        for _ in range(len(self.proxy_configs)):
            proxy = self.proxy_configs[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxy_configs)
            
            # Skip proxies that failed recently
            if time.time() - self.proxy_status[proxy['host']]['last_failure'] > 300:  # 5 min cooldown
                return proxy
                
        return None  # All proxies in cooldown
        
    def mark_failed(self, host):
        if host in self.proxy_status:
            self.proxy_status[host]['last_failure'] = time.time()
            self.proxy_status[host]['retries'] += 1

class EmailVerifier:
    def __init__(self, proxy_configs=None, concurrency=100, timeout=30):
        self.proxy_manager = ProxyManager(proxy_configs)
        self.resolver = aiodns.DNSResolver()
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(concurrency)
        self.mx_cache = {}
        self.domain_rates = defaultdict(int)
        
    async def test_proxy_connection(self, proxy_config):
        try:
            proxy = Proxy(
                proxy_type=proxy_config['proxy_type'],
                host=proxy_config['host'],
                port=proxy_config['port'],
                username=proxy_config['username'],
                password=proxy_config['password'],
                rdns=proxy_config.get('rdns', True)
            )
            sock = await asyncio.wait_for(
                proxy.connect(dest_host='google.com', dest_port=80),
                timeout=10
            )
            sock.close()
            return True
        except Exception as e:
            print(f"Proxy test failed for {proxy_config['host']}: {e}")
            return False

    async def get_mx_servers(self, domain):
        if domain in self.mx_cache:
            return self.mx_cache[domain]
            
        try:
            records = await self.resolver.query(domain, 'MX')
            mx_records = sorted([(record.priority, record.host) for record in records])
            self.mx_cache[domain] = mx_records
            return mx_records
        except Exception:
            return []

    async def smtp_check(self, email, mx_servers):
        domain = email.split('@')[1]
        self.domain_rates[domain] += 1
        
        if self.domain_rates[domain] > 5:
            await asyncio.sleep(1)  # Rate limiting
            
        for priority, host in mx_servers:
            for attempt in range(2):  # Retry once
                proxy_config = self.proxy_manager.get_next_proxy()
                try:
                    async with self.semaphore:
                        if proxy_config:
                            proxy = Proxy(
                                proxy_type=proxy_config['proxy_type'],
                                host=proxy_config['host'],
                                port=proxy_config['port'],
                                username=proxy_config['username'],
                                password=proxy_config['password'],
                                rdns=proxy_config.get('rdns', True)
                            )
                            sock = await proxy.connect(dest_host=host, dest_port=25)
                            smtp = aiosmtplib.SMTP(timeout=self.timeout)
                            await smtp.connect(sock=sock)
                        else:
                            smtp = aiosmtplib.SMTP(hostname=host, port=25, timeout=self.timeout)
                            await smtp.connect()

                        code, _ = await smtp.ehlo()
                        await smtp.mail('test@example.com')
                        code, _ = await smtp.rcpt(email)
                        await smtp.quit()
                        return code == 250
                        
                except Exception as e:
                    if proxy_config:
                        self.proxy_manager.mark_failed(proxy_config['host'])
                    if attempt == 0:
                        continue  # Retry once
                    print(f"SMTP check failed for {email} @ {host}: {e}")
                    
        return False

async def main():
    # Example proxy configurations
    proxy_configs = [
        # {
        #     'proxy_type': ProxyType.SOCKS5,
        #     'host': '13.203.5.98',
        #     'port': 1080,
        #     'username': 'proxyuser',
        #     'password': 'proxypassd@@12',
        #     'rdns': True
        # },
        {
            'proxy_type': ProxyType.SOCKS5,
            'host': '104.219.171.245',
            'port': 50101,
            'username': '3bBijqtx',
            'password': 'I24hk93mt8',
            'rdns': True
        },
        # {
        #     'proxy_type': ProxyType.SOCKS5,
        #     'host': '13.203.5.99',
        #     'port': 1080,
        #     'username': 'proxyuser',
        #     'password': 'proxypass',
        #     'rdns': True
        # }
    ]
    
    verifier = EmailVerifier(proxy_configs=proxy_configs)
    
    # Test all proxies first
    print("Testing proxy connections...")
    for config in proxy_configs:
        if await verifier.test_proxy_connection(config):
            print(f"✅ {config['host']}:{config['port']} is working")
        else:
            print(f"❌ {config['host']}:{config['port']} failed")
    
    # Verify sample emails
    emails = [
        "test@gmail.com",
        "contact@microsoft.com",
        "support@hubstaff.com",
        "developer@acadecraft.com",
        "conrad@fcbbanks.com",
        "invalid@example.com"
    ]
    
    print("\nVerifying emails...")
    for email in emails:
        domain = email.split('@')[1]
        mx_servers = await verifier.get_mx_servers(domain)
        if not mx_servers:
            print(f"{email}: No MX records")
            continue
            
        is_valid = await verifier.smtp_check(email, mx_servers)
        print(f"{email}: {'✅ Valid' if is_valid else '❌ Invalid'}")

if __name__ == "__main__":
    # asyncio.run(main())
        email = 'abhishe_k_@renew.com'
        email = re.sub(r'[.\-_]+', lambda m: m.group(0)[0], email)  # Collapse repeats, keep only one

        # Remove any of . - _ before @
        email = re.sub(r'[.\-_]+@', '@', email)

        # Strip special characters (., -, _) from beginning and end
        email = email.strip('.-_')
        print(email)