import asyncio
import logging
from python_socks import ProxyType
from python_socks.async_.asyncio import Proxy

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def test_socks5_proxy():
    proxy_config = {
        'proxy_type': ProxyType.SOCKS5,
        'host': '13.203.5.98',
        'port': 1080,
        'username': 'proxyuser',
        'password': 'proxypassd@@12',
        'rdns': True,
        'timeout': 10
    }

    target_host = 'alt1.aspmx.l.google.com'
    target_port = 25

    try:
        print("⌛ Creating proxy connection...")
        proxy = Proxy(
            proxy_type=proxy_config['proxy_type'],
            host=proxy_config['host'],
            port=proxy_config['port'],
            username=proxy_config['username'],
            password=proxy_config['password'],
            rdns=proxy_config['rdns']
        )

        # Connect through proxy - returns a socket object
        print("⌛ Connecting to target through proxy...")
        sock = await asyncio.wait_for(
            proxy.connect(dest_host=target_host, dest_port=target_port),
            timeout=proxy_config['timeout']
        )
        print("✅ Proxy connection established successfully!")

        # Create StreamReader/StreamWriter from socket
        reader, writer = await asyncio.open_connection(sock=sock)
        
        # Test SMTP communication
        print("⌛ Testing SMTP communication...")
        
        # Read SMTP banner
        banner = await reader.readuntil(b'\r\n')
        print(f"📨 SMTP Banner: {banner.decode().strip()}")
        
        # Send EHLO
        writer.write(b"EHLO example.com\r\n")
        await writer.drain()
        
        # Read response
        response = await reader.readuntil(b'\r\n')
        print(f"📨 SMTP Response: {response.decode().strip()}")

    except asyncio.TimeoutError:
        print("❌ Connection timed out - check your proxy server")
    except ConnectionRefusedError:
        print("❌ Connection refused - proxy may not be available")
    except Exception as e:
        print(f"❌ Failed with error: {type(e).__name__}: {str(e)}")
    finally:
        if 'writer' in locals():
            writer.close()
            await writer.wait_closed()
        if 'sock' in locals():
            sock.close()

async def main():
    print("\n🔍 Starting SOCKS5 Proxy Test...")
    await test_socks5_proxy()
    print("Test completed")

if __name__ == "__main__":
    asyncio.run(main())