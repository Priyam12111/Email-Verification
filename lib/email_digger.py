import dns.resolver
import smtplib

def get_smtp_server(domain):
    try:
        mx_records = dns.resolver.resolve(domain, 'MX')
        smtp_servers = [str(record.exchange)[:-1] for record in mx_records]
        return smtp_servers
    except dns.resolver.NoAnswer:
        return None
    except dns.resolver.NXDOMAIN:
        return None
    except dns.exception.Timeout:
        return None


def verify_email_address(smtp_server, email_address):
    try:
        server = smtplib.SMTP(smtp_server)
        server.ehlo_or_helo_if_needed()
        code, message = server.verify(email_address)
        server.quit()
        return code, message
    except smtplib.SMTPException as e:
        return None, str(e)


def check_email_status(email_address):
    domain = email_address.split('@')[-1]
    smtp_servers = get_smtp_server(domain)
    if not smtp_servers:
        return 'not_valid'
    for smtp_server in smtp_servers:
        code, _ = verify_email_address(smtp_server, email_address)
        if code in [250, 251]:
            return 'may_be_valid'
        elif code in [252]:
            return 'may_be_valid'
        elif code in [450, 550, 551, 553]:
            return 'not_valid'
    return 'not_valid'


def main():
    email_address = "john.doe@microsoft.com"
    status = check_email_status(email_address)
    print("Email address status:", status)


if __name__ == "__main__":
    main()


