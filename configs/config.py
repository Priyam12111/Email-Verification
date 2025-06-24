class Config:
    PATTERNS = [
        "{first}.{last}@{domain}",
        "{first_initial}{last}@{domain}",
        "{first}@{domain}",
        "{first}.{last_initial}@{domain}",

        # "{last}.{first}@{domain}",
        # "{last_initial}.{first}@{domain}"
        # "{first_initial}.{last}@{domain}",
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


    SENDING_PATTERNS = [
        "{first}.{last}@{domain}",
        "{first_initial}{last}@{domain}",
        "{first}@{domain}",
        "{first}.{last_initial}@{domain}"
    ]


    SETTINGS = {
            'google' : {
                'url' : "https://accounts.google.com",
                'pos' : {
                    'username' : (727, 322),
                    'submit' : (1086, 515)
                },
                'selectors' : {
                    'username' : '//*[@id="identifierId"]',
                    'response' : '//*[@id="yDmH0d"]/c-wiz/div/div[2]/div/div/div[1]/form/span/section/div/div/div[1]/div/div[2]/div[2]/div',
                    'confirm' : '//*[@id="headingText"]',
                    'workspace_confirm' : '/html/body/div[2]/div[1]/div[2]/h1/span'
                }
            },
            'microsoft' : {
                'url' : "https://myaccount.microsoft.com",
                'pos' : {
                    'username' : (511, 333),
                    'submit' : (800, 462),
                },
                'selectors' : {
                    'username' : '//*[@id="i0116"]',
                    'response' : '//*[@id="usernameError"]'
                }
            }
        }