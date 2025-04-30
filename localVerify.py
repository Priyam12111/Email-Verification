import re
from deep import main
import pandas as pd

# Fetch the file
# Create Patterns based on Firstname, Lastname, and Domain
# Create a list of sample emails and ids
PATTERNS = [
    "{first}.{last}@{domain}",
    "{first_initial}{last}@{domain}",
    "{first}@{domain}",
    "{first}.{last_initial}@{domain}",

    "{last}.{first}@{domain}",
    "{last_initial}.{first}@{domain}",
    "{first_initial}.{last}@{domain}",
    "{last_initial}{first_initial}@{domain}",

    "{last}@{domain}",
    "{first}{last_initial}@{domain}",
    "{first}{last}@{domain}",
    "{first}_{last}@{domain}",

    "{first}-{last}@{domain}",
    "{last}{first_initial}@{domain}",
    "{first_initial}{last_initial}@{domain}",
    "{last_initial}{first}@{domain}",
    "{last}{first}@{domain}",
]

class LocalVerify:
    def __init__(self):
        self.filename = "sample.csv"
        self.first_names = []
        self.last_names = []
        self.domains = []
        self.patterns = list()
        self.ids = list()

    def r_csv(self,file_path):
        """Read CSV file and return a list of first names, last names, and domains."""
        df = pd.read_csv(file_path)
        self.first_names = df['FirstName'].tolist()
        self.last_names = df['LastName'].tolist()
        self.domains = df['Domain'].tolist()
        
    def create_patterns(self,firstName,lastName,domain,index):
        pattern =  PATTERNS[index]
        try:
            implied = pattern.format(first=firstName,last=lastName,domain=domain,first_initial=firstName[0],last_initial=lastName[0]).lower().replace('"','').replace("(","").replace(")","")
        except Exception:
            implied = ["None"]
        self.patterns.append(implied)
        self.ids.append(f"{firstName},{lastName}")
    
    
    def wrreplace(self, search_text, replace_text):
        with open(self.filename, 'r+') as f:
            file = f.read()
            file = re.sub(search_text, replace_text, file, 1)
            f.seek(0)
            f.write(file)
            f.truncate()

    def generate_email_patterns(self):
        """Verify email using the patterns."""
        self.r_csv(self.filename)
        for i in range(len(self.first_names)):
            firstName = self.first_names[i]
            lastName = self.last_names[i]
            domain = self.domains[i]
            for index in range(len(PATTERNS)):
                self.create_patterns(firstName,lastName,domain,index)

    def verify_email(self):
        self.generate_email_patterns()
        results = main(self.patterns, self.ids)
        print("Results:")
        for result in results:
            if result['valid']:
                self.wrreplace(result['id'], f"{result['id']},{result['email']}")
                print(f"Email: {result['email']}, Valid: {result['valid']}, Reason: {result['reason']}")
    
l_verify = LocalVerify()
l_verify.verify_email()
