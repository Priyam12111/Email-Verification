import unicodedata
import re
import csv

# List of prefixes to remove (converted to lowercase)
prefixes = {
    "asst. prof.", "asst.", "prof.", "dr.", "ph.d.",'m.sc.','mba-pmp','mcse', "jr.", "ar.", "ms.", "miss.", "mrs.",
    "capt.", "cmdr.", "adm.", "amb.", "sgt.", "fr.", "hon.", "lt.", "maj.", "gen.", "md.",
    "adv.", "mr.", "mx.", "engr.", "rev.", "er.","ass.","professor","ca.", "ca ","chrp.","cma",
    "sr.","ao.","univ.-prof",
    
    "l.i.s.w.-s",
    "phd","ph.d", "mba","msc","mni","tap.cert"," ca","PHR","pmp","md.","mpa","m.s.ed","ed.s.","rttp","clp","m. ed","lssbb","ctt",
    "drph","mph.","tap.dip","fcipd","cipd","assoc.","assoc","doctor","ed.d","m.ed","b.ed","m.eng","prosser","m.s.",
    "cdmpa","m.r","m.b.a","b.a","bsc.","eng","med","bed","m.ed.","b.ed.","m.d.","bscn","cpcu","cap.","cpp.","apmp",
    "cfps","cgma","acma",'iit',"iim","xlri","cdo","chro","chro - ","mcmi","cmgr","pgdip","prof.",
    "ebp.csrt","mba.rn","mba-ir","pme","assoc.cipd","mics","m.a.sc","csw",
    "iv","iii","ii","ll.b","ed","ma.",
}

# # Make pattern flexible to match prefix + (space OR .letter)
# prefix_pattern = r"^(?:" + r"|".join(re.escape(p) for p in sorted(prefixes, key=len, reverse=True)) + r")(?=\s|[a-z])"

# def remove_prefixes(name):
#     name = name.lower().strip()
    
#     while True:
#         match = re.match(prefix_pattern, name)
#         if not match:
#             break
#         name = name[len(match.group(0)):].lstrip()

#     return name
normalized_prefixes = {re.sub(r'[^\w]', '', p.lower()) for p in prefixes}

def remove_prefixes(name):
    name = name.strip()
    tokens = re.split(r"[\s,]+", name)

    while tokens:
        first_token_norm = re.sub(r'[^\w]', '', tokens[0].lower())
        if first_token_norm in normalized_prefixes:
            tokens.pop(0)
        else:
            break

    return " ".join(tokens)

otherfixes = {
    "psy. m.","- chro -","ed. d","cpp.cap.apmp",
}

# Sort by length to match longer ones first
sorted_otherfixes = sorted(otherfixes, key=len, reverse=True)

# Build pattern to match any of the otherfixes at the end (with optional comma/space before it)
otherfix_pattern = r"(?:,?\s*(?:" + "|".join(re.escape(p) for p in sorted_otherfixes) + r"))\s*$"

def remove_otherfixes(name):
    name = name.strip()
    # Keep removing matching postfix strings from the end
    while re.search(otherfix_pattern, name, flags=re.IGNORECASE):
        name = re.sub(otherfix_pattern, '', name, flags=re.IGNORECASE).strip()
    return name

def remove_postfixes(name):
    name = name.strip()
    tokens = re.split(r"[\s,]+", name)

    while tokens:
        last_token_norm = re.sub(r'[^\w]', '', tokens[-1].lower())
        if last_token_norm in normalized_prefixes:
            tokens.pop()
        else:
            break

    return " ".join(tokens)

post2fixes = {
    "ed. d","m. ed","ph. d"
}

# Regex: Match any of the post2fixes at the end, preceded by optional space or comma
postfix_pattern = r"(?:,?\s*(?:" + r"|".join(re.escape(p) for p in sorted(post2fixes, key=len, reverse=True)) + r"))+$"

def remove_post2fixes(name):
    name = name.strip().lower()
    # Keep removing matching post2fixes from the end
    name = re.sub(postfix_pattern, '', name).strip()
    return name
def clean_edges(name):
    name = name.strip()
    return name.strip("-. _")

def clean_and_extract(name):
    # 1: Remove accents
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode()
    
    # 2: Convert to lowercase
    name = name.lower()
    # 3: Remove text inside parentheses
    name = re.sub(r'\(.*?\)', '', name)
    name = re.sub(r'\[.*?\]', '', name)
    
    # 4: Split by comma
    parts = [part.strip() for part in name.split(',') if part.strip()]
    name = parts[0] if parts else ''
    
    # 5: Remove prefix/postfix at the start
    name = remove_post2fixes(name)
    name = remove_prefixes(name)
    name = remove_postfixes(name)
    name = remove_otherfixes(name)
    # print("name22=", name)
    name = name.split('(', 1)[0].strip()
    # 6. Remove all characters not allowed in email
    # Allow: a-z, 0-9, @, ., _, -
    name = re.sub(r'[^a-z0-9._-]', ' ', name)
    
    # 7: Strip and remove extra spaces
    name = ' '.join(name.strip().split())
    
    name = clean_edges(name)
    # 8. Final trim
    name = name.strip()
    return name
    
    
# Test examples
# print(clean_and_extract("CHAYAN B. - CHRO - CDO XLRI IIM IIT"))  
# print(clean_and_extract("Brittney Freeman M. Ed"))  
# print(clean_and_extract("Victor Spotloe Jr. CSW"))  

# Read input CSV and write to output CSV
# with open('name.csv', newline='', encoding='utf-8') as infile, \
#      open('output.csv', 'w', newline='', encoding='utf-8') as outfile:

#     reader = csv.reader(infile)
#     writer = csv.writer(outfile)
    
#     # Write header
#     writer.writerow(['old_name', 'new_name', 'first_name', 'last_name'])
    
#     for row in reader:
#         if not row: continue  # Skip empty rows
#         old_name = row[0]
#         new_name = clean_and_extract(old_name)
#         name_parts = new_name.replace('.', ' ').replace('_', ' ').split()
#         first = name_parts[0] if len(name_parts) > 0 else ''
#         last = name_parts[-1].rsplit('-', 1)[-1] if len(name_parts) > 1 else ''
#         writer.writerow([old_name, new_name, first, last])