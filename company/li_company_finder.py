import os
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup
import pymongo
import requests
from consumer import *


MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:AdminStrongPass123@14.195.222.181:8989/admin",
)
DB_NAME = "e-finder"
COLLECTION_NAME = "company-1"

CSV_PATH = Path("company") / "formatter.csv"


client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME][COLLECTION_NAME]


def sanitize_company_name(raw_row: str) -> str:
    """Normalize the company name extracted from the first CSV column."""

    name = raw_row.split(",")[0].lower()
    for suffix in (" ltd", " ind", " inc", " llc", " corp", " co"):
        name = name.replace(suffix, "")
    return name.strip()


def find_company_by_name(name_lc: str) -> Optional[str]:
    """Return the first matching company's salesUrl for a name prefix."""

    cursor = db.find({"name_lc": {"$regex": f"^{name_lc}"}}).limit(1)

    explain = cursor.explain()
    stats = explain.get("executionStats", {})
    keys_examined = stats.get("totalKeysExamined")
    docs_examined = stats.get("totalDocsExamined")
    print("keysExamined:", keys_examined, "docsExamined:", docs_examined)

    company = next(cursor, None)
    return company.get("salesUrl") if company else None


def get_li_page(
    url: str,
) -> Optional[tuple[str, str, int]]:
    """Convert a LinkedIn company URL to its Sales Navigator equivalent."""
    if not url:
        return None

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-ch-prefers-color-scheme": "dark",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    }

    cookies = {
        "bcookie": "v=2&2190d805-ace0-478a-8743-f1df9156e5da",
        "lang": "v=2&lang=en-us",
        "lidc": "b=VGST02:s=V:r=V:a=V:p=V:g=3679:u=1:x=1:i=1766135268:t=1766221668:v=2:sig=AQEf2QrJgi7_99jW35ymc-dKnLS_dYAW",
        "JSESSIONID": "ajax:0475566273713126418",
        "bscookie": "v=1&2025121909074783d412ae-0f13-4cc2-8c9e-948a65f31919AQFD2igWmhmjwk4BC5PsMdl_fv-OoA3C",
    }

    resp = requests.get(url, headers=headers, timeout=20)
    if resp.status_code != 200:
        print(resp.text)
        return None
    html = BeautifulSoup(resp.text, "html.parser")
    top_card = html.find("div", class_="top-card-layout__entity-info-container")
    if top_card:
        h1 = top_card.find("h1")
        h2 = top_card.find("h2")
        p = top_card.find_all("p")[-1]
        emp = (
            p.text.strip().split(" ")[-2]
            if p and ("employees") in p.text.lower()
            else "N/A"
        )
        company = h1.text.strip() if h1 else "N/A"
        industry = h2.text.strip() if h2 else "N/A"
        try:
            nEmp = int(emp.replace(",", "")) if emp != "N/A" else 0
        except ValueError:
            nEmp = 0
        return (company, industry, nEmp)
    return None


def main(start=0, grind=False) -> None:
    data = CSV_PATH.read_text(encoding="utf-8")
    rows = data.splitlines()
    filter_rows = [
        row.split(",")[0]
        for row in rows
        if row.endswith(",None") and len(row.split(",")) == 2
    ]
    if grind:
        process(filter_rows)
        return
    for row in rows[start:]:
        name_lc = sanitize_company_name(row)
        url = find_company_by_name(name_lc)
        # Note: wrreplace does a regex-based single replacement.
        wrreplace(str(CSV_PATH), row, f"{row},{url}")
        print(name_lc, url)


if __name__ == "__main__":
    # print(get_li_page("https://www.linkedin.com/company/coursera/"))
    main(73, grind=True)
