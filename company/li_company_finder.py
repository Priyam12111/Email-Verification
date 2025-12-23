import os
from pathlib import Path
from typing import Optional
from bs4 import BeautifulSoup
import pymongo
import requests
from consumer import *
from company_data_retrieve import (
    fetch_linkedin_company_data,
    iter_first_element_children,
    to_company_url,
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:AdminStrongPass123@14.195.222.181:8989/admin",
)
DB_NAME = "e-finder"
COLLECTION_NAME = "company-1"
CSV_NAME = "Training and development companies.csv"
CSV_PATH = Path("company") / CSV_NAME


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
    if not url.startswith("https://www.linkedin.com/company/"):
        return None
    company_url_id = url.split("https://www.linkedin.com/company/")[-1].strip("/")
    cookie = True
    try:
        int(company_url_id)
    except ValueError:
        cookie = False
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
        "Cookie": (
            'bcookie="v=2&bf7ed467-3c93-4d1c-8a4b-c28199502403"; bscookie="v=1&20251201062241f11b4250-b4c2-4f19-8c55-dd6d8205bf23AQF1uueACroQVe40Z-spqwjlUDqIngm9"; timezone=Asia/Calcutta; li_theme=light; li_theme_set=app; dfpfpt=38371b022267448da88eb7d2a2e2d275; _pxvid=252cba50-ce7e-11f0-8059-9f84b84af31d; sdui_ver=sdui-flagship:0.1.22557+SduiFlagship0; visit=v=1&M; fid=AQGIEXayR8hMJgAAAZsq29hgQLJu7xHS5ufEIGga4i-32lZhVylK-hN81qsQCeG10y0w7v-_0NFDog; li_rm=AQE4En9C6GMeUQAAAZsq5Ieg7J7ZYwTNgUbQkIzDoJAh2Ki57D1ufNkP95Vg3i30F-Qdbk3iXI-Mgfcea7aiSAzDdvezf0GrfkfFemi57warsNamnV4XRu7pVvHp1hfGap4u3w7nR-3n2VDL3i3UmLMxJ7v7kXira6w_daveYKn5TKiWlIR4ybpVYe2ZhcAOK1vglUfR18qy7E6M2oQJGDDs-oUTy5z1DQXjYm0EOpu70_5luoSLgcNAzb24MAfyMNHc9nc0IBE5mAgthSZaB89N39K3XQZ7taK2-mhtRreu4zYpuKORAV566mSGXUnyXtC8gPVz-8WqUuz_ND_Llg; JSESSIONID="ajax:4043523951376788201"; g_state={"i_l":0}; liap=true; li_at=AQEDAT6ZujgBWrfAAAABmzaVJUcAAAGbWqGpR1YAgdpu_NsDxp36rDwyhO4M_TY4qUltIyOgzbGO6Y_QJLDAakoWLYHsawSzhfQNqF_P4UP6wPgh-hebVyCciIY1crOKI8tQRk2_zKJBd3xV2SiCm4Kr; lidc="b=OB72:s=O:r=O:a=O:p=O:g=569:u=144:x=1:i=1766147303:t=1766221373:v=2:sig=AQFSoRtYKp0SKJ4huB_qhctO2gMquHc-"; UserMatchHistory=AQJzGLKpigN6jgAAAZs2v_S6uF0SA4lkS5oBD84Wqy-k9ZCQKFRmVmU60sxWDREFgerlBksvW2X7jpIM8tB8jxTgQWAMBvuhhrGh8LjE3WB8Cgq90CTMEiu6KTFzbkiA5li9u19wAqhXX1xpQHibnlshUTJtZinpKho9Q9gALVwPqEtpqXY2WiADl8d2uK_2XAqtfQm4nEVGonEwSkVzZ8BayNQaBYE1E1JolqRx7pVXGGmOrSv8bdl8z4nvsmhp3PmMV7eu5DMwA9bGt10ZyoAc7gGmhavch8DJxmgFEu34JsHzkWHblU9WG3yf0SD4CQgbwgamK5Q1d1Q4SHx_; fptctx2=taBcrIH61PuCVH7eNCyH0J9Fjk1kZEyRnBbpUW3FKs9tGxvFYomOX3g9ICSZbFXeucH8ESkVbDYlYqJ8q%252fQskF1V1hgQfEMCoeucYm1aTH3yBrsGjvTvuMtLVfyitJt%252fPwc2LBeecoY40gzXsOExwicSEyVPIcokXXl0RFMl%252fy1lp5JEWt%252f7DAvy4p0F68y%252fwGfWADRmK%252fUEwJipceFYk6KYVgPpVEkfZX13QJaFHp9vQNlZiMxyTaaD9nVKq00amN3%252bU6AZ9Ml7BYjErguV7NdyBcsTkwRfYLDRTIdr%252bBaGC1U53AZNEKaEEwXrROLMi8oB43LhNwA77pFtSf%252fmvD7DVnUvs2bcfotiG96lk%252bo%253d; PLAY_LANG=en; lang=v=2&lang=en-US"'
            if cookie
            else ""
        ),
    }

    resp = requests.get(url, headers=headers, timeout=20)
    print(resp.url)  # final URL after redirect
    print(resp.history)
    if resp.status_code != 200:
        print(resp.text)
        return None
    html = BeautifulSoup(resp.text, "html.parser")
    top_card = html.find("div", class_="top-card-layout__entity-info-container")
    print(top_card)
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
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        rows = f.read().splitlines()
    start = 11
    end = 20
    for row in rows[start:end]:
        name_lc = sanitize_company_name(row)
        expected_url = row.split(",")[1].strip()
        url = find_company_by_name(name_lc)
        print("name:", name_lc, "url:", url)
        if not url:
            link = process([name_lc], db=db, CSV_NAME=CSV_NAME)
            continue
        with requests.Session() as session:
            payload = fetch_linkedin_company_data(name_lc, session=session)
            if payload:
                print("Fetched data for:", name_lc)
                for child in iter_first_element_children(payload):
                    child_id = child.get("id", "")
                    company_url = to_company_url(child_id)
                    if not company_url:
                        continue

                    if company_url == expected_url:
                        employee_count_range = child.get("employeeCountRange", {})
                        industry = child.get("industry", {})
                        address = child.get("address", {})
                        country = (
                            address.get("country", "")
                            if isinstance(address, dict)
                            else ""
                        )
                        print(
                            f'Match found for "{name_lc}" with ID: {company_url} '
                            f"employeeCountRange: {employee_count_range} "
                            f"industry: {industry} "
                            f"country: {country}"
                        )
                        print(repr(expected_url))
                        status = db.update_one(
                            {"salesUrl": url},
                            {
                                "$set": {
                                    "employeeCountRange": employee_count_range,
                                    "industry": industry,
                                    "country": country,
                                    "excel_source": CSV_NAME,
                                }
                            },
                        )
                        print("Updated documents:", status.modified_count)
                        print("----")
