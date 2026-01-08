from __future__ import annotations

import argparse
import csv
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any, Iterable, Optional

import pymongo
import requests


LINKEDIN_SALES_FACET_TYPEAHEAD_URL = (
    "https://www.linkedin.com/sales-api/salesApiFacetTypeahead"
)


logger = logging.getLogger(__name__)


MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:AdminStrongPass123@14.195.222.181:8989/Syncupteams?authSource=admin&directConnection=true",
)
DB_NAME = "e-finder"
COLLECTION_NAME = "company-1"


client = pymongo.MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=3000,
    maxPoolSize=10,
    connectTimeoutMS=3000,
    socketTimeoutMS=3000,
)
db = client[DB_NAME][COLLECTION_NAME]
DEFAULT_PARAMS: dict[str, Any] = {
    "q": "query",
    "start": 0,
    "count": 10,
    "type": "COMPANY_WITH_LIST",
    "query": "",  # filled per request
}


# NOTE: These headers (especially Cookie/CSRF) are session-specific.
# They are kept here to preserve current behavior, but consider moving them
# to environment variables or a local config file that is not committed.
HEADERS: dict[str, str] = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "csrf-token": "ajax:2647341132451890817",
    "priority": "u=1, i",
    "referer": "https://www.linkedin.com/sales/search/people?query=(spellCorrectionEnabled%3Atrue%2CrecentSearchParam%3A(id%3A5335895644%2CdoLogHistory%3Atrue)%2Ckeywords%3A81892450)",
    "sec-ch-prefers-color-scheme": "dark",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "x-li-identity": "dXJuOmxpOmVudGVycHJpc2VQcm9maWxlOih1cm46bGk6ZW50ZXJwcmlzZUFjY291bnQ6Mjk3MDMyNTE2LDQ5NTEwODg2Myk",
    "x-li-lang": "en_US",
    "x-li-page-instance": "urn:li:page:d_sales2_search_people;JP91MDQKQlij4Y/XK96JMw==",
    "x-li-track": '{"clientVersion":"2.0.6222","mpVersion":"2.0.6222","osName":"web","timezoneOffset":5.5,"timezone":"Asia/Calcutta","deviceFormFactor":"DESKTOP","mpName":"lighthouse-web","displayDensity":0.9375,"displayWidth":1440,"displayHeight":810}',
    "x-restli-protocol-version": "2.0.0",
    "Cookie": 'bcookie="v=2&a942cd90-17ce-4f53-8206-c5266ab14922"; bscookie="v=1&20251011043645d129c075-0994-4976-8ea0-856160b9f08dAQFFTgvbp6Or7USKd-9cs23bMbrWYJFw"; dfpfpt=1c462a2e9f2942d7b734711b4ec1ad41; li_theme=light; li_theme_set=app; li_sugr=f26f6f2b-0a9c-4210-b6c8-b6629538da2f; _guid=320662c4-9add-41e3-bb17-f078c117075e; aam_uuid=52419692327188766450617774903574288651; timezone=Asia/Calcutta; s_ips=730; li_rm=AQE6p08eUG4xXAAAAZpOK29SYW2Cts5TFLURDt9EIPnYPzGRg_4rXFF-RfKqjFpI9CIGb16xX7iN7UrmFvFssYepcv0OGqRM-Talw8T0uJ7XMC5wuJ2SZozWGW0jhB8EBEokCX7u4NIvVBxzDCwJmHd4q1ngG-jc3R2kb_L32_eX3i0KXvlkdzJGrq51bTHtbGkF6Bx3aOhJhEYjKZWL18VdGYbEGTvflkx4V2RCYh5qwAxFhJ_bw2ClLlD0x5LlE5DxJJvI4GKZa87bqWGlYv7ZTqdH3afQLetfbFztre3FTVmZ5JScnAMkvlRezEFXekLGWTITe2BdCDUKGWoqNg; visit=v=1&M; _pxvid=e70d5991-a65b-11f0-a7ef-6fed534edb6e; _uetvid=305bfb70c3b611f097d443f5e1d44715; gpv_pn=business.linkedin.com%2Fsales-solutions%2Fsales-navigator; s_tp=8272; s_tslv=1763384771221; mbox=session#34b2faf829974f88b8f7c6d976769ac0#1763386632|PC#34b2faf829974f88b8f7c6d976769ac0.41_0#1778936772; sdui_ver=sdui-flagship:0.1.22557+SduiFlagship0; _gcl_au=1.1.1682771215.1760157461.730972816.1766125993.1766125993; AnalyticsSyncHistory=AQKEW0J7hx7FgQAAAZtExTExivSchaVO5X8EdGAlW_A3vAaCSljWX9ybtm2P-_mXeU0ZUGgvCp2uQchkyA26tA; lms_ads=AQF08aqJ5C5FYgAAAZtExTMpVe78JPQqJq3Mz525YDTYXoE1rAq4OC_DM4rH6QQWP-5PqSHSRwTToGktWnuQQ4osBDeAl8hQ; lms_analytics=AQF08aqJ5C5FYgAAAZtExTMpVe78JPQqJq3Mz525YDTYXoE1rAq4OC_DM4rH6QQWP-5PqSHSRwTToGktWnuQQ4osBDeAl8hQ; AMCVS_14215E3D5995C57C0A495C55%40AdobeOrg=1; fptctx2=taBcrIH61PuCVH7eNCyH0F58uBDuZFZOunQHZt3FugltKi9c9cR%252fa3Yk2XZFm8bDfHoWSbzOvNulutV5YboXYdHcfEOZqxWXANoxC4WlUl2zxNhmy0zWGah%252f1pOlVhaGs7tRyqPefE53toP1wnK%252fqMY%252bbxsxsAh%252bgwLLXHn1k9QuP7w5b0GyBeW7Qf5TRGEXQS4ncbl%252fTYY%252bWk21RenIBZxEKAJCjNn2tjigXDldsw1E6GQbS%252f4mfMASvWdFlIaxVqTzw7z53zvi3jldEJZf9kVj8sbcnwiQydsM%252fsQh%252b4tvCyALGBNmWKmZQZ21BVmdtta1ET%252bPcnBRHwnxR3COQXhgYRp%252fHPYclSeTTOV1weU%253d; __cf_bm=u2zX1euqBRMyQrBmUNRKWU9hn7rsE6jGB2th9i.Ebb8-1766388798-1.0.1.1-39KkY5Y8qOwS2T9OACgAJzWu5.uLLUqyAxdm9VEku56w3dx.AEtMrk1LVC0Ky0bSpkfPq9F7pclXW947rIFTwt1LmNbEBcFumuipEYx9iMw; li_g_recent_logout=v=1&true; g_state={"i_l":1,"i_p":1766396016790}; fid=AQGxd84qhP1YoQAAAZtE-looBzL_R5MBhcR_whgCpXY3oGOrKn6kr7J8OjdBagV5mjZjfKX6rVxKDg; liap=true; JSESSIONID="ajax:2647341132451890817"; UserMatchHistory=AQJfKvJqPRetbgAAAZtE-poIdBWWklG2Z_gGwhLIFV0xHRLD2zIIsVIyY9pODYTcFqeYaVT0Jl9DosyGz_KA6orSvgAOOF6Jih-WXrjKw3BqyxY5_XBbGN89N_GjvZezKCs5opF6jzA7oqWyNKJf2jYT117DsoAffGsYzPJuQLu5pKBH9Fs4HLwIIuMJO8tgInYJ1oSROWWSYVm63g81rjaSUw--ksce1G_R08_mPQCsmrqqle6-nVDAnT9n_h0R40iEWQUXsnn7hBXBAKPGtSIikRWtU7VqdNcH6n2V5k14y6FbNNx511HV55Pp2w6XYVpOazRddW-74p0E5frv1Jy-yl8oGz-TX1lCJMeYvIqCp3eaSA; AMCV_14215E3D5995C57C0A495C55%40AdobeOrg=-637568504%7CMCIDTS%7C20445%7CMCMID%7C52196983684023409100602893273060649664%7CMCAAMLH-1766993635%7C12%7CMCAAMB-1766993635%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1766396035s%7CNONE%7CMCCIDH%7C-1807037927%7CvVersion%7C5.1.1; lidc="b=OB19:s=O:r=O:a=O:p=O:g=4254:u=1425:x=1:i=1766388834:t=1766457458:v=2:sig=AQGfZyFvBrMLTu4yGoMRsbRYcoR4PTcw"; PLAY_LANG=en; lang=v=2&lang=en-US; li_at=AQEFARABAAAAABm_IioAAAGbRPqQ2gAAAZtpB55qVgAAs3VybjpsaTplbnRlcnByaXNlQXV0aFRva2VuOmVKeGpaQUFDd1MzUkxpQmF0dW5RZnhBdExqdXRoUkhFcUdCK2NnTE1pTjZVZVl5QkVRQy9GQWw2XnVybjpsaTplbnRlcnByaXNlUHJvZmlsZToodXJuOmxpOmVudGVycHJpc2VBY2NvdW50OjI5NzAzMjUxNiw0OTUxMDg4NjMpXnVybjpsaTptZW1iZXI6NTEyMzkyMTE5IKqdNkbVGiF7ih-9_lMeitXrLo9EX3OiWytJkuJK-kxiAFF4y7oPIEYavhanSZkBpF3HEO4ZbgjgqZj5KsIv4HfRdkXZMtATRfZI0hskBiKMCtNlaxdXr-wBwnOWJYp6QqCPvAbzRWHKfUw1Hoxb0Yol8vj6VUnhVUdz1wx519KL1uGv1K955cqpJfU4hRBHyT-qnw; li_ep_auth_context=AHVhcHA9c2FsZXNOYXZpZ2F0b3IsYWlkPTI5NzAzMjUxNixpaWQ9Mzg3ODE1MDQ0LHBpZD00OTUxMDg4NjMsZXhwPTE3Njg5ODA4NjU2NjcsY3VyPXRydWUsc2lkPTE1Mzg0MTkxNDIsY2lkPTIwMTM1MjEwOTYB6YiX505DYoUAeMCkcLH3xTTlb5U; li_a=AQJ2PTEmc2FsZXNfY2lkPTM4NzgxNTA0NCUzQSUzQTM4NzczOTY0NCUzQSUzQXRpZXIxJTNBJTNBMjk3MDMyNTE2X5vQ70A1PjOZ6ry1O9xc1C39gYw',
}


def sanitize_company_name(raw_name: str) -> str:
    """Normalize a company name for searching.

    - Lowercase
    - Trim punctuation
    - Remove common legal suffixes when they appear at the end
    """

    name = re.sub(r"\s+", " ", (raw_name or "").strip().lower())
    name = name.strip("\t ,.")

    suffixes = (
        " ltd",
        " ltd.",
        " limited",
        " inc",
        " inc.",
        " llc",
        " corp",
        " corp.",
        " co",
        " co.",
        " ind",
    )
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip("\t ,.")
            break

    return name


def iter_companies_from_csv(path: Path) -> Iterable[str]:
    """Yield company names from the first column of a CSV file."""

    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        # skip header
        next(reader, None)
        for row in reader:
            if not row:
                continue
            name = (row[0] or "").strip()
            if name:
                yield name


def fetch_linkedin_company_data(
    company_query: str, *, session: Optional[requests.Session] = None
) -> Optional[dict[str, Any]]:
    """Fetch typeahead company data from the LinkedIn Sales Navigator API."""

    company_query = company_query.strip()
    if not company_query:
        return None

    request_params = dict(DEFAULT_PARAMS)
    request_params["query"] = company_query

    http = session or requests
    try:
        resp = http.get(
            LINKEDIN_SALES_FACET_TYPEAHEAD_URL,
            params=request_params,
            headers=HEADERS,
            timeout=20,
        )
    except requests.RequestException:
        logger.exception("LinkedIn API request errored for query=%r", company_query)
        sleep(20)
        return None

    if resp.status_code != 200:
        logger.warning(
            "LinkedIn API request failed for query=%r status=%s",
            company_query,
            resp.status_code,
        )
        sleep(20)
        return None

    return resp.json()


def iter_first_element_children(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Safely iterate the `children` of the first element in the response."""

    elements = payload.get("elements")
    if not isinstance(elements, list) or not elements:
        return []
    first = elements[0]
    if not isinstance(first, dict):
        return []
    children = first.get("children")
    if not isinstance(children, list):
        return []
    return (child for child in children if isinstance(child, dict))


def to_company_url(li_urn: str) -> Optional[str]:
    """Convert a LinkedIn entity id/URN to a public company URL."""

    if not li_urn:
        return None
    suffix = li_urn.split(":")[-1].strip()
    if not suffix:
        return None
    return f"https://www.linkedin.com/company/{suffix}"


def _find_existing_company_by_prefix(name_prefix_lc: str) -> Optional[dict[str, Any]]:
    """Return the first DB record whose name_lc begins with the given prefix."""

    if not name_prefix_lc:
        return None
    safe_prefix = re.escape(name_prefix_lc)
    cursor = db.find({"name_lc": {"$regex": f"^{safe_prefix}"}}).limit(1)
    return next(cursor, None)


def _upsert_company_record(
    *,
    existing: Optional[dict[str, Any]],
    raw_name: str,
    search_name: str,
    company_url: str,
    employee_count_range: Any,
    industry: Any,
    country: str,
    excel_source: str,
) -> None:
    try:
        doc = {
            "name": raw_name,
            "name_lc": raw_name.lower(),
            "search_name": search_name,
            "salesUrl": company_url,
            "employeeCountRange": employee_count_range,
            "industry": industry,
            "country": country,
            "excel_source": excel_source,
        }

        if existing:
            db.update_one(
                {"_id": existing["_id"]},
                {"$set": {**doc, "updatedAt": datetime.utcnow()}},
                upsert=True,
            )
        else:
            db.insert_one({**doc, "createdAt": datetime.utcnow()})

    except pymongo.errors.DuplicateKeyError:
        logger.warning("Duplicate key: %s", raw_name)


def process_csv(
    csv_path: Path,
    *,
    excel_source: str,
    start_row: int = 0,
    request_pause_every: int = 100,
    pause_seconds: int = 60,
) -> None:
    """Read companies from a CSV and enrich/insert them into MongoDB."""

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    logger.info("Processing %s (start_row=%s)", csv_path, start_row)

    with requests.Session() as session:
        processed = 0
        for idx, raw_name in enumerate(iter_companies_from_csv(csv_path), start=1):
            if idx < start_row:
                logger.debug("Skipping row %s: %s", idx, raw_name)
                continue

            try:
                search_name = sanitize_company_name(raw_name)
                existing = _find_existing_company_by_prefix(search_name)
                if existing and existing.get("industry"):
                    logger.info(
                        "Already in DB row=%s name=%r url=%s",
                        idx,
                        raw_name,
                        existing.get("salesUrl"),
                    )
                    continue

                processed += 1
                if request_pause_every and processed % request_pause_every == 0:
                    logger.info(
                        "Processed %s rows; pausing %ss...", processed, pause_seconds
                    )
                    sleep(pause_seconds)

                logger.info(
                    "Searching row=%s name=%r (query=%r)", idx, raw_name, search_name
                )

                payload = fetch_linkedin_company_data(search_name, session=session)
                if not payload:
                    continue

                for child in iter_first_element_children(payload):
                    company_url = to_company_url(child.get("id", ""))
                    if not company_url:
                        continue

                    employee_count_range = child.get("employeeCountRange", {})
                    industry = child.get("industry", {})
                    address = child.get("address", {})
                    country = (
                        address.get("country", "") if isinstance(address, dict) else ""
                    )

                    logger.info(
                        "Match row=%s name=%r url=%s employees=%s industry=%s country=%s",
                        idx,
                        raw_name,
                        company_url,
                        employee_count_range,
                        industry,
                        country,
                    )

                    _upsert_company_record(
                        existing=existing,
                        raw_name=raw_name,
                        search_name=search_name,
                        company_url=company_url,
                        employee_count_range=employee_count_range,
                        industry=industry,
                        country=country,
                        excel_source=excel_source,
                    )
                    break
            except Exception:
                logger.exception("Error processing row=%s name=%r", idx, raw_name)


def _prompt_int(
    prompt: str, *, default: Optional[int] = None, min_value: int = 0
) -> int:
    while True:
        raw = input(prompt).strip()
        if raw == "" and default is not None:
            value = default
        else:
            try:
                value = int(raw)
            except ValueError:
                print("Please enter a valid integer.")
                continue
        if value < min_value:
            print(f"Value must be >= {min_value}.")
            continue
        return value


def _list_excel_files(excels_dir: Path) -> list[str]:
    if not excels_dir.exists() or not excels_dir.is_dir():
        raise FileNotFoundError(f"Excels directory not found: {excels_dir}")
    return [f for f in os.listdir(excels_dir) if not f.startswith("~$")]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich company records using LinkedIn Sales API + MongoDB"
    )
    parser.add_argument(
        "--excels-dir", default="Excels", help="Directory containing CSV files"
    )
    parser.add_argument(
        "--excel-start", type=int, default=None, help="Start index in Excels listing"
    )
    parser.add_argument(
        "--excel-end",
        type=int,
        default=None,
        help="End index (inclusive) in Excels listing",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=None,
        help="Start row in the first CSV (1-based data row)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    excels_dir = Path(args.excels_dir)
    excel_files = _list_excel_files(excels_dir)
    if not excel_files:
        raise RuntimeError(f"No files found in {excels_dir}")

    for i, filename in enumerate(excel_files):
        print(f"[{i}] : {filename}")

    excel_start = args.excel_start
    if excel_start is None:
        excel_start = _prompt_int("Enter starting excel index: ", min_value=0)

    excel_end = args.excel_end
    if excel_end is None:
        excel_end = _prompt_int(
            f"Enter ending excel index (default {len(excel_files) - 1}): ",
            default=len(excel_files) - 1,
            min_value=excel_start,
        )

    start_row = args.start_row
    if start_row is None:
        start_row = _prompt_int(
            "Enter starting index for iterate (0 for beginning): ",
            default=0,
            min_value=0,
        )

    excel_end = min(excel_end, len(excel_files) - 1)
    for file_index in range(excel_start, excel_end + 1):
        csv_name = excel_files[file_index]
        csv_path = excels_dir / csv_name
        process_csv(csv_path, excel_source=csv_name, start_row=start_row)
        start_row = 0


if __name__ == "__main__":
    main()
