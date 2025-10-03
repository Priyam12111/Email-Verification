import socket
from zoneinfo import ZoneInfo
from datetime import datetime, timezone
from configs.db import pattern_stats

IST = ZoneInfo("Asia/Kolkata")
PC_NAME = socket.gethostname()

def now_ist():
    return datetime.now(tz=IST)

def ist_date_bucket():
    # daily bucket in IST (00:00–23:59 IST)
    return now_ist().strftime("%Y-%m-%d")

def _stats_key():
    return {"date_ist": ist_date_bucket(), "pc": PC_NAME}

def inc_stats(*, queries: int = 0, valids: int = 0):
    # upsert & bump counts
    pattern_stats.update_one(
        _stats_key(),
        {
            "$setOnInsert": {
                "createdAt_utc": datetime.now(timezone.utc),
                "date_ist": ist_date_bucket(),
                "pc": PC_NAME,
            },
            "$set": {"updatedAt_utc": datetime.now(timezone.utc)},
            "$inc": {"queries": queries, "valids": valids},
        },
        upsert=True,
    )