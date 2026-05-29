import json
import os
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ["BRONZE_BUCKET_NAME"]
HN_SEARCH_BASE = "https://hn.algolia.com/api/v1/search"
HN_TAGS = "(story,ask_hn,comment,job,poll)"

ALGOLIA_MAX_HITS = 1000
MIN_INTERVAL_SECONDS = 1800
INITIAL_INTERVAL_HOURS = 3


def fetch_hn(ts_from: int, ts_to: int) -> dict:
    params = urllib.parse.urlencode({
        "tags": HN_TAGS,
        "numericFilters": f"created_at_i>={ts_from},created_at_i<{ts_to}",
        "hitsPerPage": ALGOLIA_MAX_HITS,
        "page": 0,
    })
    url = f"{HN_SEARCH_BASE}?{params}"
    logger.info(f"Fetching: {url}")

    req = urllib.request.Request(url, headers={"User-Agent": "HN-Bronze-Collector/1.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def collect_interval(ts_from: int, ts_to: int, all_hits: list) -> None:
    response = fetch_hn(ts_from, ts_to)
    hits = response.get("hits", [])

    interval_size = ts_to - ts_from

    if len(hits) == ALGOLIA_MAX_HITS and interval_size > MIN_INTERVAL_SECONDS:
        mid = ts_from + interval_size // 2
        collect_interval(ts_from, mid, all_hits)
        collect_interval(mid, ts_to, all_hits)
    else:
        all_hits.extend(hits)

def save_to_s3(hits: list, date_str: str) -> str:
    s3_client = boto3.client("s3")

    year, month, day = date_str.split("-")
    s3_key = f"bronze/hacker_news/year={year}/month={month}/day={day}/data.json"

    body = json.dumps(hits, ensure_ascii=False)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "source": "hacker_news",
            "collection_date": date_str,
            "hit_count": str(len(hits)),
        },
    )

    logger.info(f"Saved {len(hits)} hits to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def lambda_handler(event, context):
    logger.info("Starting Hacker News Bronze Layer collection")

    today_utc = datetime.now(timezone.utc).date()
    yesterday = today_utc - timedelta(days=1)
    date_str = str(yesterday)

    day_start = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, tzinfo=timezone.utc)
    day_end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=timezone.utc)

    interval_seconds = INITIAL_INTERVAL_HOURS * 3600
    all_hits = []

    ts = int(day_start.timestamp())
    day_end_ts = int(day_end.timestamp())

    while ts < day_end_ts:
        ts_to = min(ts + interval_seconds, day_end_ts)
        collect_interval(ts, ts_to, all_hits)
        ts = ts_to

    s3_key = save_to_s3(all_hits, date_str)

    result = {
        "status": "completed",
        "collection_date": date_str,
        "total_hits": len(all_hits),
        "s3_key": s3_key,
    }

    logger.info(f"Collection finished: {json.dumps(result)}")
    return result