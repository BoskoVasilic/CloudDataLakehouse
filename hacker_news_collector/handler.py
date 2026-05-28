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

ITEM_TYPES = ["story", "ask_hn", "comment", "job", "poll"]


def get_date_range_timestamps():
    today_utc = datetime.now(timezone.utc).date()
    yesterday = today_utc - timedelta(days=1)

    start_dt = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, tzinfo=timezone.utc)

    return int(start_dt.timestamp()), int(end_dt.timestamp()), yesterday


def fetch_hn_page(item_type: str, timestamp_from: int, timestamp_to: int, page: int) -> dict:
    params = urllib.parse.urlencode({
        "tags": item_type,
        "numericFilters": f"created_at_i>{timestamp_from},created_at_i<{timestamp_to}",
        "hitsPerPage": 1000,
        "page": page,
    })
    url = f"{HN_SEARCH_BASE}?{params}"
    logger.info(f"Fetching: {url}")

    req = urllib.request.Request(url, headers={"User-Agent": "HN-Bronze-Collector/1.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def collect_all_items_for_type(item_type: str, ts_from: int, ts_to: int) -> list:
    all_items = []
    page = 0

    while True:
        data = fetch_hn_page(item_type, ts_from, ts_to, page)
        hits = data.get("hits", [])
        all_items.extend(hits)

        logger.info(
            f"Type={item_type} | Page={page} | Hits on page={len(hits)} | "
            f"Total so far={len(all_items)} | NbPages={data.get('nbPages', 0)}"
        )

        if page >= data.get("nbPages", 1) - 1:
            break
        page += 1

    return all_items


def save_to_s3(items: list, item_type: str, date_str: str) -> str:
    s3_client = boto3.client("s3")

    year, month, day = date_str.split("-")
    s3_key = (
        f"bronze/hacker_news/{item_type}/"
        f"year={year}/month={month}/day={day}/data.json"
    )

    body = "\n".join(json.dumps(item, ensure_ascii=False) for item in items)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "source": "hacker_news",
            "item_type": item_type,
            "collection_date": date_str,
            "item_count": str(len(items)),
        },
    )

    logger.info(f"Saved {len(items)} items to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


def lambda_handler(event, context):
    logger.info("Starting Hacker News Bronze Layer collection")

    ts_from, ts_to, yesterday = get_date_range_timestamps()
    date_str = str(yesterday)  # format: YYYY-MM-DD
    logger.info(f"Collecting data for date: {date_str} | ts_from={ts_from} | ts_to={ts_to}")

    summary = {}
    errors = []

    for item_type in ITEM_TYPES:
        try:
            items = collect_all_items_for_type(item_type, ts_from, ts_to)
            if items:
                s3_key = save_to_s3(items, item_type, date_str)
                summary[item_type] = {"count": len(items), "s3_key": s3_key}
            else:
                logger.info(f"No items found for type={item_type} on {date_str}")
                summary[item_type] = {"count": 0, "s3_key": None}
        except Exception as e:
            logger.error(f"Error collecting type={item_type}: {str(e)}", exc_info=True)
            errors.append({"item_type": item_type, "error": str(e)})

    result = {
        "status": "completed" if not errors else "completed_with_errors",
        "collection_date": date_str,
        "summary": summary,
        "errors": errors,
    }

    logger.info(f"Collection finished: {json.dumps(result)}")

    if errors:
        raise RuntimeError(f"Errors during collection: {json.dumps(errors)}")

    return result
