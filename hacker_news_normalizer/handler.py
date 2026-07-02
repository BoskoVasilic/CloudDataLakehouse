import json
import os
import re
import logging
import uuid
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from html.parser import HTMLParser

import boto3
import pandas as pd
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BRONZE_BUCKET = os.environ["BRONZE_BUCKET_NAME"]
SILVER_BUCKET = os.environ["SILVER_BUCKET_NAME"]

SILVER_USERS_PATH = f"s3://{SILVER_BUCKET}/silver/users/"
SILVER_POSTS_PATH = f"s3://{SILVER_BUCKET}/silver/posts/"

HN_USER_API = "https://hacker-news.firebaseio.com/v0/user/{username}.json"
HN_ITEM_API = "https://hacker-news.firebaseio.com/v0/item/{item_id}.json"

MAX_WORKERS = 50


class HTMLStripper(HTMLParser):

    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed).strip()


def strip_html(text: str) -> str | None:
    if not text:
        return None
    stripper = HTMLStripper()
    stripper.feed(text)
    clean = stripper.get_data()
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean if clean else None


def epoch_to_utc_iso(epoch: int) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_user_karma(username: str) -> tuple[str, int | None, str | None]:

    url = HN_USER_API.format(username=username)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "HN-Silver-Normalizer/1.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
            if data is None:
                return username, None, None
            karma = data.get("karma")
            created_at = epoch_to_utc_iso(data.get("created"))
            return username, karma, created_at
    except Exception as e:
        logger.warning(f"Failed to fetch karma for user '{username}': {e}")
        return username, None, None


def fetch_karma_for_all_users(usernames: list[str]) -> dict[str, dict]:
    logger.info(f"Fetching karma for {len(usernames)} unique users (max_workers={MAX_WORKERS})")

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_user_karma, username): username
            for username in usernames
        }
        for future in as_completed(futures):
            username, karma, created_at = future.result()
            results[username] = {
                "karma_score": karma,
                "created_at": created_at,
            }

    fetched = sum(1 for v in results.values() if v["karma_score"] is not None)
    logger.info(f"Karma fetch complete: {fetched}/{len(usernames)} successful")
    return results


def fetch_job_score(job_id: str) -> tuple[str, int | None]:
    url = HN_ITEM_API.format(item_id=job_id)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "HN-Silver-Normalizer/1.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
            if data is None:
                return job_id, None
            return job_id, data.get("score")
    except Exception as e:
        logger.warning(f"Failed to fetch score for job_id={job_id}: {e}")
        return job_id, None


def fetch_scores_for_jobs(job_ids: list[str]) -> dict[str, int | None]:
    logger.info(f"Fetching scores for {len(job_ids)} job posts")

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_job_score, job_id): job_id
            for job_id in job_ids
        }
        for future in as_completed(futures):
            job_id, score = future.result()
            results[job_id] = score

    fetched = sum(1 for v in results.values() if v is not None)
    logger.info(f"Job score fetch: {fetched}/{len(job_ids)} successful")
    return results


def read_bronze_hits(date_str: str) -> list:
    year, month, day = date_str.split("-")
    s3_key = f"bronze/hacker_news/year={year}/month={month}/day={day}/data.json"
    s3_path = f"s3://{BRONZE_BUCKET}/{s3_key}"

    logger.info(f"Reading bronze data from: {s3_path}")

    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=BRONZE_BUCKET, Key=s3_key)
    raw = response["Body"].read().decode("utf-8")
    hits = json.loads(raw)

    logger.info(f"Loaded {len(hits)} hits from bronze")
    return hits


TAG_TO_TYPE = {
    "story":   "story",
    "ask_hn":  "ask",
    "comment": "comment",
    "job":     "job",
    "poll":    "poll",
}

PRIORITY_TAGS = ["ask_hn", "job", "poll", "comment", "story"]

def extract_post_type(hit: dict) -> str:
    tags = set(hit.get("_tags", []))
    for tag in PRIORITY_TAGS:
        if tag in tags:
            return TAG_TO_TYPE[tag]
    return "unknown"


def flatten_children(hit: dict) -> str | None:
    children = hit.get("children") or hit.get("kids") or []
    if not children:
        return None
    return json.dumps(children)


def build_dataframes(hits: list, karma_map: dict, date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    job_ids = [
        str(hit.get("objectID"))
        for hit in hits
        if "job" in hit.get("_tags", []) and hit.get("objectID")
    ]

    job_score_map = fetch_scores_for_jobs(job_ids) if job_ids else {}

    users_map = {}
    posts_rows = []

    for hit in hits:
        author = hit.get("author")
        if not author:
            continue

        if author not in users_map:
            user_karma_data = karma_map.get(author, {})
            users_map[author] = {
                "user_id": str(uuid.uuid4()),
                "username": author,
                "platform": "HackerNews",
                "karma_score": user_karma_data.get("karma_score"),
                "is_verified": None,
                "followers_count": None,
                "created_at": user_karma_data.get("created_at"),
                "first_seen_date": date_str,
            }

        post_type  = extract_post_type(hit)
        created_at = epoch_to_utc_iso(hit.get("created_at_i"))
        content    = strip_html(hit.get("text") or hit.get("story_text") or hit.get("comment_text") or hit.get("title") or hit.get("url"))
        post_id = str(hit.get("objectID") or hit.get("story_id"))

        if post_type == "job":
            points = job_score_map.get(post_id)
        else:
            points = hit.get("points")

        posts_rows.append({
            "post_id":         post_id,
            "author_username": author,
            "content_text":    content,
            "created_at":      created_at,
            "post_type":       post_type,
            "points":          points,
            "year":            date_str.split("-")[0],
            "month":           date_str.split("-")[1],
            "day":             date_str.split("-")[2],
        })

    df_users = pd.DataFrame(list(users_map.values()))
    df_posts = pd.DataFrame(posts_rows)

    logger.info(f"Built users DataFrame: {len(df_users)} rows")
    logger.info(f"Built posts DataFrame: {len(df_posts)} rows")

    return df_users, df_posts


def deduplicate(df_users: pd.DataFrame, df_posts: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    before_users = len(df_users)
    before_posts = len(df_posts)

    df_users = df_users.drop_duplicates(subset=["username"])
    df_posts = df_posts.drop_duplicates(subset=["post_id"])

    logger.info(f"Dedup users: {before_users} → {len(df_users)}")
    logger.info(f"Dedup posts: {before_posts} → {len(df_posts)}")

    return df_users, df_posts


def load_existing_users(platform: str) -> pd.DataFrame:
    path = f"{SILVER_USERS_PATH}"
    try:
        df_existing = wr.s3.read_parquet(
            path=path,
            dataset=True,
            partition_filter=lambda x: x["platform"] == platform,
        )
        logger.info(f"Loaded {len(df_existing)} existing users for platform={platform}")
        return df_existing
    except wr.exceptions.NoFilesFound:
        logger.info(f"No existing users found for platform={platform}, starting fresh")
        return pd.DataFrame()


def upsert_users(df_existing: pd.DataFrame, df_new: pd.DataFrame) -> pd.DataFrame:
    if df_existing.empty:
        logger.info("No existing users, using new users as-is")
        return df_new

    new_by_username = df_new.set_index("username")

    updated_rows = []
    for _, existing_row in df_existing.iterrows():
        username = existing_row["username"]
        if username in new_by_username.index:
            new_data = new_by_username.loc[username]
            existing_row = existing_row.copy()
            existing_row["karma_score"] = new_data["karma_score"]
            if pd.isna(existing_row["created_at"]):
                existing_row["created_at"] = new_data["created_at"]
        updated_rows.append(existing_row)

    df_updated = pd.DataFrame(updated_rows)

    existing_usernames = set(df_existing["username"])
    df_brand_new = df_new[~df_new["username"].isin(existing_usernames)]

    if not df_brand_new.empty:
        logger.info(f"Adding {len(df_brand_new)} brand new users")
        df_result = pd.concat([df_updated, df_brand_new], ignore_index=True)
    else:
        df_result = df_updated

    logger.info(
        f"Upsert complete: {len(df_existing)} existing + "
        f"{len(df_brand_new)} new = {len(df_result)} total"
    )
    return df_result


def cast_types(df_users: pd.DataFrame, df_posts: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:

    df_users["user_id"]     = df_users["user_id"].astype("string")
    df_users["username"]    = df_users["username"].astype("string")
    df_users["platform"]    = df_users["platform"].astype("string")
    df_users["karma_score"] = pd.array(df_users["karma_score"], dtype=pd.Int64Dtype())
    df_users["is_verified"] = df_users["is_verified"].astype(object)
    df_users["followers_count"] = df_users["followers_count"].astype(object)
    df_users["created_at"]  = df_users["created_at"].astype("string")
    df_users["first_seen_date"] = df_users["first_seen_date"].astype("string")

    df_posts["post_id"]         = df_posts["post_id"].astype("string")
    df_posts["author_username"] = df_posts["author_username"].astype("string")
    df_posts["content_text"]    = df_posts["content_text"].astype("string")
    df_posts["created_at"]      = df_posts["created_at"].astype("string")
    df_posts["post_type"]       = df_posts["post_type"].astype("string")
    df_posts["points"]          = pd.array(df_posts["points"], dtype=pd.Int64Dtype())
    df_posts["year"]            = df_posts["year"].astype("string")
    df_posts["month"]           = df_posts["month"].astype("string")
    df_posts["day"]             = df_posts["day"].astype("string")

    return df_users, df_posts


def save_to_silver(df_users: pd.DataFrame, df_posts: pd.DataFrame):
    if not df_users.empty:
        df_existing_users = load_existing_users("HackerNews")

        df_users_final = upsert_users(df_existing_users, df_users)

        df_users_final["karma_score"] = pd.array(
            df_users_final["karma_score"], dtype=pd.Int64Dtype()
        )
        df_users_final["platform"] = df_users_final["platform"].astype("string")

        logger.info(f"Writing {len(df_users)} users to silver parquet...")
        wr.s3.to_parquet(
            df=df_users,
            path=SILVER_USERS_PATH,
            dataset=True,
            mode="overwrite_partitions",
            partition_cols=["platform"],
        )
        logger.info("Users written successfully")

    if not df_posts.empty:
        logger.info(f"Writing {len(df_posts)} posts to silver parquet...")
        wr.s3.to_parquet(
            df=df_posts,
            path=SILVER_POSTS_PATH,
            dataset=True,
            mode="overwrite_partitions",
            partition_cols=["year", "month", "day"],
        )
        logger.info("Posts written successfully")


def lambda_handler(event, context):
    logger.info(f"Starting HN Silver Layer normalization | event={json.dumps(event)}")

    if "date" in event:
        date_str = event["date"]
    else:
        from datetime import timedelta
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        date_str = str(yesterday)

    logger.info(f"Processing date: {date_str}")

    hits = read_bronze_hits(date_str)

    if not hits:
        logger.warning(f"No hits found for {date_str}, nothing to normalize")
        return {"status": "skipped", "date": date_str, "reason": "no_data"}

    unique_usernames = list({hit["author"] for hit in hits if hit.get("author")})
    logger.info(f"Found {len(unique_usernames)} unique authors")

    karma_map = fetch_karma_for_all_users(unique_usernames)

    df_users, df_posts = build_dataframes(hits, karma_map, date_str)

    df_users, df_posts = deduplicate(df_users, df_posts)

    df_users, df_posts = cast_types(df_users, df_posts)

    save_to_silver(df_users, df_posts)

    result = {
        "status": "completed",
        "date": date_str,
        "users_written": len(df_users),
        "posts_written": len(df_posts),
        "karma_fetched": sum(1 for v in karma_map.values() if v["karma_score"] is not None),
    }

    logger.info(f"Silver normalization finished: {json.dumps(result)}")
    return result