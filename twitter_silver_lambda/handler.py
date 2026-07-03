import os
import re
import uuid
import logging

import boto3
import numpy as np
import pandas as pd
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ["BRONZE_BUCKET_NAME"]
SILVER_PREFIX = "silver"
BRONZE_KEY = "bronze/twitter/source/Bitcoin_tweets.csv"
CHUNK_SIZE = 50_000
TMP_CSV = "/tmp/bitcoin_tweets.csv"

HTML_TAG_RE = re.compile(r"<[^>]+>")
EXCEL_EPOCH = pd.Timestamp("1899-12-30", tz="UTC")

PATH_USERS = f"s3://{S3_BUCKET}/{SILVER_PREFIX}/users/"
PATH_POSTS = f"s3://{S3_BUCKET}/{SILVER_PREFIX}/posts/"

TWITTER_YEARS = ["2021", "2022", "2023"]

seen_usernames: set = set()


def parse_date_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    has_numeric = numeric.notna()
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
    if has_numeric.any():
        result[has_numeric] = EXCEL_EPOCH + pd.to_timedelta(numeric[has_numeric], unit="D")
    is_string = ~has_numeric & series.notna()
    if is_string.any():
        result[is_string] = pd.to_datetime(series[is_string], utc=True, errors="coerce")
    return result


def safe_year(dt_series: pd.Series) -> pd.Series:
    return dt_series.dt.year.apply(lambda x: str(int(x)) if pd.notna(x) else None)


def safe_month(dt_series: pd.Series) -> pd.Series:
    return dt_series.dt.month.apply(lambda x: f"{int(x):02d}" if pd.notna(x) else None)


def safe_day(dt_series: pd.Series) -> pd.Series:
    return dt_series.dt.day.apply(lambda x: f"{int(x):02d}" if pd.notna(x) else None)


def process_chunk(chunk: pd.DataFrame):
    global seen_usernames

    # --- USERS ---
    users = (
        chunk[["user_name", "user_created", "user_verified", "user_followers"]]
        .drop_duplicates(subset=["user_name"])
        .copy()
    )

    users = users[~users["user_name"].isin(seen_usernames)]

    if not users.empty:
        seen_usernames.update(users["user_name"].tolist())

        created_dt = parse_date_series(users["user_created"])
        valid_created = created_dt.apply(
            lambda x: x if pd.notna(x) and x.year >= 2006 else pd.NaT
        )

        users["created_at"] = valid_created.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        users = users.rename(columns={
            "user_name": "username",
            "user_verified": "is_verified",
            "user_followers": "followers_count",
        })
        users["is_verified"] = (
            users["is_verified"].astype(str).str.strip().str.upper()
            .map({"TRUE": True, "FALSE": False})
        )
        users["followers_count"] = pd.to_numeric(users["followers_count"], errors="coerce").astype("Int64")
        users["user_id"] = [str(uuid.uuid4()) for _ in range(len(users))]
        users["platform"] = "X"
        users["karma_score"] = pd.array([None] * len(users), dtype=pd.Int64Dtype())
        # X nema koncept "first seen" razlicit od registracije - koristimo created_at
        users["first_seen_date"] = users["created_at"]

        users = users[[
            "user_id", "username", "platform", "karma_score",
            "is_verified", "followers_count", "created_at", "first_seen_date"
        ]]
    else:
        users = None

    # --- POSTS ---
    posts = chunk[["user_name", "date", "text", "is_retweet"]].copy()
    created_at_dt = parse_date_series(posts["date"])

    posts["created_at"] = created_at_dt.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    posts["year"]  = safe_year(created_at_dt)
    posts["month"] = safe_month(created_at_dt)
    posts["day"]   = safe_day(created_at_dt)

    posts = posts[posts["year"].apply(lambda x: x is not None and int(x) >= 2006)]

    posts["post_type"] = np.where(
        posts["is_retweet"].astype(str).str.strip().str.upper() == "TRUE", "retweet", "tweet"
    )
    posts["content_text"] = posts["text"].astype(str).str.replace(HTML_TAG_RE, "", regex=True).str.strip()
    posts["post_id"] = (
        posts["user_name"].astype(str) + "_" + posts["created_at"].astype(str)
    ).apply(hash).abs().astype(str)
    posts = posts.rename(columns={"user_name": "author_username"})
    posts["points"] = pd.array([None] * len(posts), dtype=pd.Int64Dtype())

    posts = posts.drop_duplicates(subset=["author_username", "content_text", "created_at"])
    posts = posts[[
        "post_id", "author_username", "content_text",
        "created_at", "post_type", "points", "year", "month", "day"
    ]]

    return users, posts


def lambda_handler(event, context):
    global seen_usernames
    seen_usernames = set()

    logger.info("Pokrenut Twitter Silver normalizer")

    raise Exception("TEST - namerni crash za proveru alarma")

    
    logger.info("Brisem stare X partition-e (users platform=X, posts year=2021/2022/2023)...")
    wr.s3.delete_objects(f"{PATH_USERS}platform=X/")
    for y in TWITTER_YEARS:
        wr.s3.delete_objects(f"{PATH_POSTS}year={y}/")

    logger.info(f"Preuzimam s3://{S3_BUCKET}/{BRONZE_KEY} na {TMP_CSV}")
    boto3.client("s3").download_file(S3_BUCKET, BRONZE_KEY, TMP_CSV)
    logger.info(f"Preuzeto {os.path.getsize(TMP_CSV)/1e6:.1f} MB")

    total_rows = 0
    total_users = 0
    total_posts = 0

    csv_reader = pd.read_csv(
        TMP_CSV,
        chunksize=CHUNK_SIZE,
        on_bad_lines="skip",
        engine="python",
        encoding="utf-8",
        encoding_errors="replace",
    )

    for i, chunk in enumerate(csv_reader):
        total_rows += len(chunk)
        users_chunk, posts_chunk = process_chunk(chunk)

        if users_chunk is not None and not users_chunk.empty:
            wr.s3.to_parquet(
                df=users_chunk, path=PATH_USERS, dataset=True,
                partition_cols=["platform"], mode="append",
            )
            total_users += len(users_chunk)

        if not posts_chunk.empty:
            wr.s3.to_parquet(
                df=posts_chunk, path=PATH_POSTS, dataset=True,
                partition_cols=["year", "month", "day"], mode="append",
            )
            total_posts += len(posts_chunk)

        logger.info(f"Chunk {i+1} | redova: {total_rows} | users: {total_users} | posts: {total_posts}")
        del users_chunk, posts_chunk, chunk

    os.remove(TMP_CSV)

    result = {
        "status": "completed",
        "total_rows_processed": total_rows,
        "unique_users_written": total_users,
        "posts_written": total_posts,
    }
    logger.info(f"Gotovo: {result}")
    return result