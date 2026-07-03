import json
import os
import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SILVER_BUCKET = os.environ["SILVER_BUCKET_NAME"]
GOLD_BUCKET = os.environ["GOLD_BUCKET_NAME"]

SILVER_USERS_PATH = f"s3://{SILVER_BUCKET}/silver/users/"
SILVER_POSTS_PATH = f"s3://{SILVER_BUCKET}/silver/posts/"
GOLD_PATH = f"s3://{GOLD_BUCKET}/gold"
GOLD_DQ_PATH = f"s3://{SILVER_BUCKET}/gold/hacker_news/data_quality_score/"


def read_silver(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    year, month, day = date_str.split("-")

    logger.info(f"Reading silver users for platform=HackerNews")
    df_users = wr.s3.read_parquet(
        path=SILVER_USERS_PATH,
        dataset=True,
        partition_filter=lambda x: x["platform"] == "HackerNews",
    )

    logger.info(f"Reading silver posts for {date_str}")
    df_posts = wr.s3.read_parquet(
        path=SILVER_POSTS_PATH,
        dataset=True,
        partition_filter=lambda x: (
                x["year"] == year and
                x["month"] == month and
                x["day"] == day
        ),
    )

    logger.info(f"Silver read: {len(df_users)} users, {len(df_posts)} posts")
    return df_users, df_posts


def save_metric(df: pd.DataFrame, metric_name: str, partition_cols: list[str]):
    if df.empty:
        logger.warning(f"Skipping empty DataFrame for metric: {metric_name}")
        return

    path = f"{GOLD_PATH}{metric_name}/"
    logger.info(f"Writing {len(df)} rows to {path}")

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=partition_cols,
    )
    logger.info(f"Written: {metric_name}")


def compute_daily_post_type_metric(df_posts: pd.DataFrame, date_str: str) -> pd.DataFrame:
    counts = (
        df_posts
        .groupby("post_type", dropna=True)
        .size()
        .reset_index(name="post_count")
    )
    counts["date"] = date_str
    counts["post_count"] = pd.array(counts["post_count"], dtype=pd.Int64Dtype())

    logger.info(f"Post type counts:\n{counts.to_string(index=False)}")
    return counts[["date", "post_type", "post_count"]]


def compute_daily_users_metric(df_users: pd.DataFrame, df_posts: pd.DataFrame, date_str: str) -> pd.DataFrame:
    active_usernames = set(df_posts["author_username"].dropna().unique())
    total_users = len(df_users)

    new_users = int(
        df_users[df_users["first_seen_date"] == date_str]
        ["username"]
        .isin(active_usernames)
        .sum()
    )

    df_metric = pd.DataFrame([{
        "date": date_str,
        "platform": "HackerNews",
        "total_users": pd.array([total_users], dtype=pd.Int64Dtype())[0],
        "new_users": pd.array([new_users], dtype=pd.Int64Dtype())[0],
    }])

    logger.info(f"Daily users: total_active={total_users}, new={new_users}")
    return df_metric[["date", "platform", "total_users", "new_users"]]


def compute_top10_karma(df_users: pd.DataFrame, date_str: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_with_karma = (
        df_users[["username", "karma_score"]]
        .dropna(subset=["karma_score"])
        .drop_duplicates(subset=["username"])
    )

    def build_top10(df_sorted: pd.DataFrame) -> pd.DataFrame:
        df_top = df_sorted.head(10).copy().reset_index(drop=True)
        df_top["rank"] = range(1, len(df_top) + 1)
        df_top["date"] = date_str
        df_top["karma_score"] = pd.array(df_top["karma_score"], dtype=pd.Int64Dtype())
        df_top["rank"] = pd.array(df_top["rank"], dtype=pd.Int64Dtype())
        return df_top[["date", "rank", "username", "karma_score"]]

    df_highest = build_top10(df_with_karma.sort_values("karma_score", ascending=False))
    df_lowest = build_top10(df_with_karma.sort_values("karma_score", ascending=True))

    logger.info(f"Top karma highest: {df_highest['username'].tolist()}")
    logger.info(f"Top karma lowest:  {df_lowest['username'].tolist()}")

    return df_highest, df_lowest


def compute_top10_jobs(df_posts: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df_jobs = (
        df_posts[df_posts["post_type"] == "job"]
        [["post_id", "author_username", "content_text", "points"]]
        .dropna(subset=["points"])
        .sort_values("points", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    df_jobs["rank"] = range(1, len(df_jobs) + 1)
    df_jobs["date"] = date_str
    df_jobs["points"] = pd.array(df_jobs["points"], dtype=pd.Int64Dtype())
    df_jobs["rank"] = pd.array(df_jobs["rank"], dtype=pd.Int64Dtype())

    logger.info(f"Top jobs count: {len(df_jobs)}")
    return df_jobs[["date", "rank", "post_id", "author_username", "content_text", "points"]]


def compute_top10_stories(df_posts: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df_stories = (
        df_posts[df_posts["post_type"] == "story"]
        [["post_id", "author_username", "content_text", "points"]]
        .dropna(subset=["points"])
        .sort_values("points", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    df_stories["rank"] = range(1, len(df_stories) + 1)
    df_stories["date"] = date_str
    df_stories["points"] = pd.array(df_stories["points"], dtype=pd.Int64Dtype())
    df_stories["rank"] = pd.array(df_stories["rank"], dtype=pd.Int64Dtype())

    logger.info(f"Top stories count: {len(df_stories)}")
    return df_stories[["date", "rank", "post_id", "author_username", "content_text", "points"]]


USERS_KEY_COLS = ["user_id", "username", "platform", "karma_score", "created_at"]
POSTS_KEY_COLS = ["post_id", "author_username", "content_text", "created_at", "post_type", "points"]


def save_data_quality_score(df_users: pd.DataFrame, df_posts: pd.DataFrame, date_str: str):
    dq_rows = []

    for col in ["created_at", "content_text", "post_type", "author_username", "points", "post_id"]:
        total = len(df_posts)
        valid = int(df_posts[col].notna().sum())
        dq_rows.append({
            "date": date_str,
            "metric_name": f"posts_{col}",
            "total_rows": total,
            "valid_rows": valid,
            "quality_pct": round(valid / total * 100, 2) if total > 0 else 0.0,
        })

    for col in ["karma_score", "created_at", "username"]:
        total = len(df_users)
        valid = int(df_users[col].notna().sum())
        dq_rows.append({
            "date": date_str,
            "metric_name": f"users_{col}",
            "total_rows": total,
            "valid_rows": valid,
            "quality_pct": round(valid / total * 100, 2) if total > 0 else 0.0,
        })

    dq_df = pd.DataFrame(dq_rows)
    wr.s3.to_parquet(
        df=dq_df,
        path=GOLD_DQ_PATH,
        dataset=True,
        partition_cols=["date"],
        mode="overwrite_partitions",
    )
    logger.info(f"Data Quality Score written for {date_str}: {len(dq_df)} metrics")


def lambda_handler(event, context):
    logger.info(f"Starting HN Gold Layer transformation | event={json.dumps(event)}")

    if "date" in event:
        date_str = event["date"]
    else:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        date_str = str(yesterday)

    logger.info(f"Processing date: {date_str}")

    df_users, df_posts = read_silver(date_str)

    if df_posts.empty:
        logger.warning(f"No posts found for {date_str}")
        return {"status": "skipped", "date": date_str, "reason": "no_data"}

    df_post_types = compute_daily_post_type_metric(df_posts, date_str)
    df_daily_users = compute_daily_users_metric(df_users, df_posts, date_str)
    df_karma_highest, \
        df_karma_lowest = compute_top10_karma(df_users, date_str)
    df_top_jobs = compute_top10_jobs(df_posts, date_str)
    df_top_stories = compute_top10_stories(df_posts, date_str)
    save_data_quality_score(df_users, df_posts, date_str)

    save_metric(df_post_types, "/hacker_news/daily_post_type_metric", ["date"])
    save_metric(df_daily_users, "/daily_users_metric", ["platform", "date"])
    save_metric(df_karma_highest, "/hacker_news/top10_karma_highest", ["date"])
    save_metric(df_karma_lowest, "/hacker_news/top10_karma_lowest", ["date"])
    save_metric(df_top_jobs, "/hacker_news/top10_jobs_by_score", ["date"])
    save_metric(df_top_stories, "/hacker_news/top10_stories_by_score", ["date"])

    result = {
        "status": "completed",
        "date": date_str,
        "metrics": {
            "post_types": len(df_post_types),
            "daily_users": len(df_daily_users),
            "karma_highest": len(df_karma_highest),
            "karma_lowest": len(df_karma_lowest),
            "top_jobs": len(df_top_jobs),
            "top_stories": len(df_top_stories),
        }
    }

    logger.info(f"Gold transformation finished: {json.dumps(result)}")
    return result