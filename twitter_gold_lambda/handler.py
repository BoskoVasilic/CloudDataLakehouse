import os
import logging
import pandas as pd
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ["BRONZE_BUCKET_NAME"]
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold/twitter"

PATH_USERS = f"s3://{S3_BUCKET}/{SILVER_PREFIX}/users/"
PATH_POSTS = f"s3://{S3_BUCKET}/{SILVER_PREFIX}/posts/"
PATH_GOLD  = f"s3://{S3_BUCKET}/{GOLD_PREFIX}"

BRONZE_TOTAL_ROWS      = 4_693_091
SILVER_POSTS           = 4_689_288
SILVER_USERS           = 655_836
REMOVED_NULL_ROWS      = 3_737
REMOVED_PRE_2006       = 1
REMOVED_DUPLICATES     = 65
USERS_NULL_CREATED     = 3_040
USERS_PRE_2006_CREATED = 72
USERS_SENTINEL_1970    = 7

TWITTER_YEARS = ["2021", "2022", "2023"]


def save(df: pd.DataFrame, name: str):
    path = f"{PATH_GOLD}/{name}/"
    logger.info(f"Upisujem {name} u {path}...")
    wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite")
    logger.info(f"{name} upisan")


def calc_daily_user_counts(users_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Racunam daily_user_counts...")
    df = users_df.copy()
    df["date"] = pd.to_datetime(df["created_at"], errors="coerce").dt.date.astype(str)
    df = df[df["date"].notna() & (df["date"] != "NaT") & (df["date"] != "None")]
    daily = df.groupby("date").size().reset_index(name="new_users_count")
    daily["platform"] = "X"
    daily = daily[["date", "platform", "new_users_count"]].sort_values("date")
    logger.info(f"daily_user_counts: {len(daily)} redova")
    return daily


def calc_top10_by_followers(users_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Racunam top10_users_by_followers...")
    df = users_df[users_df["followers_count"].notna()].copy()
    df = df.sort_values("followers_count", ascending=False).head(10).reset_index(drop=True)
    df["rank"] = df.index + 1
    df = df[["rank", "username", "followers_count", "is_verified"]]
    logger.info(f"top10_users_by_followers: {len(df)} redova")
    return df


def calc_data_quality_score(users_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Racunam data_quality_score...")
    rows = []

    rows.append({
        "metric_name": "pipeline_bronze_total",
        "total_rows": BRONZE_TOTAL_ROWS,
        "valid_rows": SILVER_POSTS,
        "quality_pct": round(SILVER_POSTS / BRONZE_TOTAL_ROWS * 100, 2),
    })
    rows.append({
        "metric_name": "pipeline_removed_null",
        "total_rows": BRONZE_TOTAL_ROWS,
        "valid_rows": BRONZE_TOTAL_ROWS - REMOVED_NULL_ROWS,
        "quality_pct": round((BRONZE_TOTAL_ROWS - REMOVED_NULL_ROWS) / BRONZE_TOTAL_ROWS * 100, 2),
    })
    rows.append({
        "metric_name": "pipeline_removed_duplicates",
        "total_rows": BRONZE_TOTAL_ROWS - REMOVED_NULL_ROWS - REMOVED_PRE_2006,
        "valid_rows": SILVER_POSTS,
        "quality_pct": round(SILVER_POSTS / (BRONZE_TOTAL_ROWS - REMOVED_NULL_ROWS - REMOVED_PRE_2006) * 100, 2),
    })

    for col in ["created_at", "followers_count", "is_verified"]:
        total = len(users_df)
        valid = int(users_df[col].notna().sum())
        rows.append({
            "metric_name": f"users_{col}",
            "total_rows": total,
            "valid_rows": valid,
            "quality_pct": round(valid / total * 100, 2) if total > 0 else 0.0,
        })

    rows.append({
        "metric_name": "posts_total",
        "total_rows": BRONZE_TOTAL_ROWS,
        "valid_rows": SILVER_POSTS,
        "quality_pct": round(SILVER_POSTS / BRONZE_TOTAL_ROWS * 100, 2),
    })

    dq_df = pd.DataFrame(rows)
    logger.info(f"data_quality_score: {len(dq_df)} metrika")
    return dq_df


def lambda_handler(event, context):
    logger.info("Pokrenut Twitter Gold calculator")

    logger.info("Ucitavam silver users (platform=X)...")
    users_df = wr.s3.read_parquet(
        path=PATH_USERS,
        dataset=True,
        partition_filter=lambda x: x["platform"] == "X",
    )
    logger.info(f"Ucitano {len(users_df)} korisnika")

    daily_df = calc_daily_user_counts(users_df)
    top10_df = calc_top10_by_followers(users_df)
    dq_df    = calc_data_quality_score(users_df)

    save(daily_df, "daily_user_counts")
    save(top10_df, "top10_users_by_followers")
    save(dq_df,    "data_quality_score")

    result = {
        "status": "completed",
        "daily_user_counts_rows": len(daily_df),
        "top10_rows": len(top10_df),
        "data_quality_metrics": len(dq_df),
    }
    logger.info(f"Gotovo: {result}")
    return result