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


TWITTER_YEARS = ["2021", "2022", "2023"]
TWITTER_POST_TYPES = ["tweet", "retweet"]


USERS_QUALITY_COLUMNS = ["username", "platform", "is_verified", "followers_count", "created_at"]
POSTS_QUALITY_COLUMNS = ["post_id", "author_username", "content_text", "created_at", "post_type"]


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

    daily = df.groupby("date").size().reset_index(name="new_users")
    daily = daily.sort_values("date").reset_index(drop=True)
    daily["total_users"] = daily["new_users"].cumsum()
    daily["platform"] = "X"

    daily = daily[["date", "platform", "total_users", "new_users"]]
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


def _column_completeness_rows(df: pd.DataFrame, columns: list, table_label: str) -> list:
    """Za svaku kolonu racuna % ne-null vrednosti direktno iz podataka (bez konstanti)."""
    rows = []
    total = len(df)
    for col in columns:
        valid = int(df[col].notna().sum()) if col in df.columns else 0
        rows.append({
            "metric_name": f"{table_label}_{col}",
            "total_rows": total,
            "valid_rows": valid,
            "quality_pct": round(valid / total * 100, 2) if total > 0 else 0.0,
        })
    return rows


def calc_posts_quality_rows(bucket: str, years: list, post_types: list) -> tuple[list, int]:

    logger.info("Racunam posts kvalitet (chunked, bez punog ucitavanja u memoriju)...")
    path = f"s3://{bucket}/{SILVER_PREFIX}/posts/"

    total = 0
    overall_valid = 0
    valid_counts = {col: 0 for col in POSTS_QUALITY_COLUMNS}

    chunks = wr.s3.read_parquet(
        path=path,
        dataset=True,
        partition_filter=lambda x: x["year"] in years,
        columns=POSTS_QUALITY_COLUMNS,
        chunked=True,  
    )

    for chunk in chunks:
        chunk = chunk[chunk["post_type"].isin(post_types)]
        if chunk.empty:
            continue
        total += len(chunk)
        for col in POSTS_QUALITY_COLUMNS:
            valid_counts[col] += int(chunk[col].notna().sum())
        overall_valid += int(chunk[POSTS_QUALITY_COLUMNS].notna().all(axis=1).sum())
        del chunk

    rows = []
    for col in POSTS_QUALITY_COLUMNS:
        valid = valid_counts[col]
        rows.append({
            "metric_name": f"posts_{col}",
            "total_rows": total,
            "valid_rows": valid,
            "quality_pct": round(valid / total * 100, 2) if total > 0 else 0.0,
        })
    rows.append({
        "metric_name": "posts_overall",
        "total_rows": total,
        "valid_rows": overall_valid,
        "quality_pct": round(overall_valid / total * 100, 2) if total > 0 else 0.0,
    })

    logger.info(f"Posts kvalitet izracunat na {total} X postova")
    return rows, total


def calc_data_quality_score(users_df: pd.DataFrame, posts_quality_rows: list) -> pd.DataFrame:
    """
    Data Quality Score = procenat redova u tabelama koji nisu null,
    racunato ISKLJUCIVO iz stvarnih silver podataka koje lambda ucita
    (bez ijedne hardkodovane konstante). Posts deo se racuna chunked
    (vidi calc_posts_quality_rows) da se izbegne OOM na content_text.
    """
    logger.info("Racunam data_quality_score...")
    rows = []

    rows += _column_completeness_rows(users_df, USERS_QUALITY_COLUMNS, "users")

    users_total = len(users_df)
    users_valid = int(users_df[USERS_QUALITY_COLUMNS].notna().all(axis=1).sum()) if users_total > 0 else 0
    rows.append({
        "metric_name": "users_overall",
        "total_rows": users_total,
        "valid_rows": users_valid,
        "quality_pct": round(users_valid / users_total * 100, 2) if users_total > 0 else 0.0,
    })

    rows += posts_quality_rows

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

  
    posts_quality_rows, posts_total = calc_posts_quality_rows(
        S3_BUCKET, TWITTER_YEARS, TWITTER_POST_TYPES
    )
    logger.info(f"Ucitano {posts_total} X postova (chunked)")

 
    dq_df = calc_data_quality_score(users_df, posts_quality_rows)
    del users_df

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