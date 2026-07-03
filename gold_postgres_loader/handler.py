import os
import json
import logging
import boto3
import psycopg2
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GOLD_BUCKET = os.environ["GOLD_BUCKET_NAME"]
PG_HOST     = os.environ["PG_HOST"]
PG_PORT     = os.environ.get("PG_PORT", "5432")
DB_SECRET_ARN = os.environ["DB_SECRET_ARN"]

GOLD_BASE = f"s3://{GOLD_BUCKET}/gold"

# Maps (s3_path_suffix, postgres_table_name)
TABLES = [
    ("hacker_news/daily_post_type_metric",  "hn_daily_post_type_metric"),
    ("daily_users_metric",                  "daily_users_metric"),
    ("hacker_news/top10_karma_highest",     "hn_top10_karma_highest"),
    ("hacker_news/top10_karma_lowest",      "hn_top10_karma_lowest"),
    ("hacker_news/top10_jobs_by_score",     "hn_top10_jobs_by_score"),
    ("hacker_news/top10_stories_by_score",  "hn_top10_stories_by_score"),
    ("hacker_news/data_quality_score",      "hn_data_quality_score"),
    ("twitter/daily_user_counts",           "twitter_daily_user_counts"),
    ("twitter/top10_users_by_followers",    "twitter_top10_users_by_followers"),
    ("twitter/data_quality_score",          "twitter_data_quality_score"),
]

_secrets_client = boto3.client("secretsmanager")


def get_db_credentials():
    resp = _secrets_client.get_secret_value(SecretId=DB_SECRET_ARN)
    return json.loads(resp["SecretString"])


def get_connection():
    creds = get_db_credentials()
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=10,
    )


def read_gold_parquet(s3_suffix: str) -> pd.DataFrame | None:
    path = f"{GOLD_BASE}/{s3_suffix}/"
    try:
        df = wr.s3.read_parquet(path=path, dataset=True)
        logger.info(f"Read {len(df)} rows from {path}")
        return df
    except Exception as e:
        logger.warning(f"Could not read {path}: {e}")
        return None


def write_to_postgres(df: pd.DataFrame, table: str, conn):
    """
    Creates table if not exists (based on DataFrame columns),
    then inserts rows. Uses DELETE + INSERT per date partition
    so re-runs are safe (idempotent).
    """
    cols = list(df.columns)

    # Build CREATE TABLE statement - all columns as TEXT for simplicity,
    # PostgreSQL will cast on read via Superset
    col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs});'

    with conn.cursor() as cur:
        cur.execute(create_sql)

        # If table has a "date" column, delete existing rows for those dates
        # before inserting (safe re-run / idempotency)
        if "date" in cols:
            dates = df["date"].dropna().unique().tolist()
            if dates:
                placeholders = ",".join(["%s"] * len(dates))
                cur.execute(f'DELETE FROM "{table}" WHERE "date" IN ({placeholders})', dates)
                logger.info(f"Deleted existing rows for dates {dates} in {table}")

        # Bulk insert
        rows = [tuple(str(v) if v is not None and str(v) != "<NA>" else None for v in row)
                for row in df.itertuples(index=False)]
        col_names = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(["%s"] * len(cols))
        insert_sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'
        cur.executemany(insert_sql, rows)
        logger.info(f"Inserted {len(rows)} rows into {table}")

    conn.commit()


def lambda_handler(event, context):
    logger.info("Starting gold S3 -> PostgreSQL loader")

    conn = get_connection()
    logger.info(f"Connected to PostgreSQL at {PG_HOST}:{PG_PORT}")

    results = {}

    try:
        for s3_suffix, table_name in TABLES:
            df = read_gold_parquet(s3_suffix)
            if df is None or df.empty:
                logger.warning(f"Skipping {table_name} — no data")
                results[table_name] = "skipped"
                continue

            write_to_postgres(df, table_name, conn)
            results[table_name] = f"{len(df)} rows"

    finally:
        conn.close()

    logger.info(f"Loader finished: {results}")
    return {"status": "completed", "tables": results}