import subprocess
import boto3


def get_default_bucket_name() -> str:
    session = boto3.session.Session()
    account = boto3.client("sts").get_caller_identity()["Account"]
    region = session.region_name
    return f"data-lake-bucket-{account}-{region}"


def cdk_deploy():
    print("Pokrecam cdk deploy...")
    subprocess.run(["cdk.cmd", "deploy", "--require-approval", "never"], check=True)
    print("CDK deploy uspesno zavrsen.")


def upload_csv(csv_path: str, bucket: str, s3_key: str):
    print(f"Uploadujem {csv_path} -> s3://{bucket}/{s3_key}")
    s3 = boto3.client("s3")
    s3.upload_file(csv_path, bucket, s3_key)
    print("CSV uspesno uploadovan.")


if __name__ == "__main__":
    cdk_deploy()

    default_bucket = get_default_bucket_name()

    csv_path = input("Putanja do CSV fajla [Bitcoin_tweets.csv]: ").strip() or "Bitcoin_tweets.csv"
    bucket = input(f"Ime bucketa [{default_bucket}]: ").strip() or default_bucket
    s3_key = input("S3 putanja [bronze/twitter/source/Bitcoin_tweets.csv]: ").strip() or "bronze/twitter/source/Bitcoin_tweets.csv"

    upload_csv(csv_path, bucket, s3_key)
    print("Gotovo! Twitter bronze layer je spreman.")