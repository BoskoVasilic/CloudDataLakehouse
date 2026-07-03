import json
import os
import logging
import urllib.request
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ssm = boto3.client("ssm")

def get_webhook_url():
    parameter_name = os.environ.get("DISCORD_WEBHOOK_PARAMETER")

    if not parameter_name:
        raise Exception("Missing DISCORD_WEBHOOK_PARAMETER env var")

    response = ssm.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )

    url = response["Parameter"]["Value"]

    return url


def format_discord_message(sns_message: dict) -> dict:
    alarm_name   = sns_message.get("AlarmName", "Unknown Alarm")
    alarm_desc   = sns_message.get("AlarmDescription", "")
    new_state    = sns_message.get("NewStateValue", "ALARM")
    reason       = sns_message.get("NewStateReason", "")
    timestamp    = sns_message.get("StateChangeTime", "")
    region       = sns_message.get("Region", "")
    account_id   = sns_message.get("AWSAccountId", "")

    color = 0xFF0000 if new_state == "ALARM" else 0x00FF00
    status_emoji = "🔴" if new_state == "ALARM" else "🟢"

    return {
        "username": "AWS ALERT",
        "avatar_url": "https://aws.amazon.com/favicon.ico",
        "embeds": [
            {
                "title": f"{status_emoji} {alarm_name}",
                "description": alarm_desc,
                "color": color,
                "fields": [
                    {
                        "name": "Status",
                        "value": new_state,
                        "inline": True,
                    },
                    {
                        "name": "Region",
                        "value": region,
                        "inline": True,
                    },
                    {
                        "name": "Account",
                        "value": account_id,
                        "inline": True,
                    },
                    {
                        "name": "Reason",
                        "value": reason[:1024] if reason else "N/A",
                        "inline": False,
                    },
                    {
                        "name": "Time",
                        "value": timestamp,
                        "inline": False,
                    },
                ],
                "footer": {
                    "text": "Data Pipeline • AWS CloudWatch"
                },
            }
        ],
    }


def send_to_discord(payload: dict) -> None:
    webhook_url = get_webhook_url()

    body = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 AWS-Lambda"
    }

    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        logger.info(f"Discord response: {response.status}")


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    for record in event.get("Records", []):
        try:
            sns_message = json.loads(record["Sns"]["Message"])
            logger.info(f"Processing alarm: {sns_message.get('AlarmName')}")

            payload = format_discord_message(sns_message)
            send_to_discord(payload)

            logger.info("Discord notification sent successfully")
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}", exc_info=True)
            raise

    return {"status": "ok"}