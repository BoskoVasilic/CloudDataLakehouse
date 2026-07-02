#!/usr/bin/env python3
import aws_cdk as cdk
from hacker_news_bronze_stack.stack import DataCollectionStack
from network_stack.stack import NetworkStack
from twitter_silver_stack.stack import TwitterSilverStack
from twitter_gold_stack.stack import TwitterGoldStack

app = cdk.App()

network_stack = NetworkStack(app, "NetworkStack")

data_collection_stack = DataCollectionStack(
    app,
    "DataCollectionStack",
)

TwitterSilverStack(
    app,
    "TwitterSilverStack",
    bronze_bucket_name=f"data-lake-bucket-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}",
)

TwitterGoldStack(
    app,
    "TwitterGoldStack",
    bronze_bucket_name=f"data-lake-bucket-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}",
)


app.synth()