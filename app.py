#!/usr/bin/env python3
import aws_cdk as cdk
from hacker_news_bronze_stack.stack import DataCollectionStack
from twitter_silver_stack.stack import TwitterSilverStack

app = cdk.App()

data_collection_stack = DataCollectionStack(
    app,
    "DataCollectionStack",
)

TwitterSilverStack(
    app,
    "TwitterSilverStack",
    bronze_bucket_name=f"data-lake-bucket-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}",
)

app.synth()