#!/usr/bin/env python3
import aws_cdk as cdk
from hacker_news_bronze_stack.stack import DataCollectionStack

app = cdk.App()

DataCollectionStack(
    app,
    "DataCollectionStack",
)

app.synth()
