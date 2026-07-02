#!/usr/bin/env python3
import aws_cdk as cdk
from hacker_news_bronze_stack.stack import DataCollectionStack
from network_stack.stack import NetworkStack

app = cdk.App()

network_stack = NetworkStack(app, "NetworkStack")
DataCollectionStack(
    app,
    "DataCollectionStack",
)

app.synth()
