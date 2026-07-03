#!/usr/bin/env python3
import os
import aws_cdk as cdk
from hacker_news_bronze_stack.stack import DataCollectionStack
from network_stack.stack import NetworkStack
from twitter_silver_stack.stack import TwitterSilverStack
from twitter_gold_stack.stack import TwitterGoldStack
from ec2_stack.stack import Ec2Stack

app = cdk.App()

env = cdk.Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region="eu-north-1",
)

network_stack = NetworkStack(app, "NetworkStack", env=env)

data_collection_stack = DataCollectionStack(
    app,
    "DataCollectionStack",
    vpc=network_stack.vpc,
    lamba_sg=network_stack.lambda_sg,
    env=env,
)

ec2_stack = Ec2Stack(
    app,
    "Ec2Stack",
    vpc=network_stack.vpc,
    ec2_sg=network_stack.ec2_sg,
    env=env,
)

TwitterSilverStack(
    app,
    "TwitterSilverStack",
    bronze_bucket_name=f"data-lake-bucket-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}",
    vpc=network_stack.vpc,
    lamba_sg=network_stack.lambda_sg,
    env=env,
)

TwitterGoldStack(
    app,
    "TwitterGoldStack",
    bronze_bucket_name=f"data-lake-bucket-{cdk.Aws.ACCOUNT_ID}-{cdk.Aws.REGION}",
    vpc=network_stack.vpc,
    lamba_sg=network_stack.lambda_sg,
    env=env,
)

app.synth()