from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_sns as sns,
    aws_ec2 as ec2,
    aws_ssm as ssm,
    aws_sns_subscriptions as subscriptions,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    CfnOutput,
)


class DataCollectionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc, lamba_sg, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bronze_bucket = s3.Bucket(
            self,
            "DataLakeBucket",
            bucket_name=f"data-lake-bucket-{self.account}-{self.region}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

        aws_sdk_pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSSDKPandasLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python312:29",
        )

        lambda_role = iam.Role(
            self,
            "HNCollectorLambdaRole",
            role_name="hn-collector-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="IAM Role za Hacker News Bronze Layer Lambda function",
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaVPCAccessExecutionRole"
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="AllowS3BronzeWrite",
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=[f"{bronze_bucket.bucket_arn}/bronze/hacker_news/*"],
            )
        )

        silver_lambda_role = iam.Role(
            self, "HNSilverLambdaRole",
            role_name="hn-silver-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        silver_lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaVPCAccessExecutionRole"
            )
        )

        silver_lambda_role.add_to_policy(iam.PolicyStatement(
            sid="AllowBronzeRead",
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject"],
            resources=[f"{bronze_bucket.bucket_arn}/bronze/hacker_news/*"],
        ))
        silver_lambda_role.add_to_policy(iam.PolicyStatement(
            sid="AllowSilverWrite",
            effect=iam.Effect.ALLOW,
            actions=["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
            resources=[f"{bronze_bucket.bucket_arn}/silver/*"],
        ))

        silver_lambda_role.add_to_policy(iam.PolicyStatement(
            sid="AllowSilverList",
            effect=iam.Effect.ALLOW,
            actions=["s3:ListBucket"],
            resources=[bronze_bucket.bucket_arn],
        ))

        gold_lambda_role = iam.Role(
            self, "HNGoldLambdaRole",
            role_name="hn-gold-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        gold_lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaVPCAccessExecutionRole"
            )
        )

        gold_lambda_role.add_to_policy(iam.PolicyStatement(
            sid="AllowSilverRead",
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject"],
            resources=[f"{bronze_bucket.bucket_arn}/silver/*"],
        ))

        gold_lambda_role.add_to_policy(iam.PolicyStatement(
            sid="AllowGoldWrite",
            effect=iam.Effect.ALLOW,
            actions=["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
            resources=[f"{bronze_bucket.bucket_arn}/gold/*"],
        ))

        gold_lambda_role.add_to_policy(iam.PolicyStatement(
            sid="AllowGoldList",
            effect=iam.Effect.ALLOW,
            actions=["s3:ListBucket"],
            resources=[bronze_bucket.bucket_arn],
        ))

        hn_collector_lambda = _lambda.Function(
            self,
            "HNCollectorFunction",
            function_name="hn-bronze-collector",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("hacker_news_collector"),
            role=lambda_role,
            timeout=Duration.minutes(10),
            memory_size=256,
            environment={
                "BRONZE_BUCKET_NAME": bronze_bucket.bucket_name,
            },
            description="Collects HN data from pervious day and writes it to S3 bronze layer",
        )

        silver_lambda = _lambda.Function(
            self, "HNSilverNormalizer",
            function_name="hn-silver-normalizer",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("hacker_news_normalizer"),
            role=silver_lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            layers=[aws_sdk_pandas_layer],
            environment={
                "BRONZE_BUCKET_NAME": bronze_bucket.bucket_name,
                "SILVER_BUCKET_NAME": bronze_bucket.bucket_name,
            },
            description="Normalizes HN data from S3 bronze layer and writes it to S3 silver layer",
        )

        gold_lambda = _lambda.Function(
            self, "HNGoldTransformer",
            function_name="hn-gold-transformer",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("hacker_news_transformator"),
            role=gold_lambda_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            layers=[aws_sdk_pandas_layer],
            environment={
                "SILVER_BUCKET_NAME": bronze_bucket.bucket_name,
                "GOLD_BUCKET_NAME": bronze_bucket.bucket_name,
            },
            vpc=vpc,
            security_groups=[lamba_sg],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            description="Transforms HN data from S3 silver layer and writes it to S3 gold layer",
        )

        discord_webhook_param = ssm.StringParameter.from_secure_string_parameter_attributes(
            self,
            "DiscordWebhookParam",
            parameter_name="/hn-pipeline/discord-webhook-url",
        )

        discord_lambda_role = iam.Role(
            self, "DiscordNotifierRole",
            role_name="hn-discord-notifier-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        discord_lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        discord_webhook_param.grant_read(discord_lambda_role)

        discord_notifier_lambda = _lambda.Function(
            self, "DiscordNotifierFunction",
            function_name="hn-discord-notifier",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("discord_notifier"),
            role=discord_lambda_role,
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "DISCORD_WEBHOOK_PARAMETER": "/hn-pipeline/discord-webhook-url",
            },
            description="Notifies Discord channel about HN data collection",
        )

        daily_schedule = events.Rule(
            self,
            "HNCollectorSchedule",
            rule_name="hn-collector-daily-schedule",
            description="Everyday trigers HN Bronze Layer collector at 01:00 UTC",
            schedule=events.Schedule.cron(
                minute="0",
                hour="1",
                day="*",
                month="*",
                year="*",
            ),
        )
        daily_schedule.add_target(
            targets.LambdaFunction(hn_collector_lambda)
        )

        events.Rule(
            self, "SilverSchedule",
            rule_name="hn-silver-daily",
            schedule=events.Schedule.cron(minute="0", hour="2", day="*", month="*", year="*"),
        ).add_target(targets.LambdaFunction(silver_lambda))

        events.Rule(
            self, "GoldSchedule",
            rule_name="hn-gold-daily",
            schedule=events.Schedule.cron(
                minute="0", hour="3", day="*", month="*", year="*"
            ),
        ).add_target(targets.LambdaFunction(gold_lambda))

        error_topic = sns.Topic(
            self,
            "HNCollectorErrorTopic",
            topic_name="hn-collector-errors",
            display_name="Hacker News Collector Error",
        )

        error_topic.add_subscription(
            subscriptions.LambdaSubscription(discord_notifier_lambda)
        )

        discord_notifier_lambda.add_permission(
            "AllowSNSInvoke",
            principal=iam.ServicePrincipal("sns.amazonaws.com"),
            source_arn=error_topic.topic_arn,
        )

        for fn, name in [(hn_collector_lambda, "bronze"), (silver_lambda, "silver"), (gold_lambda, "gold")]:
            alarm = cloudwatch.Alarm(
                self, f"HN{name.capitalize()}ErrorAlarm",
                alarm_name=f"hn-{name}-lambda-errors",
                metric=fn.metric_errors(period=Duration.minutes(15), statistic="Sum"),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(error_topic))


        CfnOutput(
            self,
            "BronzeBucketName",
            value=bronze_bucket.bucket_name,
            description="S3 Bronze bucket title",
        )
        CfnOutput(
            self,
            "CollectorLambdaName",
            value=hn_collector_lambda.function_name,
            description="Title of Lambda function for collecting",
        )
        CfnOutput(
            self,
            "SilverLambdaName",
            value=silver_lambda.function_name,
            description="Title of Lambda function for normalizing",
        )
        CfnOutput(
            self,
            "ErrorTopicArn",
            value=error_topic.topic_arn,
            description="ARN SNS Topic for error notifications",
        )
