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
    aws_sns_subscriptions as subscriptions,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    CfnOutput,
)


class DataCollectionStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
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

        lambda_role = iam.Role(
            self,
            "HNCollectorLambdaRole",
            role_name="hn-collector-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="IAM Role za Hacker News Bronze Layer Lambda function",
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
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


        error_topic = sns.Topic(
            self,
            "HNCollectorErrorTopic",
            topic_name="hn-collector-errors",
            display_name="Hacker News Collector Error",
        )


        lambda_error_alarm = cloudwatch.Alarm(
            self,
            "HNCollectorErrorAlarm",
            alarm_name="hn-collector-lambda-errors",
            alarm_description="Alarm when HN Collector Lambda function returns error",
            metric=hn_collector_lambda.metric_errors(
                period=Duration.minutes(15),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        lambda_error_alarm.add_alarm_action(
            cw_actions.SnsAction(error_topic)
        )


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
            "ErrorTopicArn",
            value=error_topic.topic_arn,
            description="ARN SNS Topic for error notifications",
        )
