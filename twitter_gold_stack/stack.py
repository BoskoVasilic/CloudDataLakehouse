from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    Size,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    aws_ec2 as ec2,
    aws_sns as sns,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    CfnOutput,
)


class TwitterGoldStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 bronze_bucket_name: str, vpc, lamba_sg, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bronze_bucket = s3.Bucket.from_bucket_name(
            self, "DataLakeBucket", bronze_bucket_name
        )

        lambda_role = iam.Role(
            self,
            "TwitterGoldLambdaRole",
            role_name="twitter-gold-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="IAM Role za Twitter Gold Layer Lambda",
        )

        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaVPCAccessExecutionRole"
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="AllowS3SilverRead",
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject"],
                resources=[f"{bronze_bucket.bucket_arn}/silver/*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="AllowS3GoldWrite",
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject", "s3:DeleteObject"],
                resources=[f"{bronze_bucket.bucket_arn}/gold/*"],
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="AllowS3List",
                effect=iam.Effect.ALLOW,
                actions=["s3:ListBucket"],
                resources=[bronze_bucket.bucket_arn],
            )
        )

        aws_wrangler_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSWranglerLayer",
            layer_version_arn="arn:aws:lambda:eu-north-1:336392948345:layer:AWSSDKPandas-Python312:16",
        )

        twitter_gold_lambda = _lambda.Function(
            self,
            "TwitterGoldFunction",
            function_name="twitter-gold-calculator",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("twitter_gold_lambda"),
            role=lambda_role,
            timeout=Duration.minutes(15),
            memory_size=3008,
            ephemeral_storage_size=Size.gibibytes(1),
            layers=[aws_wrangler_layer],
            environment={
                "BRONZE_BUCKET_NAME": bronze_bucket_name,
            },
            vpc=vpc,
            security_groups=[lamba_sg],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            description="Racuna Twitter Gold metrike iz Silver layer-a",
        )

        error_topic = sns.Topic.from_topic_arn(
            self,
            "SharedErrorTopic",
            topic_arn=f"arn:aws:sns:{self.region}:{self.account}:hn-collector-errors",
        )

        gold_error_alarm = cloudwatch.Alarm(
            self,
            "TwitterGoldErrorAlarm",
            alarm_name="twitter-gold-lambda-errors",
            metric=twitter_gold_lambda.metric_errors(period=Duration.minutes(15), statistic="Sum"),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        gold_error_alarm.add_alarm_action(cw_actions.SnsAction(error_topic))

        CfnOutput(
            self,
            "TwitterGoldLambdaName",
            value=twitter_gold_lambda.function_name,
            description="Twitter Gold Lambda — pokrenuti rucno jednom",
        )