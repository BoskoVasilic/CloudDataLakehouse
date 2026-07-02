from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    Size,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_s3 as s3,
    CfnOutput,
)


class TwitterGoldStack(Stack):

    def __init__(self, scope: Construct, construct_id: str,
                 bronze_bucket_name: str, **kwargs) -> None:
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
                "service-role/AWSLambdaBasicExecutionRole"
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
            description="Racuna Twitter Gold metrike iz Silver layer-a",
        )

        CfnOutput(
            self,
            "TwitterGoldLambdaName",
            value=twitter_gold_lambda.function_name,
            description="Twitter Gold Lambda — pokrenuti rucno jednom",
        )