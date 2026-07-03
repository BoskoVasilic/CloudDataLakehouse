from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
    aws_secretsmanager as secretsmanager,
    CfnOutput,
)


class GoldPostgresLoaderStack(Stack):
    """
    Lambda that reads all gold parquet files from S3
    and loads them into PostgreSQL tables on the EC2 instance.
    Runs daily at 04:00 UTC (after HN gold at 03:00).
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        lambda_sg: ec2.SecurityGroup,
        gold_bucket_name: str,
        db_secret: secretsmanager.Secret,
        ec2_instance: ec2.Instance,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        aws_sdk_pandas_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "AWSSDKPandasLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python312:29",
        )

        role = iam.Role(
            self,
            "GoldLoaderRole",
            role_name="gold-postgres-loader-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        # VPC access (needed since Lambda is in private subnet)
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaVPCAccessExecutionRole"
            )
        )
        # Read all gold parquet files from S3
        role.add_to_policy(iam.PolicyStatement(
            sid="AllowGoldRead",
            effect=iam.Effect.ALLOW,
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::{gold_bucket_name}",
                f"arn:aws:s3:::{gold_bucket_name}/gold/*",
            ],
        ))

        loader_lambda = _lambda.Function(
            self,
            "GoldPostgresLoader",
            function_name="gold-postgres-loader",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("gold_postgres_loader"),
            role=role,
            timeout=Duration.minutes(10),
            memory_size=512,
            layers=[aws_sdk_pandas_layer],
            vpc=vpc,
            security_groups=[lambda_sg],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            environment={
                "GOLD_BUCKET_NAME": gold_bucket_name,
                "PG_HOST": ec2_instance.instance_private_ip,
                "PG_PORT": "5432",
                "DB_SECRET_ARN": db_secret.secret_arn,
            },
            description="Loads gold parquet data from S3 into PostgreSQL on EC2",
        )

        db_secret.grant_read(loader_lambda)

        # Runs at 04:00 UTC — after HN gold (03:00) and Twitter gold
        events.Rule(
            self,
            "GoldLoaderSchedule",
            rule_name="gold-postgres-loader-daily",
            schedule=events.Schedule.cron(
                minute="0", hour="4", day="*", month="*", year="*"
            ),
        ).add_target(targets.LambdaFunction(loader_lambda))

        CfnOutput(self, "LoaderLambdaName", value=loader_lambda.function_name)