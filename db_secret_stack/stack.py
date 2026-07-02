from aws_cdk import Stack, RemovalPolicy
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct


class DbSecretStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.db_secret = secretsmanager.Secret(
            self, "PostgresLoaderSecret",
            secret_name="lakehouse/postgres-loader",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"loader_user","dbname":"lakehouse"}',
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=24,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )