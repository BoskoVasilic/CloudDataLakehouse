from constructs import Construct
from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_iam as iam,
    CfnOutput,
)
from aws_cdk import aws_ssm as ssm
from aws_cdk import custom_resources as cr


class Ec2Stack(Stack):
    """
    EC2 instance running PostgreSQL + Apache Superset.
    Lives in the public subnet from NetworkStack.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.Vpc,
        ec2_sg: ec2.SecurityGroup,
        db_secret,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # IAM role for EC2 
        role = iam.Role(
            self,
            "Ec2InstanceRole",
            role_name="ec2-superset-postgres-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
        )
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMManagedInstanceCore"
            )
        )

        # User data script - runs on first boot, installs Postgres + Superset
        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            # System update
            "apt-get update -y",
            "apt-get upgrade -y",

            # PostgreSQL
            "apt-get install -y postgresql postgresql-contrib",
            "systemctl start postgresql",
            "systemctl enable postgresql",

            # Python + pip
            "apt-get install -y python3-pip python3-venv",

            # Apache Superset
            "pip3 install apache-superset",
            "superset db upgrade",
            "superset fab create-admin --username admin --firstname Admin "
            "--lastname Admin --email admin@example.com --password admin123",
            "superset init",

            # Start Superset on port 8088
            "nohup superset run -p 8088 --with-threads --reload --debugger &",
        )

        # EC2 instance in public subnet
        self.instance = ec2.Instance(
            self,
            "SupersetPostgresInstance",
            instance_name="superset-postgres-ec2",
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.T3, ec2.InstanceSize.MICRO
            ),
            machine_image=ec2.MachineImage.generic_linux(
                {"eu-north-1": "ami-05bfa4a7765f38076"}  # Ubuntu 24.04 LTS, Stockholm, free tier, found through AWS Marketplace
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
            ),
            security_group=ec2_sg,
            role=role,
            user_data=user_data,
        )

        CfnOutput(
            self,
            "InstancePublicIp",
            value=self.instance.instance_public_ip,
            description="EC2 public IP (for SSH / Superset access)",
        )
        CfnOutput(
            self,
            "SupersetUrl",
            value=f"http://{self.instance.instance_public_ip}:8088",
            description="Apache Superset URL",
        )

        db_secret.grant_read(self.instance.role)

        setup_document = ssm.CfnDocument(
            self, "PostgresSetupDocument",
            document_type="Command",
            content={
                "schemaVersion": "2.2",
                "description": "Create lakehouse DB and loader_user",
                "mainSteps": [{
                    "action": "aws:runShellScript",
                    "name": "setupPostgres",
                    "inputs": {
                        "runCommand": [
                            "SECRET=$(aws secretsmanager get-secret-value "
                            f"--secret-id {db_secret.secret_arn} --region eu-north-1 "
                            "--query SecretString --output text)",
                            "DBNAME=$(echo $SECRET | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"dbname\"])')",
                            "DBUSER=$(echo $SECRET | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"username\"])')",
                            "DBPASS=$(echo $SECRET | python3 -c 'import sys,json; print(json.load(sys.stdin)[\"password\"])')",
                            "sudo -u postgres psql -tc \"SELECT 1 FROM pg_database WHERE datname='$DBNAME'\" | grep -q 1 || "
                            "sudo -u postgres psql -c \"CREATE DATABASE $DBNAME\"",
                            "sudo -u postgres psql -tc \"SELECT 1 FROM pg_roles WHERE rolname='$DBUSER'\" | grep -q 1 || "
                            "sudo -u postgres psql -c \"CREATE USER $DBUSER WITH PASSWORD '$DBPASS'\"",
                            "sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE $DBNAME TO $DBUSER\"",
                            "sudo sed -i \"s/^#*listen_addresses.*/listen_addresses = '*'/\" /etc/postgresql/*/main/postgresql.conf",
                            "echo \"host all all 10.0.0.0/16 md5\" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf",
                            "sudo systemctl restart postgresql",
                        ]
                    },
                }],
            },
        )

        run_setup = cr.AwsCustomResource(
            self, "RunPostgresSetup",
            on_create=cr.AwsSdkCall(
                service="SSM",
                action="sendCommand",
                parameters={
                    "DocumentName": setup_document.ref,
                    "InstanceIds": [self.instance.instance_id],
                },
                physical_resource_id=cr.PhysicalResourceId.of("PostgresSetupRun"),
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
        )

        run_setup.node.add_dependency(setup_document)