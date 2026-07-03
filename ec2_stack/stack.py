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
            "apt-get update -y",
            "apt-get upgrade -y",

            # PostgreSQL
            "apt-get install -y postgresql postgresql-contrib",
            "systemctl start postgresql",
            "systemctl enable postgresql",

            # Python + venv (PEP 668 fix)
            "apt-get install -y python3-pip python3-venv unzip curl",

            # Superset u venv-u, sa svim paketima koji su nam trebali ručno
            "python3 -m venv /opt/superset-venv",
            "/opt/superset-venv/bin/pip install --upgrade pip",
            "/opt/superset-venv/bin/pip install apache-superset psycopg2-binary rich cachetools",

            # SECRET_KEY config
            "mkdir -p /etc/superset",
            f"echo \"SECRET_KEY = '{superset_secret_key}'\" > /etc/superset/superset_config.py",
            "echo 'export SUPERSET_CONFIG_PATH=/etc/superset/superset_config.py' >> /etc/environment",

            "export SUPERSET_CONFIG_PATH=/etc/superset/superset_config.py",
            "/opt/superset-venv/bin/superset db upgrade",
            "/opt/superset-venv/bin/superset fab create-admin --username admin --firstname Admin "
            "--lastname Admin --email admin@example.com --password admin123",
            "/opt/superset-venv/bin/superset init",

            "nohup /opt/superset-venv/bin/superset run -h 0.0.0.0 -p 8088 --with-threads "
            "> /var/log/superset.log 2>&1 &",
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
                        "which aws || (sudo apt-get update -y && sudo apt-get install -y unzip curl && "
                        "curl -s \"https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip\" -o /tmp/awscliv2.zip && "
                        "cd /tmp && unzip -q -o awscliv2.zip && sudo ./aws/install --update)",
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
                        "sudo -u postgres psql -c \"ALTER USER $DBUSER WITH PASSWORD '$DBPASS'\"",
                        "sudo -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE $DBNAME TO $DBUSER\"",
                        "sudo -u postgres psql -d $DBNAME -c \"GRANT ALL ON SCHEMA public TO $DBUSER\"",
                        "sudo sed -i \"s/^#*listen_addresses.*/listen_addresses = '*'/\" /etc/postgresql/*/main/postgresql.conf",
                        "echo \"host all all 10.0.0.0/16 md5\" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf",
                        "echo \"host all all 127.0.0.1/32 md5\" | sudo tee -a /etc/postgresql/*/main/pg_hba.conf",
                        "sudo systemctl restart postgresql",
                    ],
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