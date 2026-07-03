from constructs import Construct
from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    CfnOutput,
)


class NetworkStack(Stack):
    """
    - 1 VPC, 1 AZ
    - public subnet  -> EC2 (Postgres + Superset)
    - private subnet -> Lambdas (HN, Twitter, normalization, transform, notifier)
    - NAT Gateway so Lambdas can reach the internet (HN/X APIs)
    - 2 security groups: one for EC2, one for Lambdas
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Single VPC, one public + one private subnet, NAT for the private side
        self.vpc = ec2.Vpc(
            self,
            "ProjectVpc",
            vpc_name="social-medias-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=1,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="PublicSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="PrivateSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # EC2 security group - hosts Postgres + Superset
        self.ec2_sg = ec2.SecurityGroup(
            self,
            "Ec2SecurityGroup",
            vpc=self.vpc,
            security_group_name="ec2-superset-postgres-sg",
            description="SG for the EC2 instance running Postgres and Superset",
            allow_all_outbound=True,
        )

        # SSH access - replace with your own IP before deploying, don't leave this open
        self.ec2_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4("109.245.132.129/32"),  # TODO: swap for ec2.Peer.ipv4("YOUR_IP/32")
            connection=ec2.Port.tcp(22),
            description="SSH (temporary - lock down to admin IP)",
        )

        # Superset UI
        self.ec2_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4("109.245.132.129/32"),  # TODO: swap for ec2.Peer.ipv4("YOUR_IP/32")
            connection=ec2.Port.tcp(8088),
            description="Superset dashboard access",
        )

        # Lambda security group - shared by every Lambda in the project
        self.lambda_sg = ec2.SecurityGroup(
            self,
            "LambdaSecurityGroup",
            vpc=self.vpc,
            security_group_name="lambda-functions-sg",
            description="SG for all project Lambdas",
            allow_all_outbound=True,  # Lambdas need to call external APIs + AWS services
        )

        # Only Lambdas can talk to Postgres, not the open internet
        self.ec2_sg.add_ingress_rule(
            peer=self.lambda_sg,
            connection=ec2.Port.tcp(5432),
            description="Postgres access, Lambdas only (gold to postgres loader)",
        )

        # VPC endpoint so Lambdas in the private subnet can reach Secrets Manager
        # without going through the NAT gateway / internet
        self.secrets_manager_endpoint = self.vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            security_groups=[self.lambda_sg],
        )

        # Outputs so other stacks (HN, Twitter, EC2) can plug into this network
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id, description="VPC ID")
        CfnOutput(
            self,
            "PrivateSubnetIds",
            value=",".join([s.subnet_id for s in self.vpc.private_subnets]),
            description="Private subnet IDs (for Lambdas)",
        )
        CfnOutput(
            self,
            "PublicSubnetIds",
            value=",".join([s.subnet_id for s in self.vpc.public_subnets]),
            description="Public subnet IDs (for EC2)",
        )
        CfnOutput(self, "Ec2SecurityGroupId", value=self.ec2_sg.security_group_id)
        CfnOutput(self, "LambdaSecurityGroupId", value=self.lambda_sg.security_group_id)