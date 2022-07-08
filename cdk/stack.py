from typing import Dict
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigateway,
    aws_ec2 as ec2,
    aws_dynamodb as dynamodb,
    aws_lambda,
    aws_lambda_event_sources as events,
    aws_lambda_python_alpha,
    aws_secretsmanager as secretsmanager,
    aws_ssm as ssm,
)
from constructs import Construct

from .config import Deployment


class StacIngestionApi(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        config: Deployment,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        table = self.build_table()
        jwks_url = self.build_jwks_url(config.userpool_id)
        env = {
            "DYNAMODB_TABLE": table.table_name,
            "JWKS_URL": jwks_url,
            "ROOT_PATH": f"/{config.stage}",
            "NO_PYDANTIC_SSM_SETTINGS": "1",
            "STAC_URL": config.stac_url,
        }
        handler = self.build_api_lambda(
            table=table,
            env=env,
        )
        self.build_api(
            handler=handler,
            stage=config.stage,
        )

        self.build_ingestor(
            table=table,
            env=env,
            db_secret=self.get_db_secret(config.stac_db_secret_name),
            db_vpc=ec2.Vpc.from_lookup(self, "vpc", vpc_id=config.stac_db_vpc_id),
            db_security_group=ec2.SecurityGroup.from_security_group_id(
                self,
                "db-security-group",
                security_group_id=config.stac_db_security_group_id,
            ),
            db_subnet_public=config.stac_db_public_subnet,
        )

        self.register_ssm_parameter(
            name="jwks_url",
            value=jwks_url,
            description="JWKS URL for Cognito user pool",
        )
        self.register_ssm_parameter(
            name="dynamodb_table",
            value=table.table_name,
            description="Name of table used to store ingestions",
        )

    def build_jwks_url(self, userpool_id: str) -> str:
        region = userpool_id.split("_")[0]
        return (
            f"https://cognito-idp.{region}.amazonaws.com"
            f"/{userpool_id}/.well-known/jwks.json"
        )

    def build_table(self) -> dynamodb.ITable:
        table = dynamodb.Table(
            self,
            "ingestions-table",
            partition_key={"name": "created_by", "type": dynamodb.AttributeType.STRING},
            sort_key={"name": "id", "type": dynamodb.AttributeType.STRING},
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
        )
        table.add_global_secondary_index(
            index_name="status",
            partition_key={"name": "status", "type": dynamodb.AttributeType.STRING},
            sort_key={"name": "created_at", "type": dynamodb.AttributeType.STRING},
        )
        return table

    def build_api_lambda(
        self,
        *,
        table: dynamodb.ITable,
        env: Dict[str, str],
    ) -> apigateway.LambdaRestApi:
        handler = aws_lambda_python_alpha.PythonFunction(
            self,
            "api-handler",
            entry="api",
            index="src/handler.py",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            environment=env,
            timeout=Duration.seconds(5),
        )
        table.grant_read_write_data(handler)
        return handler

    def build_ingestor(
        self,
        *,
        table: dynamodb.ITable,
        env: Dict[str, str],
        db_secret: secretsmanager.ISecret,
        db_vpc: ec2.IVpc,
        db_security_group: ec2.ISecurityGroup,
        db_subnet_public: bool,
    ) -> aws_lambda_python_alpha.PythonFunction:
        handler = aws_lambda_python_alpha.PythonFunction(
            self,
            "stac-ingestor",
            entry="api",
            index="src/ingestor.py",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            timeout=Duration.seconds(180),
            environment={"DB_SECRET_ARN": db_secret.secret_arn, **env},
            vpc=db_vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC
                if db_subnet_public
                else ec2.SubnetType.PRIVATE_ISOLATED
            ),
            allow_public_subnet=True,
        )

        # Allow handler to read DB secret
        db_secret.grant_read(handler)

        # Allow handler to connect to DB
        db_security_group.add_ingress_rule(
            peer=handler.connections.security_groups[0],
            connection=ec2.Port.tcp(5432),
            description="Allow connections from STAC Ingestor",
        )

        # Allow handler to write results back to DBƒ
        table.grant_write_data(handler)

        # Trigger handler from writes to DynamoDB table
        handler.add_event_source(
            events.DynamoEventSource(
                table=table,
                # Read when batches reach 100...
                batch_size=100,
                # ... or when window is reached.
                max_batching_window=Duration.seconds(30),
                # Read oldest data first.
                starting_position=aws_lambda.StartingPosition.TRIM_HORIZON,
                retry_attempts=1,
            )
        )

        return handler

    def build_api(
        self,
        *,
        handler: aws_lambda.IFunction,
        stage: str,
    ) -> apigateway.LambdaRestApi:
        return apigateway.LambdaRestApi(
            self,
            f"{Stack.of(self).stack_name}-api",
            handler=handler,
            cloud_watch_role=True,
            deploy_options=apigateway.StageOptions(stage_name=stage),
        )

    def get_db_secret(self, secret_name: str) -> secretsmanager.ISecret:
        return secretsmanager.Secret.from_secret_name_v2(
            self, "pgstac-db-secret", secret_name
        )

    def register_ssm_parameter(
        self,
        name: str,
        value: str,
        description: str,
    ) -> ssm.IStringParameter:
        parameter_namespace = Stack.of(self).stack_name
        return ssm.StringParameter(
            self,
            f"{name.replace('_', '-')}-parameter",
            description=description,
            parameter_name=f"/{parameter_namespace}/{name}",
            string_value=value,
        )
