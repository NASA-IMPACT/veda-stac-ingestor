from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_lambda_python_alpha,
    aws_lambda,
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
        handler = self.build_lambda(
            table=table,
            stage=config.stage,
            jwks_url=jwks_url,
        )
        self.build_api(
            handler=handler,
            stage=config.stage,
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
        )
        table.add_global_secondary_index(
            index_name="status",
            partition_key={"name": "status", "type": dynamodb.AttributeType.STRING},
            sort_key={"name": "created_at", "type": dynamodb.AttributeType.STRING},
        )
        return table

    def build_lambda(
        self,
        *,
        table: dynamodb.ITable,
        stage: str,
        jwks_url: str,
    ) -> apigateway.LambdaRestApi:
        handler = aws_lambda_python_alpha.PythonFunction(
            self,
            "api-handler",
            entry="api",
            index="src/handler.py",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            environment={
                "DYNAMODB_TABLE": table.table_name,
                "JWKS_URL": jwks_url,
                "ROOT_PATH": f"/{stage}",
            },
        )
        table.grant_read_write_data(handler)
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
