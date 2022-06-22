from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda_python_alpha,
    aws_lambda,
    aws_s3 as s3,
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
        bucket = self.build_bucket()
        role = self.build_upload_role(bucket=bucket)
        jwks_url = self.build_jwks_url(config.userpool_id)
        handler = self.build_lambda(
            table=table,
            role=role,
            bucket=bucket,
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
            name="s3_role_arn",
            value=role.role_arn,
            description="ARN of IAM Role to be assumed by users when uploading data",
        )
        self.register_ssm_parameter(
            name="s3_upload_bucket",
            value=bucket.bucket_name,
            description="Name of bucket used to store uploaded STAC assets",
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

    def build_bucket(self) -> s3.IBucket:
        return s3.Bucket(self, "upload-bucket", removal_policy=RemovalPolicy.DESTROY)

    def build_upload_role(
        self,
        *,
        bucket: s3.IBucket,
    ) -> iam.IRole:
        return iam.Role(
            self,
            "s3-upload",
            description="Role granted to users to enable upload of STAC assets to bucket",
            assumed_by=iam.AccountPrincipal(Stack.of(self).account),
            inline_policies={
                "allow-write": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:*"],
                            resources=[f"{bucket.bucket_arn}/${{aws:userid}}/*"],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:ListBucket*"],
                            resources=[bucket.bucket_arn],
                            conditions={
                                "StringLike": {
                                    "s3:prefix": ["${aws:userid}/*", "${aws:userid}"]
                                }
                            },
                        ),
                    ]
                )
            },
        )

    def build_lambda(
        self,
        *,
        table: dynamodb.ITable,
        role: iam.IRole,
        bucket: s3.IBucket,
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
                "ROOT_PATH": f"/{stage}",
                "S3_ROLE_ARN": role.role_arn,
                "S3_UPLOAD_BUCKET": bucket.bucket_name,
                "DYNAMODB_TABLE": table.table_name,
                "JWKS_URL": jwks_url,
            },
        )
        table.grant_read_write_data(handler)
        role.grant(handler.grant_principal, "sts:AssumeRole")
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
