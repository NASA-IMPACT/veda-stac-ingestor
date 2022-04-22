import resource
from unicodedata import name
from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class StacIngestionSystem(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = self.build_table()
        bucket = self.build_bucket()
        role = self.build_upload_role(bucket)


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

    def build_upload_role(self, bucket: s3.IBucket) -> iam.IRole:
        role = iam.Role(
            self,
            "s3-upload",
            description="Role granted to users to enable upload of STAC assets to bucket",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
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
        role.assume_role_policy.add_statements(
            iam.PolicyStatement(
                actions=["sts:AssumeRole"],
                principals=[iam.AccountPrincipal(Stack.of(self).account)],
                conditions={},
            )
        )
        return role

