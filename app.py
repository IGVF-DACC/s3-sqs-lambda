#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import Environment
from s3_sqs_lambda.s3_sqs_lambda_stack import S3SqsLambdaStack
from s3_sqs_lambda.config import config

env = Environment(
    account=config["account"],
    region=config["region"],
)

app = cdk.App()
S3SqsLambdaStack(app, "S3SqsLambdaStack", env=env)
app.synth()
