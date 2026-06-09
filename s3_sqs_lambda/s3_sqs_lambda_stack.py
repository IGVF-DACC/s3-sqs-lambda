from constructs import Construct
from aws_cdk import Duration, Stack
from aws_cdk import aws_s3
from aws_cdk import aws_sqs
from aws_cdk import aws_lambda 
from aws_cdk import aws_s3_notifications
from aws_cdk import aws_secretsmanager
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from s3_sqs_lambda.config import config
LAMBDA_TIMEOUT = Duration.seconds(30)
# AWS docs recommend visibility timeout >= 6x the Lambda timeout
VISIBILITY_TIMEOUT = Duration.seconds(180)


class S3SqsLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = aws_s3.Bucket.from_bucket_name(
            self,
            "TaggingEventBucket",
            bucket_name=config['bucket_name'],
        )

        dlq = aws_sqs.Queue(self, "TaggingEventDLQ")

        queue = aws_sqs.Queue(
            self,
            "TaggingEventQueue",
            visibility_timeout=VISIBILITY_TIMEOUT,
            dead_letter_queue=aws_sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
        )

        bucket.add_event_notification(
            aws_s3.EventType.OBJECT_TAGGING_PUT,
            aws_s3_notifications.SqsDestination(queue),
        )

        tagging_handler = aws_lambda.Function(
            self,
            "TaggingHandler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            code=aws_lambda.Code.from_asset("lambda/tagging_handler"),
            handler="index.handler",
            timeout=LAMBDA_TIMEOUT,
            environment={
                "PORTAL_API_URL": config['portal_api_url'],
                "PORTAL_SECRET_ARN": config['portal_secret_arn'],
            },
        )

        bucket.grant_read(tagging_handler)

        portal_secret = aws_secretsmanager.Secret.from_secret_complete_arn(
            self,
            "PortalSecret",
            secret_complete_arn=config['portal_secret_arn'],
        )
        portal_secret.grant_read(tagging_handler)

        tagging_handler.add_event_source(
            SqsEventSource(
                queue,
                batch_size=10,
                report_batch_item_failures=True,
            )
        )
