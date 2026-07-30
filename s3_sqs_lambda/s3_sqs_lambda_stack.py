from constructs import Construct
from aws_cdk import Duration, Stack
from aws_cdk import aws_s3
from aws_cdk import aws_sqs
from aws_cdk import aws_lambda 
from aws_cdk import aws_s3_notifications
from aws_cdk import aws_secretsmanager
from aws_cdk import aws_cloudwatch
from aws_cdk import aws_cloudwatch_actions
from aws_cdk import aws_sns
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from s3_sqs_lambda.config import config
LAMBDA_TIMEOUT = Duration.seconds(30)
# AWS docs recommend visibility timeout >= 6x the Lambda timeout
VISIBILITY_TIMEOUT = Duration.seconds(180)
# Cap concurrent invocations so a bulk retagging run does not overwhelm the
# portal API, and to bound the read-modify-write window when several files patch
# the same accession at once.
MAX_CONCURRENCY = 5


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

        # PythonFunction bundles dependencies from the requirements.txt inside
        # `entry` (requires Docker at synth/deploy time).
        tagging_handler = PythonFunction(
            self,
            "TaggingHandler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            entry="lambda/tagging_handler",
            index="index.py",
            handler="handler",
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
                max_concurrency=MAX_CONCURRENCY,
                report_batch_item_failures=True,
            )
        )

        # Anything that lands in the DLQ has failed all redrive attempts, so a
        # portal collections sync needs manual investigation.
        dlq_alarm = aws_cloudwatch.Alarm(
            self,
            "TaggingEventDLQNotEmpty",
            metric=dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=(
                aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD
            ),
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
            alarm_description=(
                "Messages are in the tagging-event dead-letter queue: a portal "
                "collections sync failed all retries and needs investigation."
            ),
        )

        alarm_topic = aws_sns.Topic.from_topic_arn(
            self,
            "AlarmNotificationTopic",
            topic_arn=config['alarm_topic_arn'],
        )
        dlq_alarm.add_alarm_action(aws_cloudwatch_actions.SnsAction(alarm_topic))
