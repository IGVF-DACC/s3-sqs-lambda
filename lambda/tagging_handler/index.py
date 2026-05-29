import json

import boto3

from typing import Any, Dict, List

s3_client = boto3.client("s3")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, List[Dict[str, str]]]:
    """
    Process S3 ObjectTagging:Put events from SQS queue.

    Returns partial batch response so that successfully processed messages
    are removed from the queue even if other messages in the batch fail.
    """
    print(f"Received {len(event['Records'])} SQS messages")

    batch_item_failures: list[dict[str, str]] = []

    for record in event["Records"]:
        message_id = record["messageId"]

        try:
            body = json.loads(record["body"])
            print(f"Message body: {json.dumps(body, indent=2)}")

            if "Event" in body and body["Event"] == "s3:TestEvent":
                print("Skipping S3 test event")
                continue

            for s3_record in body["Records"]:
                bucket_name = s3_record["s3"]["bucket"]["name"]
                object_key = s3_record["s3"]["object"]["key"]
                event_name = s3_record["eventName"]

                print(f"Event: {event_name}")
                print(f"Bucket: {bucket_name}")
                print(f"Object: {object_key}")

                response = s3_client.get_object_tagging(
                    Bucket=bucket_name,
                    Key=object_key,
                )
                tags = response.get("TagSet", [])
                print(f"Tags: {tags}")

        except Exception as e:
            print(f"Error processing message {message_id}: {e}")
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
