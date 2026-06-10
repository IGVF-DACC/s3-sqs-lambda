import json
import os
import boto3
import requests

from typing import Any, Dict, List

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
portal_api_url = os.getenv("PORTAL_API_URL")
portal_secret_arn = os.getenv("PORTAL_SECRET_ARN")

secret = json.loads(
    secrets_client.get_secret_value(SecretId=portal_secret_arn)["SecretString"]
)
portal_key = secret["BACKEND_KEY"]
portal_secret_key = secret["BACKEND_SECRET_KEY"]

def get_collections(portal_url: str, portal_key: str, portal_secret_key: str) -> List[str]|None:
    r = requests.get(portal_url, auth=(portal_key, portal_secret_key))
    r.raise_for_status()
    return r.json().get("collections")

def resolve_collections(s3_object_collections: str, portal_collections: List[str]) -> List[str]:
    resolved_collections = []
    s3_object_collections = s3_object_collections.split(" ")
    resolved_collections.extend(s3_object_collections)
    resolved_collections.extend(portal_collections)
    return list(set(resolved_collections))

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

                portal_accession = next(
                    (tag["Value"] for tag in tags if tag["Key"] == "portal_accession"),
                    None,
                )
                # we might get tagging events even if the object is not in the portal
                # we might later filter the notifications to just certain prefixes      
                if portal_accession is None:
                    print("No portal_accession tag found, skipping")
                    continue

                s3_object_collections = next(
                    (tag["Value"] for tag in tags if tag["Key"] == "collections"),
                    None,
                )
                if s3_object_collections is None:
                    raise ValueError("No collections tag found")

                portal_url = f"{portal_api_url}/{portal_accession}"
                print(f"Portal URL: {portal_url}")

                portal_collections = get_collections(portal_url, portal_key, portal_secret_key)

                collections_to_patch = resolve_collections(s3_object_collections, portal_collections)
                print(f"Collections to patch: {collections_to_patch}")

                r = requests.patch(portal_url, auth=(portal_key, portal_secret_key), json={"collections": collections_to_patch})
                r.raise_for_status()
                print(f"Collections patched: {r.json()}")

        except Exception as e:
            print(f"Error processing message {message_id}: {e}")
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
