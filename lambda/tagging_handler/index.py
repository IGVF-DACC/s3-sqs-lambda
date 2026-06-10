import json
import os
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote_plus

import boto3
import requests

s3_client = boto3.client("s3")
portal_api_url = os.getenv("PORTAL_API_URL")


@lru_cache(maxsize=1)
def get_portal_auth() -> Tuple[str, str]:
    secrets_client = boto3.client("secretsmanager")
    secret = json.loads(
        secrets_client.get_secret_value(SecretId=os.environ["PORTAL_SECRET_ARN"])[
            "SecretString"
        ]
    )
    return secret["BACKEND_KEY"], secret["BACKEND_SECRET_KEY"]


def get_tag_value(tags: List[Dict[str, str]], key: str) -> Optional[str]:
    return next((tag["Value"] for tag in tags if tag["Key"] == key), None)


def parse_collections_tag(tag_value: str) -> List[str]:
    """Parse a space-separated collections tag value, dropping empty entries."""
    return tag_value.split()


def get_portal_collections(portal_url: str, auth: Tuple[str, str]) -> List[str]:
    r = requests.get(
        portal_url,
        auth=auth,
        params={"frame": "object"},
        headers={"Accept": "application/json"},
    )
    r.raise_for_status()
    return r.json().get("collections", [])


def resolve_collections(
    s3_object_collections: List[str], portal_collections: List[str]
) -> Optional[List[str]]:
    """
    Merge S3 tag collections into the portal's collections.

    Returns the merged list (portal order preserved, new entries appended in
    sorted order), or None if the portal already has every collection.
    """
    missing = set(s3_object_collections) - set(portal_collections)
    if not missing:
        return None
    return portal_collections + sorted(missing)


def process_s3_record(s3_record: Dict[str, Any]) -> None:
    bucket_name = s3_record["s3"]["bucket"]["name"]
    # Keys in S3 event notifications are URL-encoded.
    object_key = unquote_plus(s3_record["s3"]["object"]["key"])
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

    # We might get tagging events even if the object is not in the portal.
    # We might later filter the notifications to just certain prefixes.
    portal_accession = get_tag_value(tags, "portal_accession")
    if portal_accession is None:
        print("No portal_accession tag found, skipping")
        return

    # PutObjectTagging replaces the whole tag set, so unrelated tag updates
    # (or removal of the collections tag) legitimately arrive here.
    collections_tag = get_tag_value(tags, "collections")
    if collections_tag is None:
        print("No collections tag found, skipping")
        return

    s3_object_collections = parse_collections_tag(collections_tag)
    if not s3_object_collections:
        print("Empty collections tag, skipping")
        return

    portal_url = f"{portal_api_url}/{portal_accession}"
    print(f"Portal URL: {portal_url}")

    auth = get_portal_auth()
    portal_collections = get_portal_collections(portal_url, auth)

    collections_to_patch = resolve_collections(
        s3_object_collections, portal_collections
    )
    if collections_to_patch is None:
        print("Portal already has all collections, skipping")
        return
    print(f"Collections to patch: {collections_to_patch}")

    r = requests.patch(
        portal_url,
        auth=auth,
        json={"collections": collections_to_patch},
    )
    r.raise_for_status()
    print(f"Collections patched: {r.json()}")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, List[Dict[str, str]]]:
    """
    Process S3 ObjectTagging:Put events from SQS queue.

    Returns partial batch response so that successfully processed messages
    are removed from the queue even if other messages in the batch fail.
    """
    print(f"Received {len(event['Records'])} SQS messages")

    batch_item_failures: List[Dict[str, str]] = []

    for record in event["Records"]:
        message_id = record["messageId"]

        try:
            body = json.loads(record["body"])
            print(f"Message body: {json.dumps(body, indent=2)}")

            if body.get("Event") == "s3:TestEvent":
                print("Skipping S3 test event")
                continue

            for s3_record in body["Records"]:
                process_s3_record(s3_record)

        except Exception as e:
            print(f"Error processing message {message_id}: {e}")
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
