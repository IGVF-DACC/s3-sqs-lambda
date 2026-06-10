import json
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

import index


def make_sqs_event(*bodies: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "Records": [
            {"messageId": f"message-{i}", "body": json.dumps(body)}
            for i, body in enumerate(bodies)
        ]
    }


def make_s3_body(key: str = "some/object.txt") -> Dict[str, Any]:
    return {
        "Records": [
            {
                "eventName": "ObjectTagging:Put",
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": key},
                },
            }
        ]
    }


def make_tag_set(tags: Dict[str, str]) -> List[Dict[str, str]]:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


@pytest.fixture
def s3(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(index, "s3_client", mock)
    return mock


@pytest.fixture
def portal(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr(index, "requests", mock)
    monkeypatch.setattr(index, "get_portal_auth", lambda: ("key", "secret"))
    return mock


@pytest.fixture
def set_tags(s3):
    def _set_tags(tags: Dict[str, str]) -> None:
        s3.get_object_tagging.return_value = {"TagSet": make_tag_set(tags)}

    return _set_tags


@pytest.fixture
def set_portal_collections(portal):
    def _set_portal_collections(collections: Optional[List[str]]) -> None:
        response = MagicMock()
        response.json.return_value = (
            {} if collections is None else {"collections": collections}
        )
        portal.get.return_value = response

    return _set_portal_collections


@pytest.mark.parametrize(
    "tag_value, expected",
    [
        ("a b c", ["a", "b", "c"]),
        ("a  b ", ["a", "b"]),
        ("", []),
    ],
)
def test_parse_collections_tag(tag_value, expected):
    assert index.parse_collections_tag(tag_value) == expected


def test_resolve_collections_returns_none_when_nothing_new():
    assert index.resolve_collections(["a"], ["a", "b"]) is None


def test_resolve_collections_merges_preserving_portal_order():
    assert index.resolve_collections(["c", "a"], ["b", "a"]) == ["b", "a", "c"]


def test_resolve_collections_first_collection_on_empty_portal():
    assert index.resolve_collections(["a"], []) == ["a"]


def test_resolve_collections_appends_new_entries_sorted():
    assert index.resolve_collections(["z", "c"], ["a"]) == ["a", "c", "z"]


def test_handler_skips_s3_test_event(s3, portal):
    event = make_sqs_event({"Event": "s3:TestEvent"})

    result = index.handler(event, None)

    assert result == {"batchItemFailures": []}
    s3.get_object_tagging.assert_not_called()


def test_handler_skips_object_without_portal_accession(portal, set_tags):
    set_tags({"collections": "ENCODE"})

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": []}
    portal.get.assert_not_called()
    portal.patch.assert_not_called()


def test_handler_skips_object_without_collections_tag(portal, set_tags):
    set_tags({"portal_accession": "IGVFFI0001AAAA"})

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": []}
    portal.get.assert_not_called()
    portal.patch.assert_not_called()


def test_handler_skips_empty_collections_tag(portal, set_tags):
    set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": " "})

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": []}
    portal.get.assert_not_called()
    portal.patch.assert_not_called()


def test_handler_patches_new_collections(portal, set_tags, set_portal_collections):
    set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE MaveDB"})
    set_portal_collections(["ENCODE"])

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": []}
    portal.patch.assert_called_once()
    args, kwargs = portal.patch.call_args
    assert args[0].endswith("/IGVFFI0001AAAA")
    assert kwargs["json"] == {"collections": ["ENCODE", "MaveDB"]}


def test_handler_patches_first_collection_when_portal_has_none(
    portal, set_tags, set_portal_collections
):
    set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE"})
    set_portal_collections(None)

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": []}
    assert portal.patch.call_args.kwargs["json"] == {"collections": ["ENCODE"]}


def test_handler_no_patch_when_portal_already_has_collections(
    portal, set_tags, set_portal_collections
):
    set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE"})
    set_portal_collections(["MaveDB", "ENCODE"])

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": []}
    portal.patch.assert_not_called()


def test_handler_decodes_url_encoded_key(s3, portal, set_tags):
    set_tags({})

    index.handler(make_sqs_event(make_s3_body(key="my+folder/file%281%29.txt")), None)

    s3.get_object_tagging.assert_called_once_with(
        Bucket="test-bucket", Key="my folder/file(1).txt"
    )


def test_handler_reports_failed_message_in_batch_failures(s3, portal):
    s3.get_object_tagging.side_effect = RuntimeError("boom")

    result = index.handler(make_sqs_event(make_s3_body()), None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "message-0"}]}


def test_handler_one_bad_message_does_not_fail_others(
    portal, set_tags, set_portal_collections
):
    set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE"})
    set_portal_collections([])
    event = make_sqs_event({"not": "an s3 event"}, make_s3_body())

    result = index.handler(event, None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "message-0"}]}
    portal.patch.assert_called_once()
