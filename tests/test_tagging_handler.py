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
def mocked_aws(monkeypatch):
    """Mock S3, portal HTTP calls, and secret lookup; return the mocks."""
    s3 = MagicMock()
    monkeypatch.setattr(index, "s3_client", s3)

    requests_mock = MagicMock()
    monkeypatch.setattr(index, "requests", requests_mock)

    monkeypatch.setattr(index, "get_portal_auth", lambda: ("key", "secret"))

    def set_tags(tags: Dict[str, str]) -> None:
        s3.get_object_tagging.return_value = {"TagSet": make_tag_set(tags)}

    def set_portal_collections(collections: Optional[List[str]]) -> None:
        response = MagicMock()
        response.json.return_value = (
            {} if collections is None else {"collections": collections}
        )
        requests_mock.get.return_value = response

    return s3, requests_mock, set_tags, set_portal_collections


class TestParseCollectionsTag:
    def test_splits_on_whitespace(self):
        assert index.parse_collections_tag("a b c") == ["a", "b", "c"]

    def test_drops_empty_entries(self):
        assert index.parse_collections_tag("a  b ") == ["a", "b"]

    def test_empty_value(self):
        assert index.parse_collections_tag("") == []


class TestResolveCollections:
    def test_returns_none_when_nothing_new(self):
        assert index.resolve_collections(["a"], ["a", "b"]) is None

    def test_merges_preserving_portal_order(self):
        assert index.resolve_collections(["c", "a"], ["b", "a"]) == ["b", "a", "c"]

    def test_first_collection_on_empty_portal(self):
        assert index.resolve_collections(["a"], []) == ["a"]

    def test_appends_new_entries_sorted(self):
        assert index.resolve_collections(["z", "c"], ["a"]) == ["a", "c", "z"]


class TestHandler:
    def test_skips_s3_test_event(self, mocked_aws):
        s3, _, _, _ = mocked_aws
        event = make_sqs_event({"Event": "s3:TestEvent"})

        result = index.handler(event, None)

        assert result == {"batchItemFailures": []}
        s3.get_object_tagging.assert_not_called()

    def test_skips_object_without_portal_accession(self, mocked_aws):
        _, requests_mock, set_tags, _ = mocked_aws
        set_tags({"collections": "ENCODE"})

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": []}
        requests_mock.get.assert_not_called()
        requests_mock.patch.assert_not_called()

    def test_skips_object_without_collections_tag(self, mocked_aws):
        _, requests_mock, set_tags, _ = mocked_aws
        set_tags({"portal_accession": "IGVFFI0001AAAA"})

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": []}
        requests_mock.get.assert_not_called()
        requests_mock.patch.assert_not_called()

    def test_skips_empty_collections_tag(self, mocked_aws):
        _, requests_mock, set_tags, _ = mocked_aws
        set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": " "})

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": []}
        requests_mock.get.assert_not_called()
        requests_mock.patch.assert_not_called()

    def test_patches_new_collections(self, mocked_aws):
        _, requests_mock, set_tags, set_portal_collections = mocked_aws
        set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE MaveDB"})
        set_portal_collections(["ENCODE"])

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": []}
        requests_mock.patch.assert_called_once()
        args, kwargs = requests_mock.patch.call_args
        assert args[0].endswith("/IGVFFI0001AAAA")
        assert kwargs["json"] == {"collections": ["ENCODE", "MaveDB"]}

    def test_patches_first_collection_when_portal_has_none(self, mocked_aws):
        _, requests_mock, set_tags, set_portal_collections = mocked_aws
        set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE"})
        set_portal_collections(None)

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": []}
        assert requests_mock.patch.call_args.kwargs["json"] == {
            "collections": ["ENCODE"]
        }

    def test_no_patch_when_portal_already_has_collections(self, mocked_aws):
        _, requests_mock, set_tags, set_portal_collections = mocked_aws
        set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE"})
        set_portal_collections(["MaveDB", "ENCODE"])

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": []}
        requests_mock.patch.assert_not_called()

    def test_url_encoded_key_is_decoded(self, mocked_aws):
        s3, _, set_tags, _ = mocked_aws
        set_tags({})

        index.handler(make_sqs_event(make_s3_body(key="my+folder/file%281%29.txt")), None)

        s3.get_object_tagging.assert_called_once_with(
            Bucket="test-bucket", Key="my folder/file(1).txt"
        )

    def test_failed_message_reported_in_batch_failures(self, mocked_aws):
        s3, _, _, _ = mocked_aws
        s3.get_object_tagging.side_effect = RuntimeError("boom")

        result = index.handler(make_sqs_event(make_s3_body()), None)

        assert result == {"batchItemFailures": [{"itemIdentifier": "message-0"}]}

    def test_one_bad_message_does_not_fail_others(self, mocked_aws):
        _, requests_mock, set_tags, set_portal_collections = mocked_aws
        set_tags({"portal_accession": "IGVFFI0001AAAA", "collections": "ENCODE"})
        set_portal_collections([])
        event = make_sqs_event({"not": "an s3 event"}, make_s3_body())

        result = index.handler(event, None)

        assert result == {"batchItemFailures": [{"itemIdentifier": "message-0"}]}
        requests_mock.patch.assert_called_once()
