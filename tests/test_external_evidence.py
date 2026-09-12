import math

import pytest

from maigret.web import external_evidence
from maigret.web.external_evidence import (
    ExternalEvidenceValidationError,
    normalize_bounded_document,
    normalize_external_evidence,
)


def evidence_envelope(**overrides):
    payload = {
        "schema_version": 1,
        "source_id": "client.datamart",
        "source_record_id": "record-42",
        "source_version": "2026-08-30T12:00:00Z",
        "record_type": "identity.observation",
        "content_hash": f"sha256:{'a' * 64}",
        "observed_at": "2026-08-30T12:00:00Z",
        "validity": {
            "from": "2026-01-01T00:00:00+00:00",
            "to": "2026-12-31T23:59:59+00:00",
        },
        "handling": {
            "classification": "restricted",
            "authority": "client-alpha",
            "policy_tags": ["case-use-only"],
        },
        "locator": {"uri": "datamart://client-alpha/record-42/v1"},
        "attributes": {"kind": "structured", "score": 0.82},
        "preview": "Bounded, redacted analyst preview.",
    }
    payload.update(overrides)
    return payload


def test_external_evidence_contract_normalizes_a_valid_envelope():
    normalized = normalize_external_evidence(evidence_envelope())

    assert normalized["source_id"] == "client.datamart"
    assert normalized["content_hash"].startswith("sha256:")
    assert normalized["observed_at"].utcoffset().total_seconds() == 0
    assert normalized["locator"] == {
        "uri": "datamart://client-alpha/record-42/v1"
    }


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"schema_version": 2}, "Unsupported external evidence schema"),
        ({"unexpected": "value"}, "unsupported fields"),
        ({"observed_at": "2026-08-30T12:00:00"}, "include a timezone"),
        ({"content_hash": "sha256:not-a-hash"}, "content_hash"),
        ({"locator": {"uri": "file:///etc/passwd"}}, "Unsupported"),
        ({"locator": {"uri": "http://example.test/record"}}, "Unsupported"),
        (
            {"locator": {"uri": "https://example.test/item?access_token=secret"}},
            "query strings",
        ),
        (
            {
                "validity": {
                    "from": "2026-12-01T00:00:00Z",
                    "to": "2026-01-01T00:00:00Z",
                }
            },
            "must not follow",
        ),
    ],
)
def test_external_evidence_contract_rejects_unsafe_payloads(overrides, message):
    with pytest.raises(ExternalEvidenceValidationError, match=message):
        normalize_external_evidence(evidence_envelope(**overrides))


def test_bounded_documents_reject_credentials_and_resource_exhaustion_values():
    for credential_key in (
        "client_secret",
        "clientSecret",
        "apiKey",
        "APIKey",
        "db_password",
        "dbpassword",
        "dbpasswordvalue",
        "mysecretvalue",
        "passwordvalue",
        "secretvalue",
        "serviceApiKeyValue",
        "authorizationHeader",
    ):
        with pytest.raises(
            ExternalEvidenceValidationError,
            match="credential field",
        ):
            normalize_bounded_document(
                {"nested": {credential_key: "do-not-store"}}, "document"
            )
    with pytest.raises(ExternalEvidenceValidationError, match="non-finite"):
        normalize_bounded_document({"score": math.inf}, "document")
    with pytest.raises(ExternalEvidenceValidationError, match="maximum depth"):
        normalize_bounded_document(
            {"a": {"b": {"c": {"d": {"e": {"f": {"g": 1}}}}}}},
            "document",
        )
    with pytest.raises(ExternalEvidenceValidationError, match="control characters"):
        normalize_bounded_document({"value": "safe\x00unsafe"}, "document")


@pytest.mark.parametrize(
    "credential_key",
    [
        "serviceAPIKeyValue",
        "service.API-KEY:value",
        "API__Key",
        "APIKey",
        "authorizationHEADER",
        "SESSIONTokenValue",
        "connectionSTRING",
        "PRIVATEkey",
        "paßword",
    ],
)
def test_compact_credential_keys_remain_rejected(credential_key):
    with pytest.raises(ExternalEvidenceValidationError, match="credential field"):
        normalize_bounded_document({credential_key: "do-not-store"}, "document")


@pytest.mark.parametrize(
    "key", ["HTTPStatus", "APIResult", "public_key", "sourceID", "A" * 100]
)
def test_noncredential_keys_remain_accepted(key):
    assert normalize_bounded_document({key: "safe"}, "document") == {key: "safe"}


def test_document_key_limit_applies_before_credential_scan(monkeypatch):
    def unexpected_scan(_key):
        pytest.fail("An oversized key reached credential scanning")

    monkeypatch.setattr(external_evidence, "_secret_key", unexpected_scan)
    with pytest.raises(ExternalEvidenceValidationError, match="key is too large"):
        normalize_bounded_document({"A" * 10_000: "safe"}, "document")


@pytest.mark.parametrize("invalid_value", [None, [], "", 0, False])
@pytest.mark.parametrize("field_name", ["validity", "attributes"])
def test_external_evidence_rejects_explicit_non_object_optional_fields(
    field_name, invalid_value
):
    with pytest.raises(ExternalEvidenceValidationError, match=field_name):
        normalize_external_evidence(evidence_envelope(**{field_name: invalid_value}))


@pytest.mark.parametrize("invalid_value", [[], "", 0, False])
def test_external_evidence_rejects_falsey_non_timestamp_validity_values(
    invalid_value,
):
    with pytest.raises(ExternalEvidenceValidationError, match="validity.from"):
        normalize_external_evidence(evidence_envelope(validity={"from": invalid_value}))
