"""Isolated collector adapters and OpenLedger observation normalization."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from maigret.web.persona_intelligence import (
    claim_fingerprint,
    evidence_fingerprint,
)

USER_SCANNER_ENGINE = "user_scanner_email"
USER_SCANNER_TIMEOUT_SECONDS = 420
MAX_COLLECTOR_OUTPUT_BYTES = 8_000_000
MAX_OBSERVATIONS = 600


def user_scanner_available() -> bool:
    """Check availability without importing User Scanner into the web process."""
    return importlib.util.find_spec("user_scanner") is not None


async def _stop_subprocess(
    process: asyncio.subprocess.Process, communicate_task: asyncio.Task
) -> None:
    communicate_task.cancel()
    await asyncio.gather(communicate_task, return_exceptions=True)
    if process.returncode is not None:
        return
    try:
        process.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


def user_scanner_email_targets(plan: Any) -> List[str]:
    """Return explicitly enabled email targets that can bind to one Persona."""
    if not isinstance(plan, dict):
        return []
    if not plan.get("enable_user_scanner_email"):
        return []
    if plan.get("processing_mode") != "same_subject":
        return []
    targets: List[str] = []
    for identifier in list(plan.get("identifiers") or []):
        if not isinstance(identifier, dict) or identifier.get("type") != "email":
            continue
        value = str(identifier.get("value") or "").strip().casefold()
        if value and value not in targets:
            targets.append(value)
    return targets[:1]


def _safe_public_url(value: Any) -> str:
    candidate = str(value or "").strip()
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return candidate[:2000]


def _bounded_mapping(value: Any, *, limit: int = 40) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    output: Dict[str, Any] = {}
    for raw_key, raw_value in list(value.items())[:limit]:
        key = str(raw_key).strip()[:100]
        if not key or isinstance(raw_value, (dict, list, tuple, set)):
            continue
        if isinstance(raw_value, (str, int, float, bool)):
            output[key] = str(raw_value)[:2000] if isinstance(raw_value, str) else raw_value
    return output


def normalize_user_scanner_results(
    target_email: str, raw_results: Any
) -> List[Dict[str, Any]]:
    """Validate and bound native User Scanner results before persistence."""
    if not isinstance(raw_results, list):
        raise ValueError("User Scanner returned a non-list result")
    observations: List[Dict[str, Any]] = []
    for raw in raw_results[:MAX_OBSERVATIONS]:
        if not isinstance(raw, dict):
            continue
        status = str(raw.get("status") or "Error").strip()[:40]
        site_name = str(raw.get("site_name") or "Unknown source").strip()[:300]
        if not site_name:
            site_name = "Unknown source"
        observations.append(
            {
                "source_engine": USER_SCANNER_ENGINE,
                "subject_type": "email",
                "subject_value": target_email[:254],
                "status": status,
                "site_name": site_name,
                "category": str(raw.get("category") or "").strip()[:100],
                "source_url": _safe_public_url(raw.get("url")),
                "reason": str(raw.get("reason") or "").strip()[:1000],
                "extra": _bounded_mapping(raw.get("extra")),
                "media": {
                    key: url
                    for key, value in _bounded_mapping(raw.get("media"), limit=12).items()
                    if (url := _safe_public_url(value))
                },
            }
        )
    return observations


async def run_user_scanner_email(
    target_email: str,
    *,
    timeout_seconds: int = USER_SCANNER_TIMEOUT_SECONDS,
    cancellation_check: Optional[Callable[[], bool]] = None,
) -> List[Dict[str, Any]]:
    """Run User Scanner outside the worker process and parse its JSON envelope."""
    if not user_scanner_available():
        raise RuntimeError("User Scanner is not installed in the worker image")
    if cancellation_check and cancellation_check():
        raise asyncio.CancelledError

    environment = dict(os.environ)
    environment.update(NO_COLOR="1", TERM="dumb")
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "maigret.web.user_scanner_runner",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=environment,
    )
    request_payload = json.dumps({"email": target_email}).encode("utf-8")
    communicate_task = asyncio.create_task(process.communicate(request_payload))
    started_at = asyncio.get_running_loop().time()
    try:
        while True:
            done, _ = await asyncio.wait({communicate_task}, timeout=0.5)
            if done:
                stdout, stderr = communicate_task.result()
                break
            if cancellation_check and cancellation_check():
                raise asyncio.CancelledError
            if asyncio.get_running_loop().time() - started_at > timeout_seconds:
                raise asyncio.TimeoutError
    except asyncio.CancelledError:
        await _stop_subprocess(process, communicate_task)
        raise
    except asyncio.TimeoutError:
        await _stop_subprocess(process, communicate_task)
        raise RuntimeError("User Scanner exceeded its collection timeout") from None

    if process.returncode != 0:
        diagnostic = stderr.decode("utf-8", errors="replace").strip()[-2000:]
        raise RuntimeError(f"User Scanner failed: {diagnostic or 'unknown error'}")
    if len(stdout) > MAX_COLLECTOR_OUTPUT_BYTES:
        raise RuntimeError("User Scanner returned an oversized result")
    try:
        envelope = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("User Scanner returned invalid JSON") from exc
    if not isinstance(envelope, dict) or envelope.get("schema_version") != 1:
        raise RuntimeError("User Scanner returned an unsupported result schema")
    return normalize_user_scanner_results(target_email, envelope.get("results"))


def extract_user_scanner_claims(
    observations: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convert positive email-registration observations into pending claims."""
    candidates: List[Dict[str, Any]] = []
    for observation in observations:
        if not isinstance(observation, dict):
            continue
        if observation.get("source_engine") != USER_SCANNER_ENGINE:
            continue
        if str(observation.get("status") or "").casefold() != "registered":
            continue
        email = str(observation.get("subject_value") or "").strip().casefold()
        site_name = str(observation.get("site_name") or "Unknown source").strip()[:300]
        if not email or not site_name:
            continue
        source_url = _safe_public_url(observation.get("source_url"))
        value = {
            "platform": site_name,
            "email": email,
            "url": source_url,
        }
        normalized_value = f"{site_name.casefold()}\0{email}"
        fingerprint = claim_fingerprint("account_registration", value)
        evidence = {
            "evidence_type": "email_registration_probe",
            "source_name": site_name,
            "source_url": source_url,
            "details": {
                "subject_type": "email",
                "subject_value": email,
                "native_status": "Registered",
                "category": str(observation.get("category") or "")[:100],
                "extra": _bounded_mapping(observation.get("extra")),
                "media": _bounded_mapping(observation.get("media"), limit=12),
            },
        }
        candidates.append(
            {
                "field_name": "account_registration",
                "value": value,
                "display_value": f"{site_name}: {email}"[:4000],
                "normalized_value": normalized_value[:4000],
                # The probe supports registration, not ownership by the Persona.
                "confidence": 55,
                "fingerprint": fingerprint,
                "source_engine": USER_SCANNER_ENGINE,
                # User Scanner does not expose a native record identifier, so use
                # the stable claim fingerprint instead of retaining another copy
                # of the email address in the lineage index.
                "source_record_id": f"{USER_SCANNER_ENGINE}:{fingerprint}",
                "native_status": "registered",
                "evidence": [
                    dict(evidence, fingerprint=evidence_fingerprint(evidence))
                ],
            }
        )
    return candidates
