# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Bounded public accounting and opaque task-event validation.

Accounting contains operational dispositions, never provider response bodies or
subject identifiers. It does not replace native evidence or review decisions.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional

COUNTS = (
    "planned",
    "started",
    "terminal",
    "completed",
    "errors",
    "timeouts",
    "cancelled",
    "interrupted",
    "unattempted",
    "unknown",
    "observations",
)
STATES = frozenset(
    {
        "running",
        "completed",
        "partial",
        "failed",
        "cancelled",
        "interrupted",
        "unknown",
    }
)
STAGE_STATES = frozenset(
    {
        "pending",
        "not_selected",
        "skipped_no_targets",
        "blocked_dependency",
        "not_started_budget",
        "running",
        "completed",
        "failed",
        "timed_out",
        "cancelled",
        "interrupted",
        "cleanup_incomplete",
        "unknown",
    }
)
ENGINES = frozenset(
    {
        "native",
        "maigret",
        "github",
        "unfurl",
        "wayback",
        "user_scanner_username",
        "user_scanner_email",
    }
)
SOURCE_ENGINES = frozenset(
    {
        "native-profile-search",
        "maigret",
        "github-public-profile",
        "unfurl-url-analysis",
        "wayback-cdx",
        "user-scanner-username",
        "user-scanner",
    }
)
UNITS = frozenset({"queries", "site_checks", "targets", "invocations"})
_CODE = re.compile(r"^[a-zA-Z0-9_-]{1,80}$")
_OPAQUE_ID = re.compile(r"^[a-f0-9]{64}$")


def public_collection_accounting(value: Any) -> Optional[dict]:
    """Fail closed on malformed snapshots and strip non-contract metadata."""
    if not isinstance(value, Mapping) or value.get("schema_version") != 1:
        return None
    revision = value.get("revision")
    if type(revision) is not int or not 0 <= revision <= 10_000_000:
        return None
    if value.get("state") not in STATES or type(value.get("known")) is not bool:
        return None
    stages = value.get("stages")
    if not isinstance(stages, list) or len(stages) > 7:
        return None
    causes = {None, 'operator_cancel', 'job_deadline', 'stage_deadline', 'cleanup_incomplete',
              'persistence_failure', 'lease_lost', 'worker_shutdown'}
    if value.get('stop_cause') not in causes:
        return None
    cleaned = []
    seen = set()
    for row in stages:
        if not isinstance(row, Mapping):
            return None
        if row.get('stop_cause') not in causes:
            return None
        stage_id = row.get("stage_id")
        if stage_id not in ENGINES or stage_id in seen:
            return None
        seen.add(stage_id)
        if (
            row.get("engine_id") not in SOURCE_ENGINES
            or row.get("status") not in STAGE_STATES
        ):
            return None
        if row.get("unit") not in UNITS:
            return None
        reason = row.get("reason")
        if reason is not None and (
            not isinstance(reason, str) or not _CODE.fullmatch(reason)
        ):
            return None
        counts = {key: row.get(key) for key in COUNTS}
        if any(
            v is not None and (type(v) is not int or not 0 <= v <= 10_000_000)
            for v in counts.values()
        ):
            return None
        from maigret.web.collection_orchestration import StageCounts

        try:
            StageCounts(**counts)
        except ValueError:
            return None
        if value['known'] and any(v is None for v in counts.values()):
            return None
        cleaned.append(
            {
                "stage_id": stage_id,
                "engine_id": row["engine_id"],
                "label": stage_id,
                "unit": row["unit"],
                "status": row["status"],
                "reason": reason,
                **({'stop_cause': row['stop_cause']} if 'stop_cause' in row else {}),
                "cleanup_complete": row.get('cleanup_complete', True) is True,
                **counts,
            }
        )
    return {
        "schema_version": 1,
        "revision": revision,
        "state": value["state"],
        "known": value["known"],
        **({'stop_cause': value['stop_cause']} if 'stop_cause' in value else {}),
        "stages": cleaned,
    }


def validate_task_batch(event: Mapping[str, Any]) -> dict:
    """Validate an internal batch before it can enter the durable event log."""
    kind = event.get("type")
    if kind not in {
        "collection_task_plan",
        "collection_task_terminal",
        "collection_task_cleanup",
    }:
        raise ValueError("Invalid collection task event")
    items = event.get("tasks")
    if not isinstance(items, list) or not 1 <= len(items) <= 250:
        raise ValueError("Invalid collection task batch size")
    result = []
    seen = set()
    for item in items:
        if not isinstance(item, Mapping) or item.get("schema_version") != 1:
            raise ValueError("Invalid collection task schema")
        clean = {"schema_version": 1}
        for key in ("task_id", "check_id", "source_id", "target_id"):
            identifier = item.get(key)
            if not isinstance(identifier, str) or not _OPAQUE_ID.fullmatch(identifier):
                raise ValueError("Invalid opaque collection identity")
            clean[key] = identifier
        if clean["task_id"] in seen:
            raise ValueError("Duplicate collection task identity")
        seen.add(clean["task_id"])
        attempt = item.get("attempt")
        if type(attempt) is not int or not 0 <= attempt <= 100:
            raise ValueError("Invalid collection attempt")
        clean["attempt"] = attempt
        if kind == 'collection_task_cleanup':
            if item.get('event_type') != 'cleanup' or item.get('cleanup_state') not in {
                'complete',
                'incomplete',
            }:
                raise ValueError('Invalid collection cleanup transition')
            clean.update(event_type='cleanup', cleanup_state=item['cleanup_state'])
        elif kind == "collection_task_terminal":
            disposition = item.get("disposition")
            if disposition not in {
                "completed",
                "error",
                "timeout",
                "cancelled",
                "interrupted",
                "unknown",
                "unattempted",
            }:
                raise ValueError("Invalid collection disposition")
            if type(item.get("attempted")) is not bool:
                raise ValueError("Invalid collection admission state")
            if (disposition == 'unattempted') != (item['attempted'] is False):
                raise ValueError('Collection disposition conflicts with admission')
            cleanup = item.get("cleanup_state")
            if cleanup not in {"complete", "pending", "incomplete", "not_required"}:
                raise ValueError("Invalid collection cleanup state")
            if disposition == 'unattempted' and cleanup != 'not_required':
                raise ValueError('Unattempted work cannot require cleanup')
            clean.update(
                disposition=disposition,
                attempted=item["attempted"],
                cleanup_state=cleanup,
            )
            reason = item.get("reason")
            if (
                reason is not None
                and isinstance(reason, str)
                and _CODE.fullmatch(reason)
            ):
                clean["reason"] = reason
        result.append(clean)
    stage_id = event.get('stage_id', 'maigret')
    if stage_id not in ENGINES:
        raise ValueError('Invalid collection task stage')
    return {"type": kind, "stage_id": stage_id, "tasks": result}


def interrupted_collection_accounting(value, events, native_document=None):
    """Reconcile saved plans after lease loss without replaying any source.

    An unreturned task may have started before the worker died. Its outcome and
    admission stay unknown; it must not be recast as an unattempted negative.
    """
    snapshot = public_collection_accounting(value)
    if snapshot is None:
        return None
    plans, terminals, cleanup = {}, {}, {}
    for event in events:
        stage = event.get('stage_id', 'maigret')
        kind = event.get('type')
        for task in event.get('tasks', []):
            task_id = task['task_id']
            if kind == 'collection_task_plan':
                plans.setdefault(stage, {})[task_id] = task
            elif kind == 'collection_task_terminal':
                terminals.setdefault(stage, {})[task_id] = task
                cleanup[task_id] = task.get('cleanup_state')
            elif kind == 'collection_task_cleanup':
                cleanup[task_id] = task.get('cleanup_state')
    for row in snapshot['stages']:
        stage = row['stage_id']
        if stage in plans:
            planned = plans[stage]
            terminal = terminals.get(stage, {})
            missing = set(planned).difference(terminal)
            counts = {key: 0 for key in COUNTS}
            counts['planned'] = len(planned)
            counts['observations'] = row['observations']
            for task in terminal.values():
                disposition = task['disposition']
                key = {'error': 'errors', 'timeout': 'timeouts'}.get(
                    disposition, disposition
                )
                counts[key] += 1
                if task['attempted']:
                    counts['started'] += 1
                if disposition in {'completed', 'error', 'timeout', 'cancelled'}:
                    counts['terminal'] += 1
            counts['unknown'] += len(missing)
            if missing:
                counts['started'] = None
            row.update(counts)
            row['cleanup_complete'] = not missing and not any(
                cleanup.get(key) in {'pending', 'incomplete'} for key in planned
            )
        elif stage == 'native' and native_document:
            doc = native_document
            if doc.get('active_query_count', 0):
                row['cleanup_complete'] = False
            executed = doc['executed_query_count']
            interrupted = doc.get('interrupted_query_count', 0) + doc.get(
                'active_query_count', 0
            )
            attempted = executed + interrupted
            row.update(
                planned=doc['planned_query_count'],
                started=attempted,
                terminal=executed,
                completed=executed - doc['error_count'],
                errors=doc['error_count'],
                timeouts=0,
                cancelled=0,
                interrupted=interrupted,
                unattempted=doc['planned_query_count'] - attempted,
                unknown=0,
                observations=doc['candidate_count'],
            )
        if (
            row['status']
            not in {
                'not_selected',
                'skipped_no_targets',
                'completed',
                'failed',
                'timed_out',
                'cancelled',
                'blocked_dependency',
                'not_started_budget',
                'interrupted',
                'cleanup_incomplete',
            }
            or row['unknown']
        ):
            row.update(status='interrupted', reason='worker_lease_lost')
    snapshot.update(
        revision=snapshot['revision'] + 1,
        state='interrupted',
        known=all(row[key] is not None for row in snapshot['stages'] for key in COUNTS),
    )
    return public_collection_accounting(snapshot)
