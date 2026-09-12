"""Transactional limits apply before sending, including redirects and retries.

A permit is consumed before network I/O and is never refunded after an ambiguous
failure. Process death therefore cannot accidentally replenish a request budget.
Provider cooldowns and half-open probes survive process and worker restarts.
"""

from __future__ import annotations

import math
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from sqlalchemy import select, update


class RequestBudgetExceeded(RuntimeError):
    retryable = False

    def __init__(self):
        super().__init__("The saved collection request allowance is exhausted")


class ProviderCooldown(RuntimeError):
    retryable = True

    def __init__(self, provider, seconds):
        self.provider = provider
        self.retry_after_seconds = max(1, math.ceil(seconds))
        super().__init__(
            f"Provider {provider} is cooling down; retry after {self.retry_after_seconds} seconds"
        )


def _now():
    return datetime.now(timezone.utc)


def _aware(value):
    return (
        value.replace(tzinfo=timezone.utc) if value and value.tzinfo is None else value
    )


def retry_after_seconds(value, now=None):
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            seconds = (
                _aware(parsedate_to_datetime(str(value))) - (now or _now())
            ).total_seconds()
        except (TypeError, ValueError, OverflowError):
            return None
    return max(0, seconds) if math.isfinite(seconds) else None


class PipelineRuntimeStore:
    def __init__(self, case_store):
        from maigret.web.pipeline_store import PipelineStore

        self.pipeline = PipelineStore(case_store)
        self.engine = case_store.engine
        self.budgets = self.pipeline._table("request_budgets")
        self.providers = self.pipeline._table("provider_state")

    @contextmanager
    def _transaction(self):
        # SQLite has no row locks: acquire its write reservation before reads.
        with self.engine.connect() as connection:
            if self.engine.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            else:
                connection.begin()
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _insert_once(self, connection, table, values, key):
        if self.engine.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif self.engine.dialect.name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise RuntimeError("Pipeline runtime requires PostgreSQL or SQLite")
        connection.execute(
            insert(table).values(**values).on_conflict_do_nothing(index_elements=[key])
        )

    @staticmethod
    def _provider(provider):
        value = str(provider or "").strip().casefold()
        if not value or len(value) > 200 or any(c in value for c in "/@?\n\r"):
            raise ValueError("A non-secret provider identifier is required")
        return value

    def _provider_row(self, connection, provider):
        provider = self._provider(provider)
        self._insert_once(
            connection,
            self.providers,
            dict(provider=provider, consecutive_failures=0, updated_at=_now()),
            "provider",
        )
        return dict(
            connection.execute(
                select(self.providers)
                .where(self.providers.c.provider == provider)
                .with_for_update()
            )
            .mappings()
            .one()
        )

    def _admit_provider(self, connection, provider):
        row = self._provider_row(connection, provider)
        now = _now()
        until = _aware(row["cooldown_until"])
        if until and until > now:
            raise ProviderCooldown(row["provider"], (until - now).total_seconds())
        token = None
        if until:
            probe_until = _aware(row["probe_until"])
            if probe_until and probe_until > now:
                raise ProviderCooldown(
                    row["provider"], (probe_until - now).total_seconds()
                )
            token = str(uuid.uuid4())
            connection.execute(
                update(self.providers)
                .where(self.providers.c.provider == row["provider"])
                .values(
                    probe_token=token,
                    probe_until=now + timedelta(seconds=120),
                    updated_at=now,
                )
            )
        return token

    def reserve_request(
        self, request_id, attempt_id, worker_id, count=1, provider=None
    ):
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError("Request reservation must be a positive integer")
        with self._transaction() as connection:
            _, task = self.pipeline._attempt_context(connection, attempt_id, worker_id)
            if task["request_id"] != request_id:
                raise ValueError("Request allowance belongs to a different attempt")
            request = self.pipeline._row(
                connection, self.pipeline._table("requests"), request_id
            )
            maximum = int(
                (request["plan"].get("budgets") or {}).get("max_requests", 100000)
            )
            if maximum < 0 or maximum > 100000:
                raise ValueError("Invalid persisted request allowance")
            self._insert_once(
                connection,
                self.budgets,
                dict(
                    request_id=request_id,
                    max_requests=maximum,
                    consumed=0,
                    updated_at=_now(),
                ),
                "request_id",
            )
            row = dict(
                connection.execute(
                    select(self.budgets)
                    .where(self.budgets.c.request_id == request_id)
                    .with_for_update()
                )
                .mappings()
                .one()
            )
            if row["consumed"] + count > row["max_requests"]:
                raise RequestBudgetExceeded()
            token = self._admit_provider(connection, provider) if provider else None
            connection.execute(
                update(self.budgets)
                .where(self.budgets.c.request_id == request_id)
                .values(consumed=row["consumed"] + count, updated_at=_now())
            )
            return {
                "max_requests": row["max_requests"],
                "consumed": row["consumed"] + count,
                "remaining": row["max_requests"] - row["consumed"] - count,
                "provider_permit": token,
            }

    def budget_snapshot(self, request_id):
        with self.engine.connect() as connection:
            request = self.pipeline._row(
                connection, self.pipeline._table("requests"), request_id
            )
            row = (
                connection.execute(
                    select(self.budgets).where(self.budgets.c.request_id == request_id)
                )
                .mappings()
                .first()
            )
            maximum = (
                row["max_requests"]
                if row
                else int(
                    (request["plan"].get("budgets") or {}).get("max_requests", 100000)
                )
            )
            consumed = row["consumed"] if row else 0
            return dict(
                max_requests=maximum, consumed=consumed, remaining=maximum - consumed
            )

    def provider_status(self, provider):
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(self.providers).where(
                        self.providers.c.provider == self._provider(provider)
                    )
                )
                .mappings()
                .first()
            )
        if not row:
            return dict(
                provider=provider, consecutive_failures=0, retry_after_seconds=0
            )
        result = dict(row)
        until = max(
            filter(None, [_aware(row["cooldown_until"]), _aware(row["probe_until"])]),
            default=_now(),
        )
        result["retry_after_seconds"] = max(
            0, math.ceil((until - _now()).total_seconds())
        )
        result.pop("probe_token", None)
        return result

    def record_provider_result(self, provider, outcome, retry_after=None, permit=None):
        with self._transaction() as connection:
            row = self._provider_row(connection, provider)
            # Late in-flight outcomes cannot close/reopen a newer circuit epoch.
            if row["cooldown_until"] and (not permit or permit != row["probe_token"]):
                return self._public_state(row)
            values = dict(updated_at=_now(), last_outcome=outcome)
            delay = retry_after_seconds(retry_after)
            failed = outcome in {
                "error",
                "timeout",
                "rate_limited",
                "blocked",
                "unavailable",
            }
            if failed:
                failures = row["consecutive_failures"] + 1
                values["consecutive_failures"] = failures
                if delay is not None or failures >= 3 or outcome == "rate_limited":
                    values.update(
                        cooldown_until=_now()
                        + timedelta(seconds=max(1, delay if delay is not None else 60)),
                        probe_until=None,
                        probe_token=None,
                    )
            elif outcome in {"found", "candidate", "not_found", "success", "complete"}:
                values.update(
                    consecutive_failures=0,
                    cooldown_until=None,
                    probe_until=None,
                    probe_token=None,
                )
            elif permit:
                values.update(probe_until=None, probe_token=None)
            connection.execute(
                update(self.providers)
                .where(self.providers.c.provider == row["provider"])
                .values(**values)
            )
            return self._public_state(dict(row, **values))

    @staticmethod
    def _public_state(row):
        return {key: value for key, value in row.items() if key != "probe_token"}
