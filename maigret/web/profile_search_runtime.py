"""Runtime resilience controls for native profile search."""

# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from maigret.web.profile_search_backend import (
    ProfileSearchClient,
    ProfileSearchRun,
)
from maigret.web.profile_search_contract import (
    ProfileSearchError,
    ProfileSearchQuery,
)
from maigret.web.provider_circuit_breaker import (
    ProviderCircuitBreaker,
    ProviderCircuitOpen,
    provider_circuits,
)

PROFILE_SEARCH_CIRCUIT_PREFIX = "profile-search"


class GovernedProfileSearchClient:
    """Apply the shared circuit without retries or evidence changes."""

    def __init__(
        self,
        client: ProfileSearchClient,
        *,
        circuit_breaker_enabled: bool,
        circuits: ProviderCircuitBreaker = provider_circuits,
    ) -> None:
        self.client = client
        self.circuit_breaker_enabled = bool(circuit_breaker_enabled)
        self.circuits = circuits
        self.last_circuit_open: Optional[ProviderCircuitOpen] = None

    @property
    def provider(self) -> str:
        return self.client.config.provider

    @property
    def circuit_key(self) -> str:
        return f"{PROFILE_SEARCH_CIRCUIT_PREFIX}:{self.provider}"

    async def search(self, query: ProfileSearchQuery) -> ProfileSearchRun:
        if not self.circuit_breaker_enabled:
            return await self.client.search(query)
        try:
            return await self.circuits.call(
                self.circuit_key,
                lambda: self.client.search(query),
                transient_error=lambda _error: False,
                transient_result=lambda run: bool(
                    isinstance(run, ProfileSearchRun)
                    and run.error is not None
                    and run.error.retryable
                ),
            )
        except ProviderCircuitOpen as error:
            self.last_circuit_open = error
            return ProfileSearchRun(
                query=query,
                provenance=None,
                evidence=(),
                error=ProfileSearchError(
                    query_id=query.query_id,
                    provider=self.provider,
                    code="circuit_open",
                    message="Search provider circuit is open.",
                    retryable=True,
                    occurred_at=datetime.now(timezone.utc),
                ),
            )
