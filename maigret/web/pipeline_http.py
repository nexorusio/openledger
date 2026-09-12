"""Compatibility transport guard inside a single supervised collector process.

New connectors may use ordinary httpx/aiohttp clients. The guard meters the
physical send boundary, so redirects and library retries each consume a permit.
It is process-scoped, including library worker threads, and is never installed
in the Flask server or orchestration worker. It is not a sandbox for untrusted
connector code; executable connectors remain reviewed application dependencies.
"""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from urllib.parse import urljoin, urlsplit
from unittest.mock import patch

from maigret.web.pipeline_runtime import (
    PipelineRuntimeStore,
    ProviderCooldown,
    RequestBudgetExceeded,
)

_active_guard = None


def current_runtime_payload():
    return _active_guard.payload if _active_guard else None


def inherit_runtime_status(result):
    if _active_guard and isinstance(result, dict) and result.get("error_code"):
        _active_guard.stop_reason = result["error_code"]
        _active_guard.retry_after = result.get("retry_after_seconds")


class TransportGuard:
    def __init__(self, store, request_id, attempt_id, worker_id):
        self.runtime = PipelineRuntimeStore(store)
        self.request_id, self.attempt_id, self.worker_id = (
            request_id,
            attempt_id,
            worker_id,
        )
        self.payload = dict(
            database_url=store.engine.url.render_as_string(hide_password=False),
            request_id=request_id,
            attempt_id=attempt_id,
            worker_id=worker_id,
        )
        self.stop_reason = None
        self.retry_after = None

    def reserve(self, provider):
        provider = str(provider or "").lower()
        try:
            return self.runtime.reserve_request(
                self.request_id, self.attempt_id, self.worker_id, provider=provider
            )["provider_permit"]
        except RequestBudgetExceeded:
            self.stop_reason = "request_budget_exhausted"
            raise
        except ProviderCooldown as exc:
            self.stop_reason = self.stop_reason or "provider_cooldown"
            self.retry_after = exc.retry_after_seconds
            raise

    def record(self, provider, permit, status=None, headers=None, error=False):
        outcome = (
            "timeout"
            if error
            else (
                "rate_limited"
                if status == 429
                else "error" if status and status >= 500 else "success"
            )
        )
        self.runtime.record_provider_result(
            provider,
            outcome,
            retry_after=(headers or {}).get("Retry-After")
            or (headers or {}).get("retry-after"),
            permit=permit,
        )

    def finish(self, result):
        result = dict(result or {})
        result["request_usage"] = self.runtime.budget_snapshot(self.request_id)
        if self.stop_reason:
            # Legacy libraries sometimes swallow transport exceptions. The outer
            # attempt still reports that collection was interrupted by a limit.
            result.update(
                completeness="partial",
                error_code=self.stop_reason,
                retryable=self.stop_reason == "provider_cooldown",
            )
            if result.get("outcome") not in {"found", "candidate", "partial"}:
                result["outcome"] = (
                    "error"
                    if self.stop_reason == "provider_cooldown"
                    else "inconclusive"
                )
            if self.retry_after:
                result["retry_after_seconds"] = self.retry_after
        return result

    @contextmanager
    def install(self):
        global _active_guard
        if _active_guard is not None:
            raise RuntimeError("Only one collector transport guard may own a process")
        _active_guard = self
        try:
            with ExitStack() as stack:
                self._httpx(stack)
                self._aiohttp(stack)
                self._urllib3(stack)
                self._curl(stack)
                self._dns_collection(stack)
                yield self
        finally:
            _active_guard = None

    def _httpx(self, stack):
        try:
            import httpx
        except ImportError:
            return
        original_sync = httpx.Client._send_single_request
        original_async = httpx.AsyncClient._send_single_request
        guard = self

        def sync(client, request):
            provider = request.url.host
            permit = guard.reserve(provider)
            try:
                response = original_sync(client, request)
            except Exception:
                guard.record(provider, permit, error=True)
                raise
            guard.record(provider, permit, response.status_code, response.headers)
            return response

        async def asynchronous(client, request):
            provider = request.url.host
            permit = guard.reserve(provider)
            try:
                response = await original_async(client, request)
            except Exception:
                guard.record(provider, permit, error=True)
                raise
            guard.record(provider, permit, response.status_code, response.headers)
            return response

        stack.enter_context(patch.object(httpx.Client, "_send_single_request", sync))
        stack.enter_context(
            patch.object(httpx.AsyncClient, "_send_single_request", asynchronous)
        )

    def _aiohttp(self, stack):
        try:
            import aiohttp
        except ImportError:
            return
        original_send = aiohttp.ClientRequest.send
        original_start = aiohttp.ClientResponse.start
        guard = self

        async def send(request, connection):
            provider = request.url.host
            permit = guard.reserve(provider)
            try:
                response = await original_send(request, connection)
            except Exception:
                guard.record(provider, permit, error=True)
                raise
            response._pipeline_provider_permit = (provider, permit)
            return response

        async def start(response, connection, *args, **kwargs):
            permit = getattr(response, "_pipeline_provider_permit", None)
            try:
                result = await original_start(response, connection, *args, **kwargs)
            except Exception:
                if permit:
                    guard.record(*permit, error=True)
                raise
            if permit:
                guard.record(*permit, response.status, response.headers)
            return result

        stack.enter_context(patch.object(aiohttp.ClientRequest, "send", send))
        stack.enter_context(patch.object(aiohttp.ClientResponse, "start", start))

    def _urllib3(self, stack):
        try:
            import urllib3
        except ImportError:
            return
        original = urllib3.connectionpool.HTTPConnectionPool._make_request
        guard = self

        def make_request(pool, connection, method, url, *args, **kwargs):
            provider = pool.host
            permit = guard.reserve(provider)
            try:
                response = original(pool, connection, method, url, *args, **kwargs)
            except (RequestBudgetExceeded, ProviderCooldown):
                raise
            except Exception:
                guard.record(provider, permit, error=True)
                raise
            guard.record(provider, permit, response.status, response.headers)
            return response

        stack.enter_context(
            patch.object(
                urllib3.connectionpool.HTTPConnectionPool, "_make_request", make_request
            )
        )

    def _dns_collection(self, stack):
        from maigret.checking import AiodnsDomainResolver

        original = AiodnsDomainResolver.check
        guard = self

        async def check(resolver):
            permit = guard.reserve("dns-collection")
            try:
                result = await original(resolver)
            except Exception:
                guard.record("dns-collection", permit, error=True)
                raise
            guard.record(
                "dns-collection", permit, status=result[1], error=bool(result[2])
            )
            return result

        stack.enter_context(patch.object(AiodnsDomainResolver, "check", check))

    def _curl(self, stack):
        try:
            from curl_cffi.requests import Session, AsyncSession
        except ImportError:
            return
        guard = self

        def redirect(client, method, url, options, response):
            location = response.headers.get("Location") or response.headers.get(
                "location"
            )
            if response.status_code not in {301, 302, 303, 307, 308} or not location:
                return None
            target = urljoin(url, location)
            if urlsplit(target).scheme not in {"http", "https"}:
                raise ValueError("Unsupported connector redirect scheme")
            if urlsplit(url).scheme == "https" and urlsplit(target).scheme == "http":
                raise ValueError("Connector redirects cannot downgrade HTTPS to HTTP")
            options = dict(options)
            options.pop("params", None)
            if (
                response.status_code == 303
                and method.upper() != "HEAD"
                or response.status_code in {301, 302}
                and method.upper() == "POST"
            ):
                method = "GET"
                for key in ("data", "content", "json", "files", "multipart"):
                    options.pop(key, None)
                headers = dict(options.get("headers") or {})
                options["headers"] = {
                    key: value
                    for key, value in headers.items()
                    if str(key).lower()
                    not in {"content-length", "content-type", "transfer-encoding"}
                }
                options["headers"].update(
                    {
                        "Content-Length": None,
                        "Content-Type": None,
                        "Transfer-Encoding": None,
                    }
                )
            if urlsplit(url).netloc != urlsplit(target).netloc:
                if (
                    client.auth
                    or client.params
                    or any(
                        str(key).lower() in {"authorization", "cookie"}
                        for key in client.headers
                    )
                ):
                    raise ValueError(
                        "Cross-host redirect with session credentials requires explicit connector handling"
                    )
                options.pop("auth", None)
                options.pop("cookies", None)
                if options.get("headers"):
                    options["headers"] = {
                        key: value
                        for key, value in dict(options["headers"]).items()
                        if str(key).lower() not in {"authorization", "cookie", "host"}
                    }
            return method, target, options

        def make_sync(original):
            def request(client, method, url, **kwargs):
                follow = kwargs.pop("allow_redirects", None)
                follow = client.allow_redirects if follow is None else follow
                maximum = kwargs.pop("max_redirects", None)
                maximum = client.max_redirects if maximum is None else maximum
                maximum = 30 if maximum is None or maximum < 0 else maximum
                options = dict(kwargs, allow_redirects=False)
                history = []
                while True:
                    provider = urlsplit(str(url)).hostname
                    permit = guard.reserve(provider)
                    try:
                        response = original(client, method, url, **options)
                    except Exception:
                        guard.record(provider, permit, error=True)
                        raise
                    guard.record(
                        provider, permit, response.status_code, response.headers
                    )
                    next_request = (
                        redirect(client, method, str(url), options, response)
                        if follow
                        else None
                    )
                    if not next_request:
                        response.history = history
                        return response
                    if len(history) >= maximum:
                        raise ValueError("Connector redirect ceiling exceeded")
                    history.append(response)
                    method, url, options = next_request

            return request

        def make_async(original):
            async def request(client, method, url, **kwargs):
                follow = kwargs.pop("allow_redirects", None)
                follow = client.allow_redirects if follow is None else follow
                maximum = kwargs.pop("max_redirects", None)
                maximum = client.max_redirects if maximum is None else maximum
                maximum = 30 if maximum is None or maximum < 0 else maximum
                options = dict(kwargs, allow_redirects=False)
                history = []
                while True:
                    provider = urlsplit(str(url)).hostname
                    permit = guard.reserve(provider)
                    try:
                        response = await original(client, method, url, **options)
                    except Exception:
                        guard.record(provider, permit, error=True)
                        raise
                    guard.record(
                        provider, permit, response.status_code, response.headers
                    )
                    next_request = (
                        redirect(client, method, str(url), options, response)
                        if follow
                        else None
                    )
                    if not next_request:
                        response.history = history
                        return response
                    if len(history) >= maximum:
                        raise ValueError("Connector redirect ceiling exceeded")
                    history.append(response)
                    method, url, options = next_request

            return request

        sync_boundary = (
            "_request_once" if hasattr(Session, "_request_once") else "request"
        )
        async_boundary = (
            "_request_once" if hasattr(AsyncSession, "_request_once") else "request"
        )
        stack.enter_context(
            patch.object(
                Session, sync_boundary, make_sync(getattr(Session, sync_boundary))
            )
        )
        stack.enter_context(
            patch.object(
                AsyncSession,
                async_boundary,
                make_async(getattr(AsyncSession, async_boundary)),
            )
        )


@contextmanager
def inherited_transport_guard(payload):
    """Receive runtime linkage from the supervisor's anonymous input pipe."""
    if payload is None:
        yield None
        return
    from maigret.web.case_store import CaseStore

    store = CaseStore(payload["database_url"])
    try:
        with TransportGuard(
            store, payload["request_id"], payload["attempt_id"], payload["worker_id"]
        ).install() as guard:
            yield guard
    finally:
        store.dispose()
