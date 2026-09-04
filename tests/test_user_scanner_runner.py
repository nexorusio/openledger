import sys
from types import ModuleType, SimpleNamespace

from maigret.web import user_scanner_runner


class _FakeResult:
    def __init__(self, username, site_name):
        self.payload = {
            "status": "Found",
            "reason": "",
            "username": username,
            "site_name": site_name,
            "category": "Social",
            "url": f"https://example.test/{username}",
            "extra": {},
            "media": {},
        }

    def update(self, **kwargs):
        self.payload["extra"].update(kwargs.get("extra") or {})
        return self

    def to_dict(self):
        return dict(self.payload)


def _install_fake_user_scanner(monkeypatch, captured):
    package = ModuleType("user_scanner")
    package.__path__ = []
    core = ModuleType("user_scanner.core")
    core.__path__ = []
    cross_scan = ModuleType("user_scanner.core.cross_scan")
    helpers = ModuleType("user_scanner.core.helpers")
    orchestrator = ModuleType("user_scanner.core.orchestrator")

    class CrossScanConfig(SimpleNamespace):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            captured["cross_config"] = kwargs

    def find_module(platform, **_kwargs):
        captured.setdefault("find_module", []).append(platform)
        return [platform]

    def run_user_module(modules, username, _config):
        captured["direct_modules"] = list(modules)
        return [_FakeResult(username, module.title()) for module in modules]

    def run_cross_scan(_results, _config, _cross_config):
        captured["cross_scan_called"] = True
        return []

    cross_scan.CrossScanConfig = CrossScanConfig
    cross_scan.run_cross_scan = run_cross_scan
    helpers.ScanConfig = SimpleNamespace
    helpers.find_module = find_module
    helpers.set_global_timeout = lambda timeout: captured.update(timeout=timeout)
    orchestrator.run_user_module = run_user_module
    orchestrator.set_concurrency = lambda value: captured.update(concurrency=value)

    for name, module in {
        "user_scanner": package,
        "user_scanner.core": core,
        "user_scanner.core.cross_scan": cross_scan,
        "user_scanner.core.helpers": helpers,
        "user_scanner.core.orchestrator": orchestrator,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_x_module_is_not_imported_or_run_without_explicit_vxtwitter_consent(
    monkeypatch,
):
    captured = {}
    _install_fake_user_scanner(monkeypatch, captured)

    results = user_scanner_runner._scan_usernames(
        {
            "usernames": ["alice"],
            "platforms": ["facebook", "instagram", "threads", "tiktok", "x"],
            "allow_vxtwitter": False,
        }
    )

    assert captured["find_module"] == [
        "facebook",
        "instagram",
        "threads",
        "tiktok",
    ]
    assert captured["direct_modules"] == captured["find_module"]
    assert captured["cross_config"] == {
        "links": "all",
        "modules": ("facebook", "instagram", "threads", "tiktok"),
        "emails": "none",
        "sweep": 0,
        "depth": 1,
    }
    assert captured["cross_scan_called"] is True
    policy_result = results[-1]
    assert policy_result["site_name"] == "X (Twitter)"
    assert policy_result["status"] == "Skipped"
    assert "api.vxtwitter.com" in policy_result["reason"]


def test_x_module_requires_explicit_vxtwitter_consent(monkeypatch):
    captured = {}
    _install_fake_user_scanner(monkeypatch, captured)

    results = user_scanner_runner._scan_usernames(
        {
            "usernames": ["alice"],
            "platforms": ["x"],
            "allow_vxtwitter": True,
        }
    )

    assert captured["find_module"] == ["x"]
    assert len(results) == 1
    assert results[0]["site_name"] == "X"
