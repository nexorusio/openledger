import pytest

from maigret.web.profile_discovery_policy import (
    PROFILE_DISCOVERY_POLICY_VERSION,
    ProfileDiscoveryPolicyError,
    govern_profile_discovery_options,
    profile_discovery_flags,
)


def test_existing_flags_default_on_but_new_outbound_features_default_off():
    assert profile_discovery_flags(environ={}) == {
        "profile_discovery_enabled": True,
        "focused_mode_enabled": True,
        "exhaustive_mode_enabled": True,
        "maigret_enabled": True,
        "user_scanner_enabled": True,
        "enrichment_providers_enabled": True,
        "provider_circuit_breakers_enabled": True,
        "governed_pivots_enabled": False,
        "search_first_enabled": False,
    }


def test_only_explicit_false_values_disable_server_flags():
    assert profile_discovery_flags(
        environ={"OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED": "off"}
    )["exhaustive_mode_enabled"] is False
    assert profile_discovery_flags(
        environ={"OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED": "typo"}
    )["exhaustive_mode_enabled"] is True


@pytest.mark.parametrize("value", ["1", "true", "YES", "on"])
def test_search_first_rollout_requires_an_explicit_true_value(value):
    assert profile_discovery_flags(
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": value}
    )["search_first_enabled"] is True


@pytest.mark.parametrize("value", ["", "0", "false", "typo"])
def test_search_first_rollout_fails_closed(value):
    assert profile_discovery_flags(
        environ={"OPENLEDGER_SEARCH_FIRST_DISCOVERY_ENABLED": value}
    )["search_first_enabled"] is False


def test_legacy_modes_receive_canonical_server_policy():
    focused = govern_profile_discovery_options({}, "fast", environ={})
    exhaustive = govern_profile_discovery_options({}, "full", environ={})

    assert focused["execution_mode"] == "focused"
    assert focused["all_sites"] is False
    assert exhaustive["execution_mode"] == "exhaustive"
    assert exhaustive["all_sites"] is True
    assert (
        exhaustive["profile_discovery_policy"]["policy_version"]
        == PROFILE_DISCOVERY_POLICY_VERSION
    )


def test_client_cannot_forge_flags_or_disable_safety_controls():
    governed = govern_profile_discovery_options(
        {
            "execution_mode": "focused",
            "execution_budget": {"total_seconds": 999_999_999},
            "profile_discovery_policy": {
                "policy_version": "client",
                "flags": {
                    "provider_circuit_breakers_enabled": False,
                    "search_first_enabled": True,
                },
                "safety_controls": {"durable_cancellation": False},
            },
        },
        environ={},
    )

    policy = governed["profile_discovery_policy"]
    assert policy["policy_version"] == PROFILE_DISCOVERY_POLICY_VERSION
    assert policy["flags"]["provider_circuit_breakers_enabled"] is True
    assert policy["flags"]["search_first_enabled"] is False
    assert policy["safety_controls"]["durable_cancellation"] is True
    assert governed["execution_budget"]["total_seconds"] == 600


@pytest.mark.parametrize(
    ("environment", "message"),
    [
        (
            {"OPENLEDGER_PROFILE_DISCOVERY_ENABLED": "false"},
            "Profile discovery",
        ),
        (
            {"OPENLEDGER_FOCUSED_DISCOVERY_ENABLED": "0"},
            "Focused",
        ),
        (
            {"OPENLEDGER_EXHAUSTIVE_DISCOVERY_ENABLED": "no"},
            "Exhaustive",
        ),
        (
            {"OPENLEDGER_MAIGRET_DISCOVERY_ENABLED": "off"},
            "Maigret",
        ),
    ],
)
def test_disabled_server_capability_refuses_new_scan(environment, message):
    mode = "exhaustive" if "EXHAUSTIVE" in " ".join(environment) else "focused"
    with pytest.raises(ProfileDiscoveryPolicyError, match=message):
        govern_profile_discovery_options({}, mode, environ=environment)


def test_disabled_user_scanner_refuses_only_plans_that_request_it():
    environment = {"OPENLEDGER_USER_SCANNER_DISCOVERY_ENABLED": "false"}
    permitted = govern_profile_discovery_options(
        {"investigation_spec": {"enable_user_scanner_username": False}},
        environ=environment,
    )
    assert permitted["execution_mode"] == "focused"

    with pytest.raises(ProfileDiscoveryPolicyError, match="User Scanner"):
        govern_profile_discovery_options(
            {"investigation_spec": {"enable_user_scanner_username": True}},
            environ=environment,
        )


def test_governed_pivot_kill_switch_is_server_owned():
    assert profile_discovery_flags(
        environ={"OPENLEDGER_GOVERNED_PIVOTS_ENABLED": "true"}
    )["governed_pivots_enabled"] is True
    assert profile_discovery_flags(
        environ={"OPENLEDGER_GOVERNED_PIVOTS_ENABLED": "false"}
    )["governed_pivots_enabled"] is False
    assert profile_discovery_flags(
        environ={"OPENLEDGER_GOVERNED_PIVOTS_ENABLED": "typo"}
    )["governed_pivots_enabled"] is False


def test_disabled_governed_pivot_refuses_a_stored_pivot_plan():
    with pytest.raises(ProfileDiscoveryPolicyError, match="Governed evidence"):
        govern_profile_discovery_options(
            {"governed_pivot_plan": {"policy_version": "test"}},
            environ={"OPENLEDGER_GOVERNED_PIVOTS_ENABLED": "false"},
        )
