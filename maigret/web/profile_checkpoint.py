# SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
# SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary

"""Retention validation for already normalized profile collector observations.

This accepts the five existing profile adapter contracts. It is not a generic
provider-response store; adding another source requires its own policy review.
"""

import math

COMMON = {
    'source_engine',
    'subject_type',
    'subject_value',
    'status',
    'site_name',
    'category',
    'source_url',
    'source_record_id',
    'reason',
    'extra',
    'media',
}
USERNAME = {
    'seed_username',
    'native_status',
    'detector_status',
    'account_status',
    'identity_confidence',
    'identity_status',
    'scan_stage',
}
EXTRA = {
    'github_public_profile': {
        'api_version',
        'github_id',
        'login',
        'account_type',
        'name',
        'company',
        'location',
        'bio',
        'created_at',
        'updated_at',
        'blog',
        'twitter_username',
        'followers',
        'following',
        'public_repos',
        'public_gists',
        'rate_limit_remaining',
        'rate_limit_reset',
    },
    'unfurl_url_analysis': {
        'unfurl_version',
        'unfurl_commit',
        'remote_lookups',
        'node_count',
        'nodes',
        'structural_analysis_only',
        'human_review_required',
    },
    'wayback_cdx': {
        'queried_profile_url',
        'query_match_type',
        'retry_after',
        'sample_direction',
        'sampled_capture_count',
        'oldest_sampled_capture_at',
        'latest_sampled_capture_at',
        'captures',
        'archived_page_content_fetched',
        'historical_presence_only',
        'human_review_required',
    },
    'user_scanner_username': None,
    'user_scanner_email': None,
}
RAW_KEYS = {
    'providerresponse',
    'rawresponse',
    'responsebody',
    'requestheaders',
    'responseheaders',
    'googleplacessearch',
    'placedetails',
    'credentials',
    'authorization',
    'proxyauthorization',
    'cookie',
    'setcookie',
}


def _reject(message):
    raise ValueError('unsupported collection checkpoint evidence: ' + message)


def _object(value, allowed=None, limit=40):
    if not isinstance(value, dict) or len(value) > limit:
        _reject('invalid or oversized object')
    if allowed is not None and set(value) - allowed:
        _reject('unknown fields')
    return value


def _scalar(value, limit=2000):
    if value is None or type(value) is bool:
        return
    if isinstance(value, str) and len(value) <= limit:
        return
    if type(value) in {int, float} and math.isfinite(value) and abs(value) <= 10**15:
        return
    _reject('unbounded or nested scalar')


def reject_private_checkpoint_fields(value, depth=0, engine=None):
    """Reject credential/raw-response keys even inside otherwise allowed fields."""
    from maigret.web.collector_adapters import _is_sensitive_url_key, GITHUB_API_VERSION

    if depth > 16:
        _reject('excessive nesting')
    if isinstance(value, dict):
        engine = value.get('source_engine', engine)
        for key, child in value.items():
            compact = ''.join(char for char in str(key).casefold() if char.isalnum())
            structural_key = (
                (depth == 0 and key == 'session_folder')
                or (
                    key == 'key'
                    and {'id', 'data_type', 'value', 'parent_id'}.issubset(value)
                )
                or (
                    engine == 'github_public_profile'
                    and key == 'api_version'
                    and child == GITHUB_API_VERSION
                )
            )
            if (
                not isinstance(key, str)
                or len(key) > 100
                or (_is_sensitive_url_key(key) and not structural_key)
                or compact in RAW_KEYS
            ):
                _reject('credential or provider-response field')
            reject_private_checkpoint_fields(child, depth + 1, engine)
    elif isinstance(value, list):
        for child in value:
            reject_private_checkpoint_fields(child, depth + 1, engine)


def validate_profile_observation(observation):
    from maigret.web.collector_adapters import (
        _safe_public_url,
        _url_has_sensitive_query_key,
        _is_sensitive_url_key,
        _UNFURL_DATA_TYPE_PATTERN,
        UNFURL_MAX_NODES,
        WAYBACK_MAX_CAPTURES,
    )

    def url(value):
        if value and (
            not isinstance(value, str)
            or not _safe_public_url(value)
            or _url_has_sensitive_query_key(value)
        ):
            _reject('unsafe evidence URL')

    item = _object(observation)
    engine = item.get('source_engine')
    if engine not in EXTRA:
        _reject('source policy')
    _object(item, COMMON | (USERNAME if engine == 'user_scanner_username' else set()))
    reject_private_checkpoint_fields(item)
    limits = {
        'site_name': 300,
        'category': 100,
        'reason': 1000,
        'subject_value': 500,
        'source_record_id': 500,
        'status': 40,
        'seed_username': 128,
        'native_status': 40,
        'scan_stage': 40,
    }
    for key, value in item.items():
        if key not in {'extra', 'media'}:
            _scalar(value, limits.get(key, 2000))
    url(item.get('source_url'))
    extra = _object(item.get('extra', {}), EXTRA[engine])
    if (
        extra.get('automatic_approval_allowed', False) is not False
        or extra.get('human_review_required', True) is not True
    ):
        _reject('review policy override')
    if (
        engine == 'user_scanner_username'
        and item.get('identity_status', 'unverified') != 'unverified'
    ):
        _reject('unreviewed identity promotion')
    media = _object(
        item.get('media', {}),
        (
            {'avatar'}
            if engine == 'github_public_profile'
            else set() if engine in {'unfurl_url_analysis', 'wayback_cdx'} else None
        ),
        limit=12,
    )
    for value in media.values():
        if not isinstance(value, str):
            _reject('invalid media URL')
        url(value)
    for key, value in extra.items():
        if key not in {'nodes', 'captures'}:
            _scalar(value)
    for key in {'blog', 'queried_profile_url'} & set(extra):
        url(extra[key])
    if (
        extra.get('remote_lookups', False) is not False
        or extra.get('archived_page_content_fetched', False) is not False
    ):
        _reject('transient or remote content')
    if 'nodes' in extra:
        nodes = extra['nodes']
        if (
            not isinstance(nodes, list)
            or len(nodes) > UNFURL_MAX_NODES
            or extra.get('node_count') != len(nodes)
        ):
            _reject('invalid node count')
        for node in nodes:
            _object(node, {'id', 'data_type', 'key', 'value', 'parent_id'}, limit=5)
            if type(node.get('id')) is not int or node['id'] <= 0:
                _reject('invalid node identity')
            if not isinstance(
                node.get('data_type'), str
            ) or not _UNFURL_DATA_TYPE_PATTERN.fullmatch(node['data_type']):
                _reject('invalid node type')
            _scalar(node.get('key'), 100)
            _scalar(node.get('value'), 1000)
            if (
                _is_sensitive_url_key(node.get('key'))
                and node.get('value') != '[redacted]'
            ):
                _reject('unredacted URL component')
            if (
                node['data_type'] == 'url.query'
                and not _is_sensitive_url_key(node.get('key'))
                and not str(node.get('value', '')).startswith('query keys: ')
            ):
                _reject('unredacted query')
            parents = node.get('parent_id')
            parents = (
                parents
                if isinstance(parents, list)
                else [] if parents is None else [parents]
            )
            if len(parents) > 8 or any(
                type(parent) is not int or parent <= 0 for parent in parents
            ):
                _reject('invalid node parents')
    if 'captures' in extra:
        captures = extra['captures']
        if (
            not isinstance(captures, list)
            or len(captures) > WAYBACK_MAX_CAPTURES
            or extra.get('sampled_capture_count') != len(captures)
        ):
            _reject('invalid capture count')
        for capture in captures:
            _object(
                capture,
                {'captured_at', 'timestamp', 'digest', 'original_url', 'replay_url'},
                limit=5,
            )
            for value in capture.values():
                _scalar(value)
            url(capture.get('original_url'))
            url(capture.get('replay_url'))
    return item
