/*
 * SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
 * SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
 */
(function (global) {
    'use strict';

    const ENGINE_LABELS = Object.freeze({
        native: 'Native profile search', maigret: 'Maigret', github: 'GitHub',
        unfurl: 'Unfurl', wayback: 'Wayback',
        user_scanner_username: 'User Scanner usernames',
        user_scanner_email: 'User Scanner emails',
        'native-profile-search': 'Native profile search',
        'github-public-profile': 'GitHub', 'unfurl-url-analysis': 'Unfurl',
        'wayback-cdx': 'Wayback', 'user-scanner-username': 'User Scanner usernames',
        'user-scanner': 'User Scanner emails',
    });
    const STATUS_LABELS = Object.freeze({
        running: 'Running', completed: 'Completed', partial: 'Partial',
        failed: 'Failed', cancelled: 'Cancelled', interrupted: 'Interrupted',
        unknown: 'Unknown', queued: 'Queued', skipped: 'Skipped', pending: 'Pending',
        not_selected: 'Not selected', skipped_no_targets: 'No targets',
        blocked_dependency: 'Blocked by dependency',
        not_started_budget: 'Not started: budget', timed_out: 'Timed out',
        cleanup_incomplete: 'Cleanup incomplete',
    });
    const REASON_LABELS = Object.freeze({
        budget_exhausted: 'Budget reached', cancelled: 'Cancelled',
        interrupted: 'Interrupted', provider_unavailable: 'Provider unavailable',
        provider_circuit_open: 'Provider circuit open', timeout: 'Timed out',
        failed: 'Failed', unavailable: 'Unavailable', unattempted: 'Unattempted',
        unknown: 'Unknown outcome', source_not_selected: 'Source not selected',
        no_eligible_targets: 'No eligible targets', dependency_not_ready: 'Dependency not ready',
        cancellation_requested: 'Cancellation requested', stage_budget_exhausted: 'Stage budget reached',
        overall_budget_exhausted: 'Overall budget reached', parent_cancelled: 'Parent cancelled',
        timed_out: 'Timed out', cleanup_incomplete: 'Cleanup incomplete',
        not_admitted: 'Not admitted',
    });
    const UNIT_LABELS = Object.freeze({
        site_checks: 'site checks', queries: 'queries', targets: 'targets',
        invocations: 'invocations',
    });
    const COUNTS = ['planned', 'started', 'terminal', 'completed', 'errors',
        'timeouts', 'cancelled', 'interrupted', 'unattempted', 'unknown', 'observations'];

    function whole(value) {
        return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
            ? value : null;
    }

    function text(value, fallback) {
        return typeof value === 'string' && value.trim() ? value.trim() : fallback;
    }

    function displayCount(value) {
        const count = whole(value);
        return count === null ? '—' : String(count);
    }

    function normalize(snapshot) {
        if (!snapshot || Number(snapshot.schema_version) !== 1) return null;
        const revision = whole(snapshot.revision);
        if (revision === null || !Array.isArray(snapshot.stages)) return null;
        const state = text(snapshot.state, 'unknown');
        return {
            schemaVersion: 1,
            revision,
            known: snapshot.known === true,
            state: Object.prototype.hasOwnProperty.call(STATUS_LABELS, state) ? state : 'unknown',
            stages: snapshot.stages.filter(stage => stage && typeof stage === 'object').map(stage => ({
                stageId: text(stage.stage_id, ''),
                engineId: text(stage.engine_id, ''),
                label: ENGINE_LABELS[text(stage.engine_id, '')] || 'Declared collection stage',
                unit: UNIT_LABELS[text(stage.unit, '')] || 'operations',
                status: STATUS_LABELS[text(stage.status, '')] ? text(stage.status, '') : 'unknown',
                reason: REASON_LABELS[text(stage.reason, '')] || '',
                counts: Object.fromEntries(COUNTS.map(key => [key, whole(stage[key])])),
            })),
        };
    }

    function create(options) {
        const panel = document.getElementById(options.panelId);
        const state = document.getElementById(options.stateId);
        const body = document.getElementById(options.bodyId);
        const unavailable = document.getElementById(options.unavailableId);
        let latestRevision = -1;
        let snapshot = null;

        function showUnavailable() {
            if (!panel || !unavailable) return;
            panel.hidden = false;
            unavailable.hidden = false;
            if (body) body.replaceChildren();
            if (state) state.textContent = 'Coverage unavailable';
        }

        function render(next) {
            if (!panel || !body || !state || !unavailable) return;
            panel.hidden = false;
            unavailable.hidden = next.known;
            state.textContent = next.known
                ? STATUS_LABELS[next.state]
                : 'Coverage unavailable';
            body.replaceChildren();
            for (const stage of next.stages) {
                const row = document.createElement('tr');
                const cells = [
                    stage.label,
                    stage.unit,
                    STATUS_LABELS[stage.status],
                    stage.reason || '—',
                    displayCount(stage.counts.terminal) + ' / ' + displayCount(stage.counts.planned),
                    displayCount(stage.counts.errors),
                    displayCount(stage.counts.timeouts),
                    displayCount(stage.counts.unattempted),
                    displayCount(stage.counts.unknown),
                ];
                for (const cellText of cells) {
                    const cell = document.createElement('td');
                    cell.textContent = cellText;
                    row.appendChild(cell);
                }
                body.appendChild(row);
            }
            if (!next.stages.length) {
                const row = document.createElement('tr');
                const cell = document.createElement('td');
                cell.colSpan = 9;
                cell.textContent = 'No declared collection stages were recorded.';
                row.appendChild(cell);
                body.appendChild(row);
            }
        }

        return {
            apply(raw) {
                const next = normalize(raw);
                if (!next) return false;
                if (next.revision <= latestRevision) return false;
                latestRevision = next.revision;
                snapshot = next;
                render(next);
                if (typeof options.onSnapshot === 'function') options.onSnapshot(next);
                return true;
            },
            markUnavailable() {
                if (!snapshot) showUnavailable();
            },
            hasKnownSnapshot() {
                return Boolean(snapshot && snapshot.known);
            },
        };
    }

    global.CollectionProgress = Object.freeze({create});
}(window));
