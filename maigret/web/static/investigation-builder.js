/*
 * SPDX-FileCopyrightText: 2026 PT Daya Prana Inovasi
 * SPDX-License-Identifier: LicenseRef-Nexorus-Proprietary
 */

(() => {
    const form = document.getElementById('investigation-builder');
    if (!form) return;

    const editor = document.getElementById('investigation-token-editor');
    const list = document.getElementById('investigation-token-list');
    const input = document.getElementById('investigation-token-input');
    const status = document.getElementById('token-editor-status');
    const aliasToggle = document.getElementById('search-likely-username-aliases');
    const emailSection = document.getElementById('email-route-confirmation');
    const emailConfirmation = document.getElementById('confirm-email-route');
    const preview = document.getElementById('investigation-plan-preview');
    const previewMode = document.getElementById('investigation-plan-mode');
    const previewSummary = document.getElementById('investigation-plan-summary');
    const routeList = document.getElementById('investigation-route-list');
    const error = document.getElementById('investigation-form-error');
    const startButton = document.getElementById('startBtn');
    const csrfToken = form.querySelector('input[name="csrf_token"]').value;
    const maximumTokens = 24;
    const typeLabels = {
        email: 'Email',
        full_name: 'Full name',
        phone: 'Phone · context',
        profile_url: 'Profile URL',
        public_url: 'Public URL · context',
        social_handle: 'Social handle',
        username: 'Username'
    };
    const routeReasons = {
        confirmation_required: 'Confirmation required',
        context_only_no_outbound: 'Context only · no outbound request',
        server_disabled: 'Disabled by server policy'
    };

    let tokens = Array.from(list.querySelectorAll('.investigation-token-chip'))
        .map(chip => ({
            type: chip.dataset.tokenType || 'pending',
            value: chip.dataset.tokenValue || ''
        }))
        .filter(token => token.value);
    let editing = null;
    let previewController = null;
    let previewSequence = 0;
    let previewPromise = Promise.resolve(false);
    let canStart = false;
    let lastBlockingMessage = '';
    let submitting = false;

    function comparisonKey(value) {
        return value.normalize('NFKC').trim().toLocaleLowerCase('en-US');
    }

    function selectedMode() {
        const selected = form.querySelector('input[name="mode"]:checked');
        return selected ? selected.value : 'quick';
    }

    function setError(message, { focus = false } = {}) {
        const visible = Boolean(message);
        error.textContent = message || '';
        error.hidden = !visible;
        input.setAttribute('aria-invalid', visible ? 'true' : 'false');
        editor.classList.toggle('has-error', visible);
        if (visible && focus) error.focus();
    }

    function updateEditorStatus(message = '') {
        if (message) {
            status.textContent = message;
            return;
        }
        status.textContent = tokens.length
            ? `${tokens.length} ${tokens.length === 1 ? 'value' : 'values'} added. Select a value to edit it.`
            : 'No values added.';
    }

    function createTokenChip(token, index) {
        const chip = document.createElement('span');
        chip.className = 'investigation-token-chip';
        if (token.invalid) chip.classList.add('is-invalid');
        chip.dataset.tokenValue = token.value;
        chip.dataset.tokenType = token.type || 'pending';

        const editButton = document.createElement('button');
        editButton.type = 'button';
        editButton.className = 'token-chip-edit';
        editButton.setAttribute('aria-label', `Edit ${token.value}`);

        const type = document.createElement('span');
        type.className = 'token-chip-type';
        type.textContent = token.invalid
            ? 'Needs correction'
            : (typeLabels[token.type] || 'Checking…');
        const value = document.createElement('span');
        value.className = 'token-chip-value';
        value.textContent = token.value;
        editButton.append(type, value);
        editButton.addEventListener('click', () => beginEdit(index));

        const removeButton = document.createElement('button');
        removeButton.type = 'button';
        removeButton.className = 'token-chip-remove';
        removeButton.setAttribute('aria-label', `Remove ${token.value}`);
        const removeIcon = document.createElement('i');
        removeIcon.dataset.lucide = 'x';
        removeIcon.setAttribute('aria-hidden', 'true');
        removeButton.appendChild(removeIcon);
        removeButton.addEventListener('click', () => removeToken(index));

        const hidden = document.createElement('input');
        hidden.type = 'hidden';
        hidden.name = 'investigation_token';
        hidden.value = token.value;
        chip.append(editButton, removeButton, hidden);
        return chip;
    }

    function renderTokens() {
        list.replaceChildren(...tokens.map(createTokenChip));
        updateEditorStatus();
        if (window.lucide) {
            window.lucide.createIcons({ attrs: { 'stroke-width': 1.8 } });
        }
    }

    function beginEdit(index) {
        if (editing) {
            setError('Finish or cancel the current edit before editing another value.');
            input.focus();
            return;
        }
        const [token] = tokens.splice(index, 1);
        editing = { index, token };
        input.value = token.value;
        renderTokens();
        refreshPreview();
        setError('');
        input.focus();
        input.select();
        updateEditorStatus(`Editing ${token.value}. Press Escape to cancel.`);
    }

    function cancelEdit() {
        if (!editing) {
            input.value = '';
            return;
        }
        tokens.splice(Math.min(editing.index, tokens.length), 0, editing.token);
        editing = null;
        input.value = '';
        renderTokens();
        refreshPreview();
        setError('');
        input.focus();
    }

    function removeToken(index) {
        const [removed] = tokens.splice(index, 1);
        renderTokens();
        refreshPreview();
        input.focus();
        updateEditorStatus(`${removed.value} removed. ${tokens.length} ${tokens.length === 1 ? 'value remains' : 'values remain'}.`);
    }

    async function commitDraft() {
        const draft = input.value.trim();
        if (!draft) return false;
        if (!editing && tokens.length >= maximumTokens) {
            setError(`Use no more than ${maximumTokens} investigation values.`);
            input.focus();
            return false;
        }
        const duplicate = tokens.some(token => comparisonKey(token.value) === comparisonKey(draft));
        if (duplicate) {
            setError('That value is already included. Edit the existing value or enter a different one.');
            input.focus();
            input.select();
            return false;
        }

        const insertionIndex = editing ? Math.min(editing.index, tokens.length) : tokens.length;
        tokens.splice(insertionIndex, 0, { type: 'pending', value: draft });
        editing = null;
        input.value = '';
        setError('');
        renderTokens();
        const accepted = await refreshPreview({ invalidIndex: insertionIndex });
        input.focus();
        return accepted;
    }

    function renderRoute(route, state) {
        const item = document.createElement('div');
        item.className = `investigation-route-item is-${state}`;
        const copy = document.createElement('div');
        const label = document.createElement('strong');
        label.textContent = route.label || route.route || 'Collection route';
        const detail = document.createElement('small');
        const count = Number(route.target_count || 0);
        const requestCount = Number(route.planned_request_count || 0);
        if (state === 'active') {
            detail.textContent = requestCount
                ? `${requestCount} planned request${requestCount === 1 ? '' : 's'}`
                : `${count} target${count === 1 ? '' : 's'}`;
        } else {
            detail.textContent = routeReasons[route.reason_code] || 'Not included in this run';
        }
        copy.append(label, detail);
        const badge = document.createElement('span');
        badge.className = 'route-status';
        badge.textContent = state === 'active' ? 'Active' : 'Skipped';
        item.append(copy, badge);
        return item;
    }

    function renderPlan(payload) {
        const routePlan = payload.route_plan || {};
        const activeRoutes = Array.isArray(routePlan.effective_routes)
            ? routePlan.effective_routes : [];
        const skippedRoutes = Array.isArray(routePlan.skipped_routes)
            ? routePlan.skipped_routes : [];
        const minutes = Math.round(Number(routePlan.budget_seconds || 0) / 60);
        const modeLabel = routePlan.requested_mode === 'full' ? 'Full Scan' : 'Quick Scan';
        previewMode.textContent = `${modeLabel} · ${minutes} min`;
        previewSummary.textContent = `${tokens.length} classified ${tokens.length === 1 ? 'value' : 'values'} · ${activeRoutes.length} active ${activeRoutes.length === 1 ? 'route' : 'routes'}${skippedRoutes.length ? ` · ${skippedRoutes.length} skipped` : ''}.`;
        routeList.replaceChildren(
            ...activeRoutes.map(route => renderRoute(route, 'active')),
            ...skippedRoutes.map(route => renderRoute(route, 'skipped'))
        );
        if (!routeList.children.length) {
            const empty = document.createElement('p');
            empty.className = 'investigation-route-empty';
            empty.textContent = 'No collection route is available for these values.';
            routeList.appendChild(empty);
        }
    }

    async function requestPreview(signal) {
        const response = await fetch(form.dataset.previewUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-OpenLedger-CSRF': csrfToken
            },
            body: JSON.stringify({
                tokens: tokens.map(token => token.value),
                mode: selectedMode(),
                search_likely_username_aliases: aliasToggle.checked,
                confirm_email_route: emailConfirmation.checked
            }),
            signal
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
            const requestError = new Error(payload.error || 'OpenLedger could not preview this investigation.');
            requestError.payload = payload;
            requestError.validationError = response.status === 400;
            throw requestError;
        }
        return payload;
    }

    function resetPreview() {
        canStart = false;
        lastBlockingMessage = 'Add at least one investigation value.';
        startButton.disabled = true;
        previewMode.textContent = 'Waiting for input';
        previewSummary.textContent = 'Add at least one value to preview the server-owned routes and limits.';
        routeList.replaceChildren();
        emailSection.hidden = true;
        emailConfirmation.checked = false;
        preview.setAttribute('aria-busy', 'false');
    }

    function refreshPreview({ invalidIndex = null } = {}) {
        const sequence = ++previewSequence;
        if (previewController) previewController.abort();
        if (!tokens.length) {
            resetPreview();
            previewPromise = Promise.resolve(false);
            return previewPromise;
        }

        previewController = new AbortController();
        preview.setAttribute('aria-busy', 'true');
        startButton.disabled = true;
        canStart = false;
        previewMode.textContent = 'Checking…';
        previewPromise = requestPreview(previewController.signal)
            .then(payload => {
                if (sequence !== previewSequence) return false;
                tokens = (payload.tokens || []).map(token => ({
                    type: token.type,
                    value: token.value
                }));
                renderTokens();
                const hasEmail = tokens.some(token => token.type === 'email');
                emailSection.hidden = !hasEmail;
                if (!hasEmail) emailConfirmation.checked = false;
                renderPlan(payload);
                canStart = Boolean(payload.can_start);
                lastBlockingMessage = payload.blocking_error || '';
                startButton.disabled = !canStart;
                if (payload.requires_email_confirmation) {
                    lastBlockingMessage = 'Confirm the bounded public email check before starting.';
                }
                setError('');
                return canStart;
            })
            .catch(requestError => {
                if (requestError.name === 'AbortError' || sequence !== previewSequence) return false;
                if (requestError.validationError && Number.isInteger(invalidIndex) && tokens[invalidIndex]) {
                    tokens[invalidIndex].invalid = true;
                    renderTokens();
                }
                canStart = false;
                lastBlockingMessage = requestError.message;
                startButton.disabled = true;
                previewMode.textContent = 'Plan unavailable';
                previewSummary.textContent = requestError.message;
                routeList.replaceChildren();
                setError(requestError.message);
                return false;
            })
            .finally(() => {
                if (sequence === previewSequence) {
                    preview.setAttribute('aria-busy', 'false');
                }
            });
        return previewPromise;
    }

    input.addEventListener('keydown', event => {
        if (event.key === 'Escape') {
            event.preventDefault();
            cancelEdit();
            return;
        }
        if (event.key !== 'Enter' && event.key !== 'Tab') return;
        const draft = input.value.trim();
        if (!draft) {
            if (event.key === 'Enter') event.preventDefault();
            return;
        }
        event.preventDefault();
        commitDraft();
    });

    editor.addEventListener('click', event => {
        if (event.target === editor || event.target === list) input.focus();
    });
    aliasToggle.addEventListener('change', refreshPreview);
    emailConfirmation.addEventListener('change', refreshPreview);
    form.querySelectorAll('input[name="mode"]').forEach(control => {
        control.addEventListener('change', refreshPreview);
    });

    form.addEventListener('submit', async event => {
        event.preventDefault();
        if (submitting) return;
        if (input.value.trim()) {
            const committed = await commitDraft();
            if (!committed) return;
        } else if (editing) {
            setError('Enter the edited value or press Escape to keep the original.', { focus: true });
            return;
        }
        await previewPromise;
        if (!canStart) {
            setError(lastBlockingMessage || 'Review the authoritative plan before starting.', { focus: true });
            return;
        }
        submitting = true;
        startButton.disabled = true;
        startButton.textContent = 'Starting…';
        HTMLFormElement.prototype.submit.call(form);
    });

    renderTokens();
    if (tokens.length) refreshPreview();
    else resetPreview();
})();
