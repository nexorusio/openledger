(function () {
    const root = document.getElementById('caseChat');
    const form = document.getElementById('caseChatForm');
    if (!root || !form) return;

    const thread = document.getElementById('caseChatThread');
    const textarea = document.getElementById('caseChatMessage');
    const persona = document.getElementById('caseChatPersona');
    const research = document.getElementById('caseChatResearch');
    const propose = document.getElementById('caseChatPropose');
    const submit = document.getElementById('caseChatSubmit');
    const errorBox = document.getElementById('caseChatError');
    const isCombined = root.dataset.combined === 'true';

    if (root.dataset.initialPrompt && !textarea.value.trim()) {
        textarea.value = root.dataset.initialPrompt;
    }
    if (root.dataset.initialResearch === 'true') {
        research.checked = true;
    }

    function icon(name) {
        const element = document.createElement('i');
        element.setAttribute('data-lucide', name);
        return element;
    }

    function appendMessage(message) {
        const empty = document.getElementById('caseChatEmpty');
        if (empty) empty.remove();

        const article = document.createElement('article');
        article.className = 'case-chat-message ' + message.role;
        article.dataset.messageId = message.id || '';

        const header = document.createElement('header');
        const avatar = document.createElement('span');
        avatar.className = 'case-chat-avatar';
        avatar.appendChild(icon(message.role === 'assistant' ? 'sparkles' : 'user-round'));
        const identity = document.createElement('span');
        const author = document.createElement('strong');
        author.textContent = message.author || '';
        const time = document.createElement('small');
        time.textContent = message.created_at || '';
        identity.append(author, time);
        header.append(avatar, identity);
        if (message.research_enabled) {
            const badge = document.createElement('span');
            badge.className = 'badge-soft';
            badge.textContent = 'Public-web research';
            header.appendChild(badge);
        }
        article.appendChild(header);

        const content = document.createElement('div');
        content.className = 'case-chat-message-content';
        if (message.role === 'assistant' && typeof message.content_html === 'string') {
            // Only the server's escaped chat renderer supplies this field.
            content.classList.add('formatted');
            content.innerHTML = message.content_html;
        } else {
            content.textContent = message.content || '';
        }
        article.appendChild(content);

        if (Array.isArray(message.sources) && message.sources.length) {
            const sources = document.createElement('div');
            sources.className = 'case-chat-sources';
            const label = document.createElement('strong');
            label.textContent = 'Sources';
            const list = document.createElement('ol');
            message.sources.forEach(function (source) {
                const item = document.createElement('li');
                const link = document.createElement('a');
                link.href = source.url;
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                link.textContent = source.title || source.url;
                item.appendChild(link);
                list.appendChild(item);
            });
            sources.append(label, list);
            article.appendChild(sources);
        }

        const proposalSummary = message.proposals || {};
        if (Array.isArray(proposalSummary.url_evidence) && proposalSummary.url_evidence.length) {
            const evidence = document.createElement('div');
            evidence.className = 'case-chat-url-evidence';
            const label = document.createElement('strong');
            label.textContent = 'Supplied URLs';
            const list = document.createElement('ul');
            proposalSummary.url_evidence.forEach(function (record) {
                const item = document.createElement('li');
                const link = document.createElement('a');
                link.href = record.supplied_url;
                link.textContent = record.supplied_url;
                link.target = '_blank';
                link.rel = 'noopener noreferrer';
                const status = document.createElement('span');
                const citations = Array.isArray(record.citation_urls) ? record.citation_urls : [];
                const citationCount = Number.isInteger(record.citation_count)
                    ? record.citation_count : citations.length;
                const wasCited = ['profile_url_cited', 'exact_url_cited'].includes(record.citation_status)
                    || citations.length > 0;
                status.textContent = wasCited
                    ? 'Matching URL cited in public-web research. Direct access and identity remain unverified.'
                    : record.citation_status === 'research_not_requested'
                        ? 'Retained as supplied context. Public-web research was not requested.'
                        : 'Retained as supplied context. No matching URL citation; this does not establish that the profile is absent or blocked.';
                item.append(link, status);
                citations.forEach(function (url) {
                    const cited = document.createElement('a');
                    cited.href = url;
                    cited.textContent = 'Cited URL: ' + url;
                    cited.target = '_blank';
                    cited.rel = 'noopener noreferrer';
                    item.appendChild(cited);
                });
                if (wasCited && citationCount > citations.length) {
                    const omitted = document.createElement('span');
                    omitted.textContent = 'Some matching citation URLs are shown only in Sources.';
                    item.appendChild(omitted);
                }
                list.appendChild(item);
            });
            evidence.append(label, list);
            article.appendChild(evidence);
        }
        if (proposalSummary.extraction_status === 'unavailable' && proposalSummary.status === 'pending_review') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note warning';
            note.textContent = 'Supplied URLs were retained for review. Other AI fact proposals could not be extracted.';
            article.appendChild(note);
        }
        if (proposalSummary.research_status === 'no_independent_citations') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note warning';
            note.appendChild(icon('shield-alert'));
            note.appendChild(document.createTextNode(
                ' Public-web research could not independently corroborate the supplied URL. It remains analyst-supplied, unverified context.'
            ));
            article.appendChild(note);
        }
        if (proposalSummary.status === 'pending_review') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note';
            note.appendChild(icon('clipboard-check'));
            const count = Number(proposalSummary.count || 0);
            const proposalLabel = proposalSummary.kind === 'relationship'
                ? ' relationship hypothes' + (count === 1 ? 'is' : 'es')
                : ' Persona proposal' + (count === 1 ? '' : 's');
            note.appendChild(document.createTextNode(
                ' ' + count + proposalLabel + ' sent to pending review.'
            ));
            article.appendChild(note);
        } else if (proposalSummary.status === 'no_supported_relationships') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note warning';
            note.appendChild(icon('shield-alert'));
            note.appendChild(document.createTextNode(
                ' No new relationship met the approved-evidence requirements; the answer remains in chat for analysis.'
            ));
            article.appendChild(note);
        } else if (proposalSummary.status === 'no_supported_facts') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note warning';
            note.appendChild(icon('shield-alert'));
            note.appendChild(document.createTextNode(
                ' No explicit, supported Persona fact was proposed; the answer remains in chat for analysis.'
            ));
            article.appendChild(note);
        } else if (proposalSummary.status === 'stale_snapshot') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note warning';
            note.appendChild(icon('refresh-cw'));
            note.appendChild(document.createTextNode(
                ' The answer was saved, but new relationship proposals require a refreshed snapshot.'
            ));
            article.appendChild(note);
        } else if (proposalSummary.status === 'unavailable') {
            const note = document.createElement('div');
            note.className = 'case-chat-proposal-note warning';
            note.appendChild(icon('triangle-alert'));
            note.appendChild(document.createTextNode(
                ' The answer was saved, but ' + (proposalSummary.kind === 'relationship' ? 'relationship' : 'Persona') + ' proposal extraction was unavailable.'
            ));
            article.appendChild(note);
        }
        thread.appendChild(article);
        if (window.lucide) window.lucide.createIcons({ attrs: { 'stroke-width': 1.8 } });
        thread.scrollTop = thread.scrollHeight;
    }

    function setBusy(busy) {
        submit.disabled = busy;
        textarea.disabled = busy;
        if (persona) persona.disabled = busy;
        research.disabled = busy;
        propose.disabled = busy;
        submit.classList.toggle('is-loading', busy);
        submit.lastChild.textContent = busy ? ' Working…' : ' Send';
    }

    form.addEventListener('submit', async function (event) {
        event.preventDefault();
        errorBox.hidden = true;
        const message = textarea.value.trim();
        if (!message) return;
        if (!isCombined && propose.checked && (!persona || !persona.value)) {
            errorBox.textContent = 'Choose a target Persona before proposing supported facts.';
            errorBox.hidden = false;
            if (persona) persona.focus();
            return;
        }
        setBusy(true);
        try {
            const response = await fetch(root.dataset.endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-OpenLedger-CSRF': root.dataset.csrfToken,
                },
                body: JSON.stringify({
                    message: message,
                    persona_id: persona?.value || null,
                    research_enabled: research.checked,
                    propose_to_persona: !isCombined && propose.checked,
                    propose_relationships: isCombined && propose.checked,
                }),
            });
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.error || 'Case chat failed.');
            appendMessage(payload.user_message);
            appendMessage(payload.assistant_message);
            textarea.value = '';
        } catch (error) {
            errorBox.textContent = error.message || 'Case chat failed.';
            errorBox.hidden = false;
        } finally {
            setBusy(false);
            textarea.focus();
        }
    });

    thread.scrollTop = thread.scrollHeight;
})();
