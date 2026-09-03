/* OpenLedger read-only relationship workspace.
 * This module transforms the server-projected graph only in browser memory.
 * It deliberately performs no HTTP requests and exposes no evidence mutation action.
 */
(() => {
    'use strict';

    const graphElement = document.getElementById('relationshipGraph');
    const dataElement = document.getElementById('relationshipGraphData');
    if (!graphElement || !dataElement || !window.vis?.DataSet || !window.vis?.Network) return;

    let graph;
    try {
        graph = JSON.parse(dataElement.textContent);
    } catch (_error) {
        return;
    }

    const colors = (node) => {
        if (node.kind === 'persona') return {background: '#0f8b8d', border: '#30c7ca', highlight: {background: '#13a5a7', border: '#e8ffff'}};
        if (node.kind === 'source') return {background: '#152538', border: '#6f849c', highlight: {background: '#1d344d', border: '#30c7ca'}};
        if (node.kind === 'attribute') return {background: '#17314a', border: '#73a5d1', highlight: {background: '#234868', border: '#d7edff'}};
        if (node.kind === 'organization') return {background: '#3a2a17', border: '#d6a14a', highlight: {background: '#584126', border: '#ffe0a0'}};
        if (node.review_status === 'approved') return {background: '#12392f', border: '#20bd83', highlight: {background: '#185040', border: '#b8ffe2'}};
        if (node.review_status === 'uncertain') return {background: '#41351e', border: '#d5a846', highlight: {background: '#594a2b', border: '#ffe6aa'}};
        return {background: '#26364a', border: '#8296ad', highlight: {background: '#344a64', border: '#d9e7f6'}};
    };
    const level = (node) => node.kind === 'persona' ? 0 : node.kind === 'source' ? 2 : 1;
    const fieldLabels = {
        company: 'Organization, institution or company',
        company_ownership: 'Ownership or leadership',
        occupation: 'Role or occupation',
    };
    const readable = (value) => fieldLabels[value] || String(value || '').replaceAll('_', ' ');
    const baseEdgeColor = (opacity = 0.78) => ({color: '#536b83', highlight: '#30c7ca', hover: '#73a5d1', opacity});
    const proposalEdgeColor = (status) => status === 'approved'
        ? {color: '#20bd83', highlight: '#b8ffe2', hover: '#57d9aa', opacity: 1}
        : {color: '#d5a846', highlight: '#ffe6aa', hover: '#edc76c', opacity: 0.82};

    const originalNodes = graph.nodes.map((node) => ({
        ...node,
        shape: node.kind === 'persona' ? 'dot' : node.kind === 'source' ? 'ellipse' : node.kind === 'attribute' ? 'diamond' : 'box',
        size: node.kind === 'persona' ? 25 : ['attribute', 'organization'].includes(node.kind) ? 19 : 15,
        color: colors(node),
        font: {color: '#d8e2ed', face: 'Alliance No. 2, Arial', size: node.kind === 'persona' ? 14 : 11},
        borderWidth: node.kind === 'persona' || node.review_status === 'approved' ? 2 : 1,
        margin: node.kind === 'claim' || node.kind === 'source' ? 9 : undefined,
        widthConstraint: {maximum: node.kind === 'persona' ? 180 : 190},
        level: level(node),
    }));
    const originalEdges = graph.edges.map((edge) => ({
        ...edge,
        color: edge.proposal_id ? proposalEdgeColor(edge.review_status) : baseEdgeColor(),
        width: edge.proposal_id ? 2 : 1.35,
        dashes: Boolean(edge.proposal_id && edge.review_status !== 'approved'),
        selectionWidth: 2.5, hoverWidth: 1.5,
        smooth: {type: 'dynamic'},
        arrows: {to: {enabled: graph.mode === 'persona', scaleFactor: 0.45}},
        font: {color: '#8191a6', face: 'Alliance No. 2, Arial', size: 9, strokeWidth: 0},
    }));
    const nodeLookup = new Map(originalNodes.map((node) => [node.id, node]));
    const edgeLookup = new Map(originalEdges.map((edge) => [edge.id, edge]));
    const nodes = new window.vis.DataSet(originalNodes);
    const edges = new window.vis.DataSet(originalEdges);
    const network = new window.vis.Network(graphElement, {nodes, edges}, {
        autoResize: true,
        interaction: {hover: true, keyboard: {enabled: true, bindToWindow: false}, multiselect: false, navigationButtons: false, selectConnectedEdges: false},
        physics: {enabled: true, solver: 'barnesHut', stabilization: {iterations: 220}, barnesHut: {gravitationalConstant: -4800, springLength: 145, springConstant: 0.035, damping: 0.16}},
        layout: {improvedLayout: true, randomSeed: 29},
    });

    const byId = (id) => document.getElementById(id);
    const inspector = byId('relationshipInspector');
    const searchInput = byId('relationshipNodeSearch');
    const searchResults = byId('relationshipSearchResults');
    const layoutSelect = byId('relationshipLayout');
    const hideButton = byId('hideRelationshipNode');
    const restoreButton = byId('restoreRelationshipNodes');
    const hiddenCount = byId('relationshipHiddenCount');
    const clearFocusButton = byId('clearRelationshipFocus');
    const focusButtons = Array.from(document.querySelectorAll('[data-relationship-focus]'));
    const fieldButtons = Array.from(document.querySelectorAll('[data-relationship-field]'));
    const graphViewButton = byId('relationshipGraphViewButton');
    const tableViewButton = byId('relationshipTableViewButton');
    const graphView = byId('relationshipGraphView');
    const tableView = byId('relationshipTableView');
    const tableBody = byId('relationshipTableBody');
    const status = byId('relationshipGraphStatus');
    const activeFields = new Set(Object.keys(graph.stats.field_counts || {}));
    const manuallyHiddenNodes = new Set();
    let selectedNodeId = null;
    let focusDepth = null;
    let visibleNodeIds = new Set(originalNodes.map((node) => node.id));
    let visibleEdgeIds = new Set(originalEdges.map((edge) => edge.id));
    let focusedNodeIds = null;

    const candidateEdges = () => originalEdges.filter((edge) => (edge.proposal_id || activeFields.has(edge.field_name)) && !manuallyHiddenNodes.has(edge.from) && !manuallyHiddenNodes.has(edge.to));
    const neighborhood = (rootId, depth, candidates) => {
        const adjacency = new Map();
        candidates.forEach((edge) => {
            if (!adjacency.has(edge.from)) adjacency.set(edge.from, new Set());
            if (!adjacency.has(edge.to)) adjacency.set(edge.to, new Set());
            adjacency.get(edge.from).add(edge.to);
            adjacency.get(edge.to).add(edge.from);
        });
        const found = new Set([rootId]);
        let frontier = new Set([rootId]);
        for (let step = 0; step < depth; step += 1) {
            const next = new Set();
            frontier.forEach((id) => (adjacency.get(id) || []).forEach((neighbor) => {
                if (!found.has(neighbor)) next.add(neighbor);
                found.add(neighbor);
            }));
            frontier = next;
        }
        return found;
    };

    const clearInspector = (message = 'Select a person, evidence record, source, shared attribute, or connection.') => {
        inspector.replaceChildren();
        const empty = document.createElement('div');
        empty.className = 'inspector-empty';
        const text = document.createElement('p');
        text.textContent = message;
        const note = document.createElement('small');
        note.textContent = 'Inspection is read-only.';
        empty.append(text, note);
        inspector.append(empty);
    };
    const addHeading = (label, kind) => {
        const heading = document.createElement('h3');
        heading.textContent = label;
        const badge = document.createElement('span');
        badge.className = 'inspector-kind';
        badge.textContent = kind;
        inspector.append(heading, badge);
    };
    const addLine = (label, value) => {
        if (value === undefined || value === null || value === '') return;
        const row = document.createElement('div');
        row.className = 'inspector-row';
        const key = document.createElement('span');
        key.textContent = label;
        const content = document.createElement('strong');
        content.textContent = String(value);
        row.append(key, content);
        inspector.append(row);
    };
    const safeUrl = (value) => {
        try {
            const parsed = new URL(value);
            return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : null;
        } catch (_error) {
            return null;
        }
    };
    const addPersonaLink = (persona) => {
        if (!persona?.persona_id) return;
        const link = document.createElement('a');
        link.className = 'btn btn-outline-secondary inspector-link';
        link.href = `/personas/${encodeURIComponent(persona.persona_id)}`;
        link.textContent = 'Open Persona profile';
        inspector.append(link);
    };
    const addList = (title) => {
        const section = document.createElement('section');
        section.className = 'inspector-section';
        const heading = document.createElement('h4');
        heading.textContent = title;
        const list = document.createElement('ul');
        list.className = 'inspector-evidence-list';
        section.append(heading, list);
        inspector.append(section);
        return list;
    };
    const appendSource = (list, source) => {
        const item = document.createElement('li');
        const url = safeUrl(source?.url);
        const label = url ? document.createElement('a') : document.createElement('strong');
        label.textContent = source?.name || 'Source record';
        if (url) {
            label.href = url; label.target = '_blank'; label.rel = 'noopener noreferrer';
        }
        item.append(label);
        if (source?.type) {
            const detail = document.createElement('small');
            detail.textContent = readable(source.type);
            item.append(detail);
        }
        list.append(item);
    };
    const sourcesForClaim = (claimId) => originalEdges.flatMap((edge) => {
        const otherId = edge.from === claimId ? edge.to : edge.to === claimId ? edge.from : null;
        const other = otherId ? nodeLookup.get(otherId) : null;
        return other?.kind === 'source' ? [{name: other.label, url: other.url, type: other.evidence_type}] : [];
    });

    const inspectNode = (id) => {
        const node = nodeLookup.get(id);
        if (!node) return;
        inspector.replaceChildren();
        addHeading(node.label, readable(node.kind));
        if (node.kind === 'persona') {
            addLine('Case', node.case_title);
            addLine('Connected visible paths', candidateEdges().filter((edge) => edge.from === id || edge.to === id).length);
            addPersonaLink(node);
        } else if (node.kind === 'claim') {
            addLine('Field', readable(node.field_name));
            addLine('Review status', readable(node.review_status));
            addLine('Evidence confidence', `${node.confidence}%`);
            const sources = sourcesForClaim(id);
            const list = addList(`Supporting sources (${sources.length})`);
            (sources.length ? sources : [{name: 'No source record attached'}]).forEach((source) => appendSource(list, source));
        } else if (node.kind === 'source') {
            addLine('Evidence type', readable(node.evidence_type || 'source record'));
            const list = addList('Source access');
            appendSource(list, {name: node.label, url: node.url, type: node.evidence_type});
        } else if (node.kind === 'organization') {
            addLine('Source case', node.case_title);
            addLine('Identity scope', readable(node.identity_scope));
            addLine('Relationship rule', 'Exact approved affiliation and analyst-confirmed organization name');
            const list = addList('Confirmation source');
            appendSource(list, {name: node.source_name || 'Confirmed organization record', url: node.source_url, type: 'analyst confirmed organization'});
        } else if (node.kind === 'ai_entity') {
            addLine('Source case', node.case_title);
            addLine('Entity type', readable(node.entity_type));
            addLine('Hypothesis review', readable(node.review_status));
            addLine('Relationship rule', 'AI-proposed cross-case relationship; analyst review controls graph status');
        } else {
            addLine('Shared field', readable(node.field_name));
            addLine('Connected Personas', node.persona_count);
            addLine('Relationship rule', 'Exact normalized value · approved claims only');
            const connections = originalEdges.filter((edge) => edge.from === id || edge.to === id);
            const list = addList(`Evidence paths (${connections.length})`);
            connections.forEach((edge) => {
                const persona = nodeLookup.get(edge.from === id ? edge.to : edge.from);
                appendSource(list, {name: `${persona.label} · ${persona.case_title}`, type: `${edge.confidence}% confidence`});
                (edge.sources || []).forEach((source) => appendSource(list, source));
            });
        }
    };
    const inspectEdge = (id) => {
        const edge = edgeLookup.get(id);
        if (!edge) return;
        const from = nodeLookup.get(edge.from);
        const to = nodeLookup.get(edge.to);
        inspector.replaceChildren();
        addHeading(`${from?.label || 'Record'} → ${to?.label || 'Record'}`, 'evidence path');
        addLine('Relationship', readable(edge.label));
        addLine('Field', readable(edge.field_name));
        if (edge.proposal_id) {
            addLine('Review status', readable(edge.review_status));
            addLine('AI confidence', `${edge.confidence}%`);
            addLine('Evidence rule', edge.relationship_rule);
            const list = addList(`Evidence anchors (${(edge.sources || []).length})`);
            ((edge.sources || []).length ? edge.sources : [{name: 'No public URL attached'}]).forEach((source) => appendSource(list, source));
            return;
        }
        if (graph.mode === 'shared') {
            addLine('Evidence rule', edge.relationship_rule || 'Exact normalized value · approved claim');
            addLine('Evidence confidence', `${edge.confidence}%`);
            const list = addList(`Attached sources (${(edge.sources || []).length})`);
            ((edge.sources || []).length ? edge.sources : [{name: 'No source URL attached'}]).forEach((source) => appendSource(list, source));
            if (from?.kind === 'persona') addPersonaLink(from);
        } else {
            const claim = from?.kind === 'claim' ? from : to?.kind === 'claim' ? to : null;
            const source = from?.kind === 'source' ? from : to?.kind === 'source' ? to : null;
            if (claim) {
                addLine('Review status', readable(claim.review_status));
                addLine('Evidence confidence', `${claim.confidence}%`);
            }
            const evidence = source ? [{name: source.label, url: source.url, type: source.evidence_type}] : claim ? sourcesForClaim(claim.id) : [];
            const list = addList(`Supporting sources (${evidence.length})`);
            (evidence.length ? evidence : [{name: 'No source record attached'}]).forEach((item) => appendSource(list, item));
        }
    };

    const updateControls = () => {
        const selected = Boolean(selectedNodeId && visibleNodeIds.has(selectedNodeId));
        hideButton.disabled = !selected;
        focusButtons.forEach((button) => {
            button.disabled = !selected;
            button.setAttribute('aria-pressed', String(selected && focusDepth === Number(button.dataset.relationshipFocus)));
        });
        clearFocusButton.disabled = focusDepth === null;
        restoreButton.disabled = manuallyHiddenNodes.size === 0;
        hiddenCount.textContent = String(manuallyHiddenNodes.size);
    };
    const nodeContext = (node) => {
        if (node.kind === 'persona') return node.case_title || 'Persona record';
        if (node.kind === 'claim') return `${readable(node.field_name)} · ${readable(node.review_status)} · ${node.confidence}% confidence`;
        if (node.kind === 'source') return readable(node.evidence_type || 'Evidence source');
        if (node.kind === 'organization') return `${node.case_title || 'Source case'} · analyst-confirmed organization`;
        return `${readable(node.field_name)} · ${node.persona_count} connected Personas`;
    };
    const addTableRow = (label, type, context, dataset, faded) => {
        const row = document.createElement('tr');
        if (faded) row.className = 'is-faded';
        [label, type, context].forEach((value, index) => {
            const cell = document.createElement('td');
            cell.textContent = value;
            cell.className = index === 0 ? 'relationship-table-record' : index === 1 ? 'relationship-table-type' : 'relationship-table-context';
            row.append(cell);
        });
        const action = document.createElement('td');
        const button = document.createElement('button');
        button.type = 'button'; button.className = 'relationship-table-inspect'; button.textContent = 'Inspect';
        Object.entries(dataset).forEach(([key, value]) => { button.dataset[key] = value; });
        button.setAttribute('aria-label', `Inspect ${label}`);
        action.append(button); row.append(action); tableBody.append(row);
    };
    const renderTable = () => {
        tableBody.replaceChildren();
        originalNodes.filter((node) => visibleNodeIds.has(node.id)).forEach((node) => addTableRow(node.label, readable(node.kind), nodeContext(node), {inspectNode: node.id}, focusedNodeIds && !focusedNodeIds.has(node.id)));
        originalEdges.filter((edge) => visibleEdgeIds.has(edge.id)).forEach((edge) => {
            const from = nodeLookup.get(edge.from); const to = nodeLookup.get(edge.to);
            const context = graph.mode === 'shared' ? `${readable(edge.field_name)} · exact approved match · ${edge.confidence}% confidence` : readable(edge.label);
            addTableRow(`${from?.label} → ${to?.label}`, 'Evidence path', context, {inspectEdge: edge.id}, focusedNodeIds && (!focusedNodeIds.has(edge.from) || !focusedNodeIds.has(edge.to)));
        });
    };
    const renderSearchResults = () => {
        searchResults.replaceChildren();
        const query = searchInput.value.trim().toLocaleLowerCase();
        if (!query) return;
        const matches = originalNodes.filter((node) => visibleNodeIds.has(node.id) && [node.label, node.kind, node.field_name, node.case_title, node.evidence_type].filter(Boolean).some((value) => String(value).toLocaleLowerCase().includes(query))).slice(0, 8);
        if (!matches.length) {
            const empty = document.createElement('p'); empty.className = 'relationship-control-help'; empty.textContent = 'No visible nodes found.'; searchResults.append(empty); return;
        }
        matches.forEach((node) => {
            const button = document.createElement('button'); button.type = 'button'; button.className = 'relationship-search-result'; button.dataset.nodeId = node.id;
            const label = document.createElement('span'); label.textContent = node.label;
            const kind = document.createElement('small'); kind.textContent = readable(node.kind);
            button.append(label, kind); searchResults.append(button);
        });
    };
    const updateStatus = () => {
        const notes = [];
        if (focusDepth !== null && selectedNodeId) notes.push(`${focusDepth}-hop focus active; unrelated nodes are faded`);
        if (manuallyHiddenNodes.size) notes.push(`${manuallyHiddenNodes.size} manually hidden`);
        if (activeFields.size < Object.keys(graph.stats.field_counts || {}).length) notes.push('evidence type filter active');
        status.textContent = `Showing ${visibleNodeIds.size} nodes and ${visibleEdgeIds.size} evidence paths.${notes.length ? ` ${notes.join('; ')}.` : ''}`;
        byId('relationshipVisibleNodeCount').textContent = String(visibleNodeIds.size);
        byId('relationshipVisibleEdgeCount').textContent = String(visibleEdgeIds.size);
    };
    const updateVisibility = ({fit = false} = {}) => {
        const candidates = candidateEdges();
        visibleEdgeIds = new Set(candidates.map((edge) => edge.id));
        visibleNodeIds = new Set();
        candidates.forEach((edge) => { visibleNodeIds.add(edge.from); visibleNodeIds.add(edge.to); });
        originalNodes.forEach((node) => { if (node.kind === 'persona' && !manuallyHiddenNodes.has(node.id)) visibleNodeIds.add(node.id); });
        focusedNodeIds = focusDepth !== null && selectedNodeId && visibleNodeIds.has(selectedNodeId) ? neighborhood(selectedNodeId, focusDepth, candidates) : null;
        nodes.update(originalNodes.map((node) => ({id: node.id, hidden: !visibleNodeIds.has(node.id), opacity: focusedNodeIds && !focusedNodeIds.has(node.id) ? 0.16 : 1})));
        edges.update(originalEdges.map((edge) => ({id: edge.id, hidden: !visibleEdgeIds.has(edge.id), color: baseEdgeColor(focusedNodeIds && (!focusedNodeIds.has(edge.from) || !focusedNodeIds.has(edge.to)) ? 0.12 : 0.78)})));
        if (selectedNodeId && !visibleNodeIds.has(selectedNodeId)) {
            selectedNodeId = null; focusDepth = null; focusedNodeIds = null; network.unselectAll(); clearInspector('The selected node is not visible under the current filters.');
        }
        updateControls(); updateStatus(); renderTable(); renderSearchResults();
        if (fit) window.setTimeout(() => network.fit({animation: {duration: 240}}), 0);
    };
    const selectNode = (id, focus = true) => {
        if (!visibleNodeIds.has(id)) return;
        selectedNodeId = id; network.selectNodes([id], false); inspectNode(id); updateVisibility();
        if (focus && !graphView.hidden) network.focus(id, {scale: Math.max(network.getScale(), 1), animation: {duration: 220}});
    };
    const selectEdge = (id) => {
        if (!visibleEdgeIds.has(id)) return;
        selectedNodeId = null; focusDepth = null; network.selectEdges([id]); inspectEdge(id); updateVisibility();
    };

    const unlock = () => nodes.update(originalNodes.map((node) => ({id: node.id, fixed: {x: false, y: false}})));
    const ring = (items, radius, phase = -Math.PI / 2) => items.map((node, index) => {
        const angle = items.length === 1 ? phase : phase + Math.PI * 2 * index / items.length;
        return {id: node.id, x: Math.cos(angle) * radius, y: Math.sin(angle) * radius, fixed: {x: true, y: true}};
    });
    const applyLayout = (layout) => {
        if (layout === 'hierarchical') {
            unlock();
            network.setOptions({layout: {hierarchical: {enabled: true, direction: 'LR', sortMethod: 'directed', levelSeparation: 190, nodeSpacing: 125}}, physics: {enabled: false}, edges: {smooth: {type: 'cubicBezier', forceDirection: 'horizontal'}}});
        } else if (layout === 'concentric') {
            network.setOptions({layout: {hierarchical: false}, physics: {enabled: false}, edges: {smooth: {type: 'curvedCW', roundness: 0.08}}});
            const positions = graph.mode === 'persona'
                ? [...ring(originalNodes.filter((node) => node.kind === 'persona'), 0), ...ring(originalNodes.filter((node) => node.kind === 'claim'), 240), ...ring(originalNodes.filter((node) => node.kind === 'source'), 455)]
                : [...ring(originalNodes.filter((node) => ['attribute', 'organization'].includes(node.kind)), 180), ...ring(originalNodes.filter((node) => node.kind === 'persona'), 390)];
            nodes.update(positions);
        } else {
            unlock();
            network.setOptions({layout: {hierarchical: false, improvedLayout: true, randomSeed: 29}, physics: {enabled: true, solver: 'barnesHut', stabilization: {iterations: 180}, barnesHut: {gravitationalConstant: -4800, springLength: 145, springConstant: 0.035}}, edges: {smooth: {type: 'dynamic'}}});
            network.stabilize(180);
        }
        window.setTimeout(() => network.fit({animation: {duration: 240}}), 20);
    };
    const setView = (view) => {
        const showGraph = view === 'graph';
        graphView.hidden = !showGraph; tableView.hidden = showGraph;
        graphViewButton.classList.toggle('active', showGraph); tableViewButton.classList.toggle('active', !showGraph);
        graphViewButton.setAttribute('aria-pressed', String(showGraph)); tableViewButton.setAttribute('aria-pressed', String(!showGraph));
        if (showGraph) window.setTimeout(() => network.redraw(), 0);
    };

    network.on('selectNode', (event) => { if (event.nodes.length) selectNode(event.nodes[0], false); });
    network.on('selectEdge', (event) => { if (!event.nodes.length && event.edges.length) selectEdge(event.edges[0]); });
    network.once('stabilizationIterationsDone', () => network.fit({animation: false}));
    searchInput.addEventListener('input', renderSearchResults);
    searchInput.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') { searchInput.value = ''; renderSearchResults(); }
        if (event.key === 'Enter') { const first = searchResults.querySelector('[data-node-id]'); if (first) { event.preventDefault(); selectNode(first.dataset.nodeId); } }
    });
    searchResults.addEventListener('click', (event) => { const result = event.target.closest('[data-node-id]'); if (result) selectNode(result.dataset.nodeId); });
    fieldButtons.forEach((button) => button.addEventListener('click', () => {
        const field = button.dataset.relationshipField;
        if (activeFields.has(field)) activeFields.delete(field); else activeFields.add(field);
        button.classList.toggle('active', activeFields.has(field)); button.setAttribute('aria-pressed', String(activeFields.has(field))); updateVisibility({fit: true});
    }));
    focusButtons.forEach((button) => button.addEventListener('click', () => { if (selectedNodeId) { focusDepth = Number(button.dataset.relationshipFocus); updateVisibility(); } }));
    clearFocusButton.addEventListener('click', () => { focusDepth = null; focusedNodeIds = null; updateVisibility(); });
    hideButton.addEventListener('click', () => {
        if (!selectedNodeId) return;
        manuallyHiddenNodes.add(selectedNodeId); selectedNodeId = null; focusDepth = null; focusedNodeIds = null; network.unselectAll(); updateVisibility({fit: true}); clearInspector('The selected node is hidden from this view. Restore it from Graph controls.');
    });
    restoreButton.addEventListener('click', () => { manuallyHiddenNodes.clear(); updateVisibility({fit: true}); });
    byId('fitRelationshipGraph').addEventListener('click', () => network.fit({animation: {duration: 240}}));
    layoutSelect.addEventListener('change', () => applyLayout(layoutSelect.value));
    graphViewButton.addEventListener('click', () => setView('graph'));
    tableViewButton.addEventListener('click', () => setView('table'));
    tableBody.addEventListener('click', (event) => {
        const nodeButton = event.target.closest('[data-inspect-node]'); const edgeButton = event.target.closest('[data-inspect-edge]');
        if (nodeButton) selectNode(nodeButton.dataset.inspectNode, false); if (edgeButton) selectEdge(edgeButton.dataset.inspectEdge);
    });
    graphElement.addEventListener('keydown', (event) => {
        if ((event.key === 'h' || event.key === 'H') && selectedNodeId) { event.preventDefault(); hideButton.click(); }
        if (event.key === 'Escape' && focusDepth !== null) { event.preventDefault(); clearFocusButton.click(); }
    });

    updateVisibility();
    applyLayout('force');
})();
