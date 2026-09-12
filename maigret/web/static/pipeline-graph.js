(() => {
  'use strict';
  const payload = document.getElementById('pipeline-graph-data');
  if (!payload) return;
  const graph = JSON.parse(payload.textContent);
  const container = document.getElementById('pipeline-graph');
  const detail = document.getElementById('pipeline-graph-detail');
  const status = document.getElementById('pipeline-graph-status');
  if (!window.vis) { status.textContent = 'Graph renderer unavailable. The complete graph JSON and version source register remain accessible.'; return; }
  const text = value => typeof value === 'string' ? value : JSON.stringify(value);
  const facts = graph.nodes.filter(node => ['account', 'claim'].includes(node.kind));
  const nodesById = new Map(graph.nodes.map(node => [node.id, node]));
  const edgesByGroup = new Map();
  graph.edges.forEach((edge, index) => {
    const key = 'group:' + edge.group_id;
    if (!edgesByGroup.has(key)) edgesByGroup.set(key, []);
    edgesByGroup.get(key).push({...edge, id: index});
  });
  let factPage = 0, sourcePage = 0, query = '';
  const factsPerPage = 40, sourcesPerPage = 200;
  const dataNodes = new vis.DataSet(), dataEdges = new vis.DataSet();
  const network = new vis.Network(container, {nodes: dataNodes, edges: dataEdges}, {
    interaction: {hover: true}, physics: {stabilization: {iterations: 70}, solver: 'barnesHut'},
    nodes: {font: {face: 'Arial', size: 12}, widthConstraint: {maximum: 220}}, edges: {smooth: false}
  });
  const controls = document.createElement('nav'); controls.className = 'pipeline-pager'; controls.setAttribute('aria-label', 'Graph pages');
  function button(label, handler) { const result = document.createElement('button'); result.type = 'button'; result.className = 'btn btn-outline-secondary'; result.textContent = label; result.addEventListener('click', handler); controls.append(result); return result; }
  const previousFacts = button('Previous facts', () => {factPage -= 1; sourcePage = 0; render();});
  const nextFacts = button('Next facts', () => {factPage += 1; sourcePage = 0; render();});
  const previousSources = button('Previous sources', () => {sourcePage -= 1; render();});
  const nextSources = button('Next sources', () => {sourcePage += 1; render();});
  container.after(controls);
  function render() {
    const matches = query ? new Set(graph.nodes.filter(node => `${text(node.label)} ${node.id}`.toLowerCase().includes(query)).map(node => node.id)) : null;
    const eligible = matches ? facts.filter(node => matches.has(node.id) || (edgesByGroup.get(node.id) || []).some(edge => matches.has(edge.from))) : facts;
    const visibleFacts = eligible.slice(factPage * factsPerPage, (factPage + 1) * factsPerPage);
    const candidateEdges = visibleFacts.flatMap(node => edgesByGroup.get(node.id) || []);
    const isObservationEdge = edge => nodesById.get(edge.from)?.kind === 'observation';
    const sourceIds = [...new Set(candidateEdges.filter(isObservationEdge).map(edge => edge.from))];
    const visibleSourceIds = new Set(sourceIds.slice(sourcePage * sourcesPerPage, (sourcePage + 1) * sourcesPerPage));
    const subject = graph.nodes.find(node => node.kind === 'subject');
    const visibleNodes = [subject, ...visibleFacts, ...[...visibleSourceIds].map(id => nodesById.get(id))].filter(Boolean);
    const semanticLinks = new Set(candidateEdges.filter(edge => edge.kind !== 'provenance').map(edge => JSON.stringify([edge.from, edge.to])));
    const visibleEdges = candidateEdges.filter(edge =>
      (!isObservationEdge(edge) || visibleSourceIds.has(edge.from)) &&
      (edge.kind !== 'provenance' || !semanticLinks.has(JSON.stringify([edge.from, edge.to])))
    );
    dataNodes.clear(); dataEdges.clear();
    dataNodes.add(visibleNodes.map(node => ({...node, label: text(node.label), shape: node.kind === 'subject' ? 'diamond' : node.kind === 'observation' ? 'dot' : 'box', color: node.kind === 'subject' ? '#bfd8e8' : node.kind === 'observation' ? '#e6eeea' : '#f2eddf'})));
    dataEdges.add(visibleEdges.map(edge => ({...edge, arrows: 'to',
      label: edge.kind === 'curated_fact' ? '' : edge.kind.replaceAll('_', ' '),
      color: edge.kind === 'contradicts' ? '#b44b4b' : edge.kind === 'supports' ? '#47765e' : '#8b9bab',
      dashes: ['excluded_evidence', 'historical_evidence', 'collection_outcome', 'absence_observation'].includes(edge.kind)
    })));
    previousFacts.disabled = factPage === 0; nextFacts.disabled = (factPage + 1) * factsPerPage >= eligible.length;
    previousSources.disabled = sourcePage === 0; nextSources.disabled = (sourcePage + 1) * sourcesPerPage >= sourceIds.length;
    status.textContent = `Showing ${visibleFacts.length} of ${eligible.length} ${query ? 'matching' : 'total'} facts (page ${factPage + 1}), and ${visibleSourceIds.size} of ${sourceIds.length} source observations (source page ${sourcePage + 1}). Edge labels distinguish support, contradiction, absence, collection outcomes and excluded evidence. The complete register and JSON retain all ${graph.item_count} facts and ${graph.observation_count} observations.`;
    network.fit({animation: false});
  }
  network.on('click', event => {
    detail.replaceChildren();
    const selected = event.nodes.length ? nodesById.get(event.nodes[0]) : event.edges.length ? graph.edges[event.edges[0]] : null;
    if (!selected) return;
    const title = document.createElement('h3'); title.textContent = selected.kind.replaceAll('_', ' ');
    const pre = document.createElement('pre'); pre.textContent = JSON.stringify(selected, null, 2);
    detail.append(title, pre);
  });
  document.getElementById('pipeline-graph-search').addEventListener('input', event => {
    query = event.target.value.trim().toLowerCase(); factPage = 0; sourcePage = 0; render();
  });
  render();
})();
