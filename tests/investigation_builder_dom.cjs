// Execute the rendered form's own route functions against a small offline DOM.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8');
new vm.Script(source); // Check syntax for the entire rendered script, including handlers.
const functions = source.slice(source.indexOf('function comparisonKey('), source.indexOf('function aliasPlanningPayload('))
    + source.slice(source.indexOf('function selectedAliases('), source.indexOf('function updateAliasCount('))
    + source.slice(source.indexOf('function identifierValues('), source.indexOf('function showRoute('))
    + source.slice(source.indexOf('function updatePlan('), source.indexOf('function updateSourceTagSelects('));
const element = () => ({ textContent: '', checked: false, disabled: false, hidden: false });
const nodes = {};
nodes['mode-subject'] = { value: 'same_subject' };
const row = (type, value) => ({querySelector: selector => selector === '.identifier-type' ? {value: type} : {value}});
let identifiers = [row('full_name', 'Alice Example'), row('username', '@alice'), row('email', 'alice@example.com'), row('phone', '+1234567890')];
let aliases = [{value: 'alice.example', checked: true}, {value: 'unchecked-alias', checked: false}];
const state = {
    document: { getElementById: id => nodes[id], createElement: () => element() },
    list: { querySelectorAll: () => identifiers },
    aliasList: {querySelectorAll: () => aliases.map(alias => ({querySelector: () => alias}))},
    plannedExactTargets: [], exactPlanResolved: false, plannedTargetKeys: new Map(),
    count: element(), summary: element(), targets: {replaceChildren(...children) {this.children = children;}},
    note: element(), error: element(), aliasRefreshPending: false, aliasPlanError: '',
    userScannerToggle: element(), userScannerUsernameToggle: element(), vxtwitterToggle: element(),
    userScannerPlatforms: [{value: 'instagram', checked: true}, {value: 'x', checked: true}],
    aiContextToggle: element(), githubEnrichmentToggle: element(), sourceTags: [],
    scannerAvailable: true, aiConnected: true, aiWebEnabled: true, focusedSiteLimit: 500,
    requestPipelinePlan: () => {},
    collectorStatus: { discovery_enabled: true, maigret_enabled: true, focused_enabled: true, exhaustive_enabled: true,
        scanner_enabled: true, native_search: {enabled: false, reason: 'Disabled by server policy.'} }
};
vm.createContext(state);
vm.runInContext(functions, state);
const update = () => vm.runInContext('updatePlan()', state);

update();
assert.equal(state.count.textContent, '2 username targets');
assert.deepEqual(Array.from(state.targets.children, child => child.textContent), ['alice', 'alice.example']);
assert.match(state.note.textContent, /One Persona/);

state.userScannerUsernameToggle.checked = true;
state.userScannerToggle.checked = true;
update();
assert.equal(state.userScannerToggle.disabled, false);
identifiers = [row('username', 'https://alice.wordpress.com/')];
aliases = [];
state.plannedExactTargets = ['alice'];
state.exactPlanResolved = true;
update();
assert.equal(state.count.textContent, '1 username target');
state.plannedExactTargets = [];
identifiers = [row('full_name', 'Alice Example')];
update();
assert.equal(state.count.textContent, '0 username targets');
// Exercise the actual async planner path with alias generation disabled.
const plannerFunctions = source.slice(source.indexOf('function aliasPlanningPayload('), source.indexOf('function applyScannerSelectionBudget('))
    + source.slice(source.indexOf('function refreshAliasCandidates('), source.indexOf('function refreshAliasesForIdentifierChanges('));
Object.assign(state, {
    generateToggle: {checked: false}, aliasNicknameInput: {value: ''}, aliasContextNumberInput: {value: ''},
    aliasRefreshSequence: 0, aliasRefreshController: null, exactTargetKeys: new Set(),
    removedAliasChoices: new Map(), plannedAliasKeysByComparison: new Map(),
    renderAliasCandidates: values => {state.lastRenderedAliases = values;},
    applyScannerSelectionBudget: values => values, AbortController, csrfToken: 'local-csrf'
});
state.aliasList.setAttribute = () => {};
state.aliasList.removeAttribute = () => {};
let requests = 0;
state.fetch = async (url, options) => {
    requests++;
    assert.equal(url, '/api/username-aliases');
    const payload = JSON.parse(options.body);
    assert.deepEqual(payload.full_names, []);
    if (requests === 1) assert.deepEqual(payload.profile_urls, ['https://alice.wordpress.com/']);
    const exact = requests === 1 ? ['Alice'] : ['Groß'];
    return {ok: true, json: async () => ({exact_targets: exact, exact_target_keys: requests === 1 ? ['alice'] : ['gross'], aliases: [{value: 'must-not-render'}]})};
};
vm.runInContext(plannerFunctions, state);
(async () => {
    identifiers = [row('username', 'https://alice.wordpress.com/'), row('full_name', 'Alice Example')];
    await vm.runInContext('refreshAliasCandidates()', state);
    assert.equal(requests, 1);
    assert.deepEqual(Array.from(state.plannedExactTargets), ['Alice']);
    assert.equal(state.lastRenderedAliases.length, 0);
    identifiers = [row('username', 'Groß'), row('username', 'gross')];
    await vm.runInContext('refreshAliasCandidates()', state);
    update();
    assert.equal(requests, 2); // Exact usernames still use server normalization when aliases are off.
    assert.deepEqual(Array.from(state.plannedExactTargets), ['Groß']);
    assert.equal(state.count.textContent, '1 username target');
    identifiers = [row('full_name', 'Alice Example')];
    await vm.runInContext('refreshAliasCandidates()', state);
    assert.equal(requests, 2); // No username, URL, or generated alias needs planning.
    console.log('single-Persona builder DOM scenarios passed');
})().catch(error => {console.error(error); process.exitCode = 1;});
