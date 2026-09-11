// Execute the rendered form's own route functions against a small offline DOM.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2], 'utf8');
new vm.Script(source); // Check syntax for the entire rendered script, including handlers.
const functions = source.slice(source.indexOf('function comparisonKey('), source.indexOf('function aliasPlanningPayload('))
    + source.slice(source.indexOf('function selectedAliases('), source.indexOf('function updateAliasCount('))
    + source.slice(source.indexOf('function identifierValues('), source.indexOf('function updateSourceTagSelects('));
const element = () => ({ textContent: '', checked: false, disabled: false, hidden: false });
const nodes = {};
for (const key of ['maigret', 'native', 'scanner-usernames', 'scanner-email', 'ai']) {
    const cells = Object.fromEntries(['status', 'reason', 'inputs'].map(key => [`.routing-${key}`, element()]));
    nodes[`routing-${key}`] = { querySelector: selector => cells[selector] };
}
nodes['mode-subject'] = { checked: true };
nodes['mode-exhaustive'] = { checked: false };
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
    collectorStatus: { discovery_enabled: true, maigret_enabled: true, focused_enabled: true, exhaustive_enabled: true,
        scanner_enabled: true, native_search: {enabled: false, reason: 'Disabled by server policy.'} }
};
vm.createContext(state);
vm.runInContext(functions, state);
const update = () => vm.runInContext('updatePlan()', state);
const cell = (key, kind) => nodes[`routing-${key}`].querySelector(`.routing-${kind}`).textContent;

update();
assert.equal(state.count.textContent, '2 username targets');
assert.equal(cell('maigret', 'inputs'), 'alice · alice.example');
assert.equal(cell('native', 'status'), 'Off');
assert.equal(cell('native', 'inputs'), 'None');
assert.equal(cell('scanner-usernames', 'status'), 'Off');
assert.equal(cell('scanner-email', 'status'), 'Off');
assert.equal(cell('ai', 'status'), 'Consent off');
assert.equal(cell('ai', 'inputs'), 'None');
assert.match(state.note.textContent, /One Persona/);

state.collectorStatus.native_search = {enabled: true, reason: 'Provider configured; credentials are checked at collection.'};
update();
assert.equal(cell('native', 'status'), 'Configured');
assert.equal(cell('native', 'inputs'), 'alice · alice.example · "Alice Example"');
assert.doesNotMatch(cell('native', 'inputs'), /@example.com|1234567890|unchecked-alias/);

state.userScannerUsernameToggle.checked = true;
state.userScannerToggle.checked = true;
update();
assert.equal(cell('scanner-usernames', 'inputs'), 'alice · alice.example');
assert.match(cell('scanner-usernames', 'reason'), /Platforms: instagram/);
assert.match(cell('scanner-usernames', 'reason'), /X is blocked/);
assert.equal(cell('scanner-email', 'inputs'), 'alice@example.com');
state.userScannerPlatforms[0].checked = false;
update();
assert.equal(cell('scanner-usernames', 'status'), 'No platforms');
assert.equal(cell('scanner-usernames', 'inputs'), 'None');
state.vxtwitterToggle.checked = true;
update();
assert.match(cell('scanner-usernames', 'reason'), /Platforms: x/);

state.aiContextToggle.checked = true;
update();
assert.equal(cell('ai', 'status'), 'Allowed when requested');
assert.match(cell('ai', 'inputs'), /Alice Example.*@alice.*alice@example.com.*1234567890/);
state.aiWebEnabled = false;
update();
assert.equal(cell('ai', 'status'), 'Allowed when requested');
assert.match(cell('ai', 'inputs'), /alice@example.com/);
assert.match(cell('ai', 'reason'), /Cited web research is off/);
state.aiConnected = false;
update();
assert.equal(cell('ai', 'inputs'), 'None');
assert.equal(cell('ai', 'status'), 'Unavailable');

nodes['mode-subject'].checked = false;
update();
assert.equal(state.userScannerToggle.checked, false);
assert.equal(state.userScannerToggle.disabled, true);
assert.equal(cell('scanner-email', 'inputs'), 'None');
assert.match(cell('scanner-email', 'reason'), /Requires One subject/);
assert.match(state.note.textContent, /generated aliases stay with their source subject/);

state.sourceTags = [{dataset: {value: 'social'}, classList: {contains: value => value === 'selected'}}];
nodes['mode-exhaustive'].checked = true;
update();
assert.match(cell('maigret', 'reason'), /Every eligible enabled site.*Include: social/);
state.collectorStatus.exhaustive_enabled = false;
update();
assert.equal(cell('maigret', 'status'), 'Unavailable');
assert.equal(cell('native', 'inputs'), 'None');
assert.equal(cell('scanner-usernames', 'inputs'), 'None');

state.collectorStatus.exhaustive_enabled = true;
identifiers = [row('username', 'https://alice.wordpress.com/')];
aliases = [];
state.plannedExactTargets = ['alice'];
state.exactPlanResolved = true;
update();
assert.equal(cell('maigret', 'inputs'), 'alice');
assert.equal(state.count.textContent, '1 username target');
state.plannedExactTargets = [];
identifiers = [row('full_name', 'Alice Example')];
update();
assert.equal(state.count.textContent, '0 username targets');
assert.equal(cell('maigret', 'status'), 'Needs a username');
assert.equal(cell('native', 'inputs'), '"Alice Example"');
state.scannerAvailable = false;
update();
assert.equal(cell('scanner-usernames', 'status'), 'Unavailable');
assert.match(cell('scanner-usernames', 'reason'), /not installed/);
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
    assert.equal(cell('maigret', 'inputs'), 'Alice');
    identifiers = [row('username', 'Groß'), row('username', 'gross')];
    await vm.runInContext('refreshAliasCandidates()', state);
    update();
    assert.equal(requests, 2); // Exact usernames still use server normalization when aliases are off.
    assert.equal(cell('maigret', 'inputs'), 'Groß');
    assert.equal(state.count.textContent, '1 username target');
    identifiers = [row('full_name', 'Alice Example')];
    await vm.runInContext('refreshAliasCandidates()', state);
    assert.equal(requests, 2); // No username, URL, or generated alias needs planning.
    console.log('routing DOM scenarios passed');
})().catch(error => {console.error(error); process.exitCode = 1;});
