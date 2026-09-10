"""P3R browser-facing collection-accounting regressions."""

from pathlib import Path
import subprocess

from maigret.web import app as web_app_module


ROOT = Path(__file__).resolve().parents[1]
LIVE_TEMPLATE = ROOT / "maigret" / "web" / "templates" / "live.html"
RESULTS_TEMPLATE = ROOT / "maigret" / "web" / "templates" / "results.html"
ACCOUNTING_SCRIPT = ROOT / "maigret" / "web" / "static" / "collection-progress.js"


def test_live_template_uses_distinct_maigret_and_user_scanner_labels():
    template = LIVE_TEMPLATE.read_text(encoding="utf-8")

    assert "Maigret site checks" in template
    assert "Maigret supported profiles" in template
    assert "Maigret review candidates" in template
    assert "User Scanner usernames found" in template
    assert "User Scanner email registrations" in template
    assert "Declared collection accounting" in template
    assert "Coverage accounting is unavailable for this legacy saved run." in template
    assert "Streaming" not in template
    assert template.count('id="stat-checked"') == 1
    assert template.count('id="stat-total"') == 1


def test_accounting_renderer_renders_safe_dom_and_ignores_replayed_revisions():
    script = r'''
const fs = require('fs');
class Element {
  constructor() { this.children = []; this.hidden = false; this.textContent = ''; this.colSpan = 0; }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren(...children) { this.children = children; }
}
const elements = Object.fromEntries([
  'collection-accounting', 'collection-accounting-state', 'collection-accounting-body',
  'collection-accounting-unavailable'
].map(id => [id, new Element()]));
global.window = {};
global.document = {
  getElementById: id => elements[id] || null,
  createElement: () => new Element(),
};
eval(fs.readFileSync(process.argv[1], 'utf8'));
const renderer = window.CollectionProgress.create({
  panelId: 'collection-accounting', stateId: 'collection-accounting-state',
  bodyId: 'collection-accounting-body', unavailableId: 'collection-accounting-unavailable',
});
const current = {
  schema_version: 1, revision: 5, known: true, state: 'partial',
  stages: [{engine_id: 'maigret', label: '<unsafe>', unit: 'site_checks', status: 'partial',
    planned: 4, terminal: 3, errors: 1, timeouts: 0, unattempted: 1, unknown: null}]
};
const duplicate = {...current};
const older = {...current, revision: 4, state: 'completed'};
const row = () => elements['collection-accounting-body'].children[0].children.map(cell => cell.textContent);
console.log(JSON.stringify([
  renderer.apply(current), row(), elements['collection-accounting-state'].textContent,
  renderer.apply(duplicate), renderer.apply(older), renderer.hasKnownSnapshot()
]));
'''
    completed = subprocess.run(
        ["node", "-e", script, str(ACCOUNTING_SCRIPT)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == (
        '[true,["Maigret","site checks","Partial","—","3 / 4","1","0","1","—"],'
        '"Partial",false,false,true]'
    )


def test_accounting_renderer_keeps_engine_units_and_outcomes_visible():
    script = ACCOUNTING_SCRIPT.read_text(encoding="utf-8")

    for required in (
        "site checks",
        "queries",
        "targets",
        "invocations",
        "terminal",
        "planned",
        "errors",
        "timeouts",
        "unattempted",
        "unknown",
        "revision <= latestRevision",
        "typeof value === 'number'",
        "not_selected",
        "stage_budget_exhausted",
        "not_admitted",
    ):
        assert required in script


def test_results_template_preserves_declared_accounting_without_mixing_units():
    template = RESULTS_TEMPLATE.read_text(encoding="utf-8")
    web_app_module.app.jinja_env.get_template("results.html")

    assert "Declared collection accounting" in template
    assert "Each source keeps its own unit" in template
    assert "Coverage accounting is unavailable for this legacy saved run." in template
    assert "engine_labels.get" in template
    assert "unit_labels.get" in template
    assert "Terminal / planned" in template
