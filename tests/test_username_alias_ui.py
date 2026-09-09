from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / "maigret" / "web" / "templates" / "index.html"
SCRIPT_PATH = ROOT / "maigret" / "web" / "static" / "investigation-builder.js"
STYLE_PATH = ROOT / "maigret" / "web" / "static" / "openledger.css"


def test_unified_builder_replaces_typed_rows_and_legacy_advanced_controls():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert 'id="investigation-token-input"' in template
    assert 'name="investigation_token"' in template
    assert "data-preview-url=\"{{ url_for('api_investigation_plan_preview') }}\"" in (
        template
    )
    assert 'name="identifier_type"' not in template
    assert 'name="identifier_value"' not in template
    assert 'name="processing_mode"' not in template
    assert 'name="tags"' not in template
    assert 'name="excluded_tags"' not in template
    assert 'name="allow_ai_context"' not in template
    assert 'name="enable_archived_url_evidence"' not in template


def test_browser_commits_whole_nonempty_values_and_preserves_empty_tab_navigation():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "if (event.key !== 'Enter' && event.key !== 'Tab') return;" in script
    empty_branch = script.index("if (!draft) {")
    prevent_commit = script.index("event.preventDefault();\n        commitDraft();")
    assert (
        "if (event.key === 'Enter') event.preventDefault();"
        in script[empty_branch:prevent_commit]
    )
    assert "return;" in script[empty_branch:prevent_commit]
    assert (
        "split("
        not in script[script.index("async function commitDraft()") : prevent_commit]
    )


def test_token_editor_exposes_edit_remove_focus_cancel_and_error_behaviour():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    script = SCRIPT_PATH.read_text(encoding="utf-8")
    styles = STYLE_PATH.read_text(encoding="utf-8")

    assert 'aria-live="polite"' in template
    assert 'role="alert" tabindex="-1"' in template
    assert "function beginEdit(index)" in script
    assert "function cancelEdit()" in script
    assert "function removeToken(index)" in script
    assert "input.focus();" in script
    assert "input.select();" in script
    assert "Use no more than ${maximumTokens} investigation values." in script
    assert "setAttribute('aria-invalid'" in script
    assert ".token-chip-edit:focus-visible" in styles
    assert ".token-editor:focus-within" in styles
    assert "@media (max-width: 767px)" in styles


def test_browser_uses_only_the_authoritative_plan_preview_for_classification():
    script = SCRIPT_PATH.read_text(encoding="utf-8")

    assert "fetch(form.dataset.previewUrl" in script
    assert "'X-OpenLedger-CSRF': csrfToken" in script
    assert "tokens: tokens.map(token => token.value)" in script
    assert "payload.tokens || []" in script
    assert "routePlan.effective_routes" in script
    assert "routePlan.skipped_routes" in script
    assert "classifyToken" not in script


def test_builder_has_one_minimal_alias_option_and_quick_full_controls():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert template.count('name="search_likely_username_aliases"') == 1
    assert "Search likely username aliases" in template
    assert 'name="alias_nicknames"' not in template
    assert 'name="alias_context_numbers"' not in template
    assert 'name="alias_candidate"' not in template
    assert 'name="mode" id="mode-quick" value="quick"' in template
    assert 'name="mode" id="mode-full" value="full"' in template
