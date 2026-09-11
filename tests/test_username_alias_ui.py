from pathlib import Path


TEMPLATE_PATH = (
    Path(__file__).resolve().parents[1] / "maigret" / "web" / "templates" / "index.html"
)


def test_alias_context_refresh_preserves_analyst_choices():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    context_handler = template.index(
        "[aliasNicknameInput, aliasContextNumberInput].forEach"
    )
    context_handler_end = template.index("}));", context_handler)
    assert (
        "refreshAliasCandidates({ preserveAnalystChoices: true })"
        in template[context_handler:context_handler_end]
    )
    assert "row.dataset.analystChanged = 'true'" in template
    assert "row.dataset.aliasEdited = 'true'" in template
    assert "function mergeAnalystAliasChoices(" in template
    assert "removed: !row.querySelector('.alias-value').value.trim()" in template
    assert "const removedAliasChoices = new Map();" in template
    assert "let plannedAliasKeysByComparison = new Map();" in template
    assert "const choicesByGeneratedKey = new Map(removedAliasChoices);" in template
    assert "matchedAliasKeys: Array.from(row._matchedAliasKeys || [])" in template
    assert "const expandedChoices = analystChoices.map(candidate =>" in template
    assert "const equalValueKeys = plannedAliasKeysByComparison.get(" in template
    assert "choicesByGeneratedKey.set(key, candidate);" in template
    assert "if (removedKeys.has(generatedKey)) return null;" in template
    assert "matchedKeys.forEach(key => row._matchedAliasKeys.add(key));" in template
    assert "removedAliasChoices.set(choice.generatedKey, choice);" in template
    assert "if (!preserveAnalystChoices) removedAliasChoices.clear();" in template
    assert "if (choice.removed) return null;" in template


def test_profile_url_reranking_preserves_analyst_choices():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "const aliasResetTypes = new Set(['full_name']);" in template
    assert "function refreshAliasesForIdentifierChanges(...types)" in template
    helper_start = template.index("function refreshAliasesForIdentifierChanges(")
    helper_end = template.index("function identifierValues(", helper_start)
    helper = template[helper_start:helper_end]
    assert "aliasResetTypes.has(type)" in helper
    assert "aliasSourceTypes.has(type)" in helper
    assert "refreshAliasCandidates({ preserveAnalystChoices: true })" in helper
    value_handler = template.index(
        "row.querySelector('.identifier-value').addEventListener('input'"
    )
    value_handler_end = template.index("});", value_handler)
    assert (
        "refreshAliasesForIdentifierChanges(type.value)"
        in template[value_handler:value_handler_end]
    )


def test_browser_uses_the_server_alias_planner():
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    assert "fetch('/api/username-aliases'" in template
    assert "'X-OpenLedger-CSRF': csrfToken" in template
    assert "profile_urls: profileUrls" in template
    assert "function cleanNameTokens(" not in template
    assert ".toLocaleLowerCase(" not in template
    assert "if (aliasRefreshPending)" in template
