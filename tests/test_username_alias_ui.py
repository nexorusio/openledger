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
