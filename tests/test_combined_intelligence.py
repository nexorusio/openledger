from maigret.web.combined_intelligence import (
    bounded_combined_context,
    normalize_combined_insights,
    overlay_relationship_proposals,
)


def _context():
    return {
        "purpose": "Determine whether Alice and Bob publish for the same network.",
        "snapshot_sha256": "a" * 64,
        "source_cases": [
            {"id": "case-a", "title": "Alice"},
            {"id": "case-b", "title": "Bob"},
        ],
        "entities": [
            {
                "reference_id": "persona:alice",
                "entity_type": "persona",
                "entity_id": "alice",
                "label": "Alice",
                "case_id": "case-a",
                "case_title": "Alice",
            },
            {
                "reference_id": "persona:bob",
                "entity_type": "persona",
                "entity_id": "bob",
                "label": "Bob",
                "case_id": "case-b",
                "case_title": "Bob",
            },
        ],
        "approved_claims": [
            {
                "reference_id": "claim:alice-site",
                "claim_id": "alice-site",
                "case_id": "case-a",
                "case_title": "Alice",
                "persona_id": "alice",
                "persona_name": "Alice",
                "field_name": "website",
                "display_value": "https://news.example/alice",
                "confidence": 80,
                "sources": [
                    {
                        "name": "Alice profile",
                        "url": "https://news.example/alice",
                        "type": "profile",
                    }
                ],
            },
            {
                "reference_id": "claim:bob-site",
                "claim_id": "bob-site",
                "case_id": "case-b",
                "case_title": "Bob",
                "persona_id": "bob",
                "persona_name": "Bob",
                "field_name": "website",
                "display_value": "https://news.example/bob",
                "confidence": 75,
                "sources": [
                    {
                        "name": "Bob profile",
                        "url": "https://news.example/bob",
                        "type": "profile",
                    }
                ],
            },
        ],
        "approved_organizations": [],
        "truncated_claim_count": 0,
    }


def _raw_proposal(**overrides):
    proposal = {
        "title": "Shared publication network",
        "relationship_type": "publication_connection",
        "subject_ref": "persona:alice",
        "object_ref": "persona:bob",
        "explanation": "Both approved profiles are attributed to the same network.",
        "confidence": 72,
        "evidence_reference_ids": [
            "claim:alice-site",
            "claim:bob-site",
            "web:1",
        ],
        "contradictory_reference_ids": [],
        "limitations": ["Publishing on one network does not prove coordination."],
    }
    proposal.update(overrides)
    return {
        "executive_summary": "The cases share a plausible publication connection.",
        "key_findings": [
            {
                "summary": "Both approved profiles use the same publishing network.",
                "reference_ids": ["claim:alice-site", "claim:bob-site"],
            }
        ],
        "contradictions": [],
        "information_gaps": ["Editorial control is not established."],
        "next_steps": ["Compare public author and masthead pages."],
        "proposals": [proposal],
    }


def test_combined_insights_require_approved_anchors_from_both_cases():
    normalized = normalize_combined_insights(
        _raw_proposal(),
        context=_context(),
        web_sources=[
            {
                "title": "Network masthead",
                "url": "https://news.example/masthead",
            }
        ],
    )

    assert normalized["executive_summary"].startswith("The cases share")
    assert len(normalized["proposals"]) == 1
    proposal = normalized["proposals"][0]
    assert proposal["subject_entity"]["case_id"] == "case-a"
    assert proposal["object_entity"]["case_id"] == "case-b"
    assert [item["reference_id"] for item in proposal["evidence"]] == [
        "claim:alice-site",
        "claim:bob-site",
        "web:1",
    ]
    assert proposal["evidence"][2]["url"] == "https://news.example/masthead"

    missing_second_case = _raw_proposal(
        evidence_reference_ids=["claim:alice-site", "web:1"]
    )
    assert (
        normalize_combined_insights(
            missing_second_case,
            context=_context(),
            web_sources=[
                {
                    "title": "Network masthead",
                    "url": "https://news.example/masthead",
                }
            ],
        )["proposals"]
        == []
    )


def test_combined_insights_reject_same_case_and_overconfident_hypotheses():
    context = _context()
    context["entities"].append(
        {
            "reference_id": "persona:alice-alt",
            "entity_type": "persona",
            "entity_id": "alice-alt",
            "label": "Alice alt",
            "case_id": "case-a",
            "case_title": "Alice",
        }
    )
    same_case = _raw_proposal(object_ref="persona:alice-alt")
    overconfident = _raw_proposal(confidence=95)

    assert (
        normalize_combined_insights(same_case, context=context, web_sources=[])[
            "proposals"
        ]
        == []
    )
    assert (
        normalize_combined_insights(overconfident, context=context, web_sources=[])[
            "proposals"
        ]
        == []
    )


def test_combined_insights_do_not_turn_contact_evidence_into_org_fact():
    context = _context()
    context["entities"][1] = {
        "reference_id": "organization:case-b",
        "entity_type": "organization",
        "entity_id": "organization:case-b",
        "label": "News Example",
        "case_id": "case-b",
        "case_title": "News Example",
    }
    context["approved_organizations"] = [
        {
            "reference_id": "organization-evidence:case-b",
            "case_id": "case-b",
            "case_title": "News Example",
            "entity_ref": "organization:case-b",
            "label": "News Example",
            "source_name": "Official site",
            "source_url": "https://news.example/",
        }
    ]
    context["approved_claims"][0]["field_name"] = "email"
    context["approved_claims"][0]["display_value"] = "alice@news.example"
    raw = _raw_proposal(
        object_ref="organization:case-b",
        relationship_type="affiliation",
        evidence_reference_ids=[
            "claim:alice-site",
            "organization-evidence:case-b",
        ],
    )

    assert (
        normalize_combined_insights(raw, context=context, web_sources=[])["proposals"]
        == []
    )


def test_bounded_context_balances_cases_and_tracks_omissions():
    context = _context()
    context["approved_claims"] *= 30

    bounded = bounded_combined_context(context, maximum_chars=3000)

    assert {item["case_id"] for item in bounded["approved_claims"][:2]} == {
        "case-a",
        "case-b",
    }
    assert bounded["truncated_claim_count"] > 0


def test_relationship_overlay_keeps_pending_dashed_semantics_and_rejections_out():
    normalized = normalize_combined_insights(
        _raw_proposal(), context=_context(), web_sources=[]
    )["proposals"][0]
    normalized.update({"id": "proposal-1", "review_status": "pending"})
    rejected = {**normalized, "id": "proposal-2", "review_status": "rejected"}
    graph = {
        "mode": "shared",
        "nodes": [],
        "edges": [],
        "stats": {"field_counts": {}},
    }

    overlaid = overlay_relationship_proposals(graph, [normalized, rejected])

    assert len(overlaid["nodes"]) == 2
    assert len(overlaid["edges"]) == 1
    assert overlaid["edges"][0]["review_status"] == "pending"
    assert overlaid["stats"]["ai_proposal_counts"] == {
        "pending": 1,
        "approved": 0,
        "uncertain": 0,
    }
