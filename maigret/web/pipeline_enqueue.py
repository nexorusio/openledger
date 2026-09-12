"""Persist primary query plans in the same transaction as their case and job."""

from copy import deepcopy


def enqueue_primary_requests(store, connection, *, job_id, case_id, options, bindings):
    from maigret.web.pipeline_query import build_query_plan, runtime_source_status
    from maigret.web.pipeline_store import PipelineStore
    from maigret.web.investigation_input import IDENTIFIER_TYPES

    pipeline = PipelineStore(store)
    saved_spec = options.get("investigation_spec") or {}
    status = options.get("pipeline_source_status") or runtime_source_status()
    results = []
    for binding in bindings:
        spec = deepcopy(saved_spec)
        identifiers = binding.get("identifiers")
        if identifiers is None:
            identifiers = [
                {"type": "username", "value": value}
                for value in binding.get("usernames", [])
            ]
        spec["identifiers"] = identifiers
        if any(
            not isinstance(item, dict)
            or item.get('type') not in IDENTIFIER_TYPES
            or not isinstance(item.get('value'), str)
            or not item['value'].strip()
            for item in identifiers
        ):
            raise ValueError(
                'Every primary query input must have a supported type and nonempty value'
            )
        bound = {str(value).casefold() for value in binding.get("usernames", [])}
        spec["search_targets"] = [
            item
            for item in spec.get("search_targets", [])
            if str(item.get("value", "")).casefold() in bound
        ]
        if not spec["search_targets"]:
            spec["search_targets"] = [
                {"value": value, "source_type": "username", "source_value": value}
                for value in binding.get("usernames", [])
            ]
        spec.update(
            processing_mode="same_subject",
            target_persona_id=binding["persona_id"],
            subject_label=binding["subject_label"],
            persona_bindings=[binding],
        )
        context = {"collection_options": options}
        plan = build_query_plan(
            spec,
            case_id=case_id,
            subject_id=binding["persona_id"],
            source_status=status,
            context=context,
        )
        results.append(
            pipeline.create_request_with_connection(
                connection,
                case_id,
                binding["persona_id"],
                plan["inputs"],
                plan,
                actor=options.get("requested_by") or "case-operator",
                job_id=job_id,
                idempotency_key="primary:" + job_id + ":" + binding["persona_id"],
            )
        )
    return results
