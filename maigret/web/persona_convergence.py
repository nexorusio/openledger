"""Explicit operator command for legacy-to-P2 Persona convergence."""

from __future__ import annotations

import argparse
import json

from sqlalchemy import select

from maigret.web.case_store import CaseStore, database_url_from_environment, personas
from maigret.web.pipeline_ingestion import converge_legacy_persona

CONFIRM_TOKEN = "CONVERGE-LEGACY-PERSONAS"


def converge_all(store, *, apply=False):
    with store.engine.connect() as connection:
        scopes = [
            dict(row)
            for row in connection.execute(
                select(personas.c.case_id, personas.c.id.label("persona_id")).order_by(
                    personas.c.case_id, personas.c.id
                )
            ).mappings()
        ]
    results = [
        converge_legacy_persona(
            store,
            scope["case_id"],
            scope["persona_id"],
            dry_run=not apply,
        )
        for scope in scopes
    ]
    return {
        "mode": "apply" if apply else "dry_run",
        "persona_count": len(results),
        "results": results,
        "auto_finalized": False,
        "qc_created": False,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Audit or explicitly converge retained legacy Persona evidence into P2."
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-token", default="")
    args = parser.parse_args(argv)
    if args.apply and args.confirm_token != CONFIRM_TOKEN:
        parser.error("--apply requires --confirm-token " + CONFIRM_TOKEN)
    database_url = database_url_from_environment()
    if not database_url:
        parser.error("DATABASE_URL or DATABASE_PASSWORD_FILE is required")
    store = CaseStore(database_url)
    try:
        print(json.dumps(converge_all(store, apply=args.apply), indent=2))
    finally:
        store.dispose()


if __name__ == "__main__":
    main()
