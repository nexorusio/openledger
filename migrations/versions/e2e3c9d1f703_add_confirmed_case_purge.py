"""Permit a confirmed terminal case deletion to purge its own P2 lineage.

Revision ID: e2e3c9d1f703
Revises: e2e2b8d0a502

P2 facts remain immutable during normal operation.  This migration adds the
transaction-scoped authorization consulted by the immutable DELETE triggers so
that a case which is no longer collecting can be permanently removed as one
atomic, explicitly confirmed operation.
"""

from alembic import op
import sqlalchemy as sa


revision = "e2e3c9d1f703"
down_revision = "e2e2b8d0a502"
branch_labels = None
depends_on = None


PIPELINE_IMMUTABLE = (
    "pipeline_observations",
    "pipeline_groups",
    "pipeline_group_observations",
    "pipeline_operator_decisions",
    "pipeline_persona_versions",
    "pipeline_version_evidence",
    "pipeline_assessments",
    "pipeline_qc_decisions",
    "pipeline_requirement_resolutions",
    "pipeline_group_revisions",
)
CONNECTOR_IMMUTABLE = (
    "pipeline_connector_pages",
    "pipeline_connector_record_versions",
)
RECEIPT_FACTS = (
    "id",
    "case_id",
    "persona_id",
    "connector_id",
    "idempotency_key",
    "content_hash",
    "payload",
    "job_id",
    "request_id",
    "created_at",
)


def _sqlite(connection):
    return connection.dialect.name == "sqlite"


def _install_pipeline_guards(connection, name):
    if _sqlite(connection):
        connection.execute(sa.text(f"DROP TRIGGER IF EXISTS {name}_update_guard"))
        connection.execute(sa.text(f"DROP TRIGGER IF EXISTS {name}_delete_guard"))
        connection.execute(sa.text(
            f"CREATE TRIGGER {name}_update_guard BEFORE UPDATE ON {name} "
            "BEGIN SELECT RAISE(ABORT, 'immutable pipeline record'); END"
        ))
        connection.execute(sa.text(
            f"CREATE TRIGGER {name}_delete_guard BEFORE DELETE ON {name} "
            "WHEN NOT EXISTS (SELECT 1 FROM pipeline_case_purge_authorizations "
            "WHERE case_id = OLD.case_id) "
            "BEGIN SELECT RAISE(ABORT, 'immutable pipeline record'); END"
        ))
        return
    connection.execute(sa.text(f"DROP TRIGGER IF EXISTS {name}_immutable_guard ON {name}"))
    connection.execute(sa.text(f"DROP FUNCTION IF EXISTS {name}_immutable()"))
    connection.execute(sa.text(
        f"CREATE FUNCTION {name}_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF TG_OP = 'DELETE' AND EXISTS "
        "(SELECT 1 FROM pipeline_case_purge_authorizations WHERE case_id = OLD.case_id) "
        "THEN RETURN OLD; END IF; "
        "RAISE EXCEPTION 'immutable pipeline record'; END; $$"
    ))
    connection.execute(sa.text(
        f"CREATE TRIGGER {name}_immutable_guard BEFORE UPDATE OR DELETE ON {name} "
        f"FOR EACH ROW EXECUTE FUNCTION {name}_immutable()"
    ))


def _install_connector_guards(connection, name):
    if _sqlite(connection):
        connection.execute(sa.text(f"DROP TRIGGER IF EXISTS {name}_update_guard"))
        connection.execute(sa.text(f"DROP TRIGGER IF EXISTS {name}_delete_guard"))
        connection.execute(sa.text(
            f"CREATE TRIGGER {name}_update_guard BEFORE UPDATE ON {name} "
            "BEGIN SELECT RAISE(ABORT, 'immutable connector record'); END"
        ))
        connection.execute(sa.text(
            f"CREATE TRIGGER {name}_delete_guard BEFORE DELETE ON {name} "
            "WHEN NOT EXISTS (SELECT 1 FROM pipeline_case_purge_authorizations "
            "WHERE case_id = OLD.case_id) "
            "BEGIN SELECT RAISE(ABORT, 'immutable connector record'); END"
        ))
        return
    connection.execute(sa.text(f"DROP TRIGGER IF EXISTS {name}_immutable_guard ON {name}"))
    connection.execute(sa.text(f"DROP FUNCTION IF EXISTS {name}_immutable()"))
    connection.execute(sa.text(
        f"CREATE FUNCTION {name}_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF TG_OP = 'DELETE' AND EXISTS "
        "(SELECT 1 FROM pipeline_case_purge_authorizations WHERE case_id = OLD.case_id) "
        "THEN RETURN OLD; END IF; "
        "RAISE EXCEPTION 'immutable connector record'; END; $$"
    ))
    connection.execute(sa.text(
        f"CREATE TRIGGER {name}_immutable_guard BEFORE UPDATE OR DELETE ON {name} "
        f"FOR EACH ROW EXECUTE FUNCTION {name}_immutable()"
    ))


def _install_receipt_guards(connection):
    name = "pipeline_connector_receipts"
    if _sqlite(connection):
        connection.execute(sa.text("DROP TRIGGER IF EXISTS pipeline_connector_receipt_facts_guard"))
        connection.execute(sa.text("DROP TRIGGER IF EXISTS pipeline_connector_receipt_delete_guard"))
        changed = " OR ".join(f"NEW.{field} IS NOT OLD.{field}" for field in RECEIPT_FACTS)
        connection.execute(sa.text(
            f"CREATE TRIGGER pipeline_connector_receipt_facts_guard BEFORE UPDATE ON {name} "
            f"WHEN {changed} BEGIN SELECT RAISE(ABORT, 'immutable connector receipt'); END"
        ))
        connection.execute(sa.text(
            f"CREATE TRIGGER pipeline_connector_receipt_delete_guard BEFORE DELETE ON {name} "
            "WHEN NOT EXISTS (SELECT 1 FROM pipeline_case_purge_authorizations "
            "WHERE case_id = OLD.case_id) "
            "BEGIN SELECT RAISE(ABORT, 'immutable connector receipt'); END"
        ))
        return
    connection.execute(sa.text("DROP TRIGGER IF EXISTS pipeline_connector_receipt_facts_guard ON pipeline_connector_receipts"))
    connection.execute(sa.text("DROP FUNCTION IF EXISTS pipeline_connector_receipt_facts_guard()"))
    changed = " OR ".join(f"NEW.{field} IS DISTINCT FROM OLD.{field}" for field in RECEIPT_FACTS)
    connection.execute(sa.text(
        "CREATE FUNCTION pipeline_connector_receipt_facts_guard() RETURNS trigger LANGUAGE plpgsql AS $$ "
        "BEGIN IF TG_OP = 'DELETE' AND EXISTS "
        "(SELECT 1 FROM pipeline_case_purge_authorizations WHERE case_id = OLD.case_id) "
        "THEN RETURN OLD; END IF; "
        "IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'immutable connector receipt'; END IF; "
        f"IF {changed} THEN RAISE EXCEPTION 'immutable connector receipt'; END IF; RETURN NEW; END; $$"
    ))
    connection.execute(sa.text(
        "CREATE TRIGGER pipeline_connector_receipt_facts_guard BEFORE UPDATE OR DELETE "
        "ON pipeline_connector_receipts FOR EACH ROW "
        "EXECUTE FUNCTION pipeline_connector_receipt_facts_guard()"
    ))


def upgrade():
    op.create_table(
        "pipeline_case_purge_authorizations",
        sa.Column("case_id", sa.String(length=36), primary_key=True),
        sa.ForeignKeyConstraint(["case_id"], ["cases.id"], ondelete="CASCADE"),
    )
    connection = op.get_bind()
    for name in PIPELINE_IMMUTABLE:
        _install_pipeline_guards(connection, name)
    for name in CONNECTOR_IMMUTABLE:
        _install_connector_guards(connection, name)
    _install_receipt_guards(connection)


def downgrade():
    raise RuntimeError("P2 case deletion support is additive and cannot be downgraded safely")
