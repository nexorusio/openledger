"""Machine-only ingestion: bearer identity and exact case/Persona scopes."""

from flask import Blueprint, jsonify, request

from maigret.web.pipeline_connector_ingestion import (
    ConnectorIngestionStore,
    MAX_BATCH_BYTES,
    authenticate_connector,
)


def register_connector_ingestion_routes(app, *, get_case_store):
    blueprint = Blueprint("connector_ingestion", __name__)

    @blueprint.before_request
    def identity_required():
        # Browser sessions and their cookies are deliberately not credentials
        # for this endpoint. Global login permits only these endpoints through.
        try:
            from flask import g

            g.connector_identity = authenticate_connector(
                request.view_args["connector_id"],
                request.headers.get("Authorization", ""),
                app.config,
            )
        except PermissionError:
            return jsonify(error="Connector authentication failed"), 401
        except (ValueError, TypeError):
            return jsonify(error="Connector configuration unavailable"), 503

    def service():
        from maigret.web.pipeline_store import PipelineStore

        return ConnectorIngestionStore(PipelineStore(get_case_store()))

    @blueprint.post("/api/connectors/<connector_id>/batches")
    def submit_batch(connector_id):
        from flask import g

        # Enforce a streaming read bound even when Content-Length is omitted.
        if request.content_length and request.content_length > MAX_BATCH_BYTES:
            return jsonify(error="Connector batch is too large"), 413
        if request.mimetype != "application/json":
            return jsonify(error="application/json is required"), 415
        body = request.stream.read(MAX_BATCH_BYTES + 1)
        if len(body) > MAX_BATCH_BYTES:
            return jsonify(error="Connector batch is too large"), 413
        try:
            import json

            payload = json.loads(body)
            result = service().accept_batch(
                connector_id,
                g.connector_identity,
                payload,
                idempotency_key=request.headers.get("Idempotency-Key", ""),
            )
        except PermissionError:
            return (
                jsonify(error="Connector scope does not permit this case and Persona"),
                403,
            )
        except KeyError:
            return jsonify(error="Case and Persona not found"), 404
        except (ValueError, TypeError, UnicodeDecodeError) as error:
            # These errors contain contract field names, never source payloads
            # or credentials. No traceback or provider response is exposed.
            return jsonify(error=str(error)), (
                409
                if "replay" in str(error) or "Idempotency key" in str(error)
                else 400
            )
        return jsonify(result), 200 if result["replayed"] else 202

    @blueprint.get("/api/connectors/<connector_id>/batches/<receipt_id>")
    def get_batch(connector_id, receipt_id):
        from flask import g

        try:
            result = service().get_receipt(
                receipt_id, connector_id=connector_id, identity=g.connector_identity
            )
        except KeyError:
            return jsonify(error="Receipt not found"), 404
        return jsonify(result)

    app.register_blueprint(blueprint)
