"""PDF projection of a frozen P2 Persona version, with full evidence register."""

from __future__ import annotations

import io
import json

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import PageBreak, SimpleDocTemplate, Spacer

from maigret.web.persona_pdf import (
    _paragraph,
    _register_fonts,
    _source_url_paragraph,
    _styles,
)


def generate_pipeline_pdf(projection):
    """No mutable claim lookup, network fetch, heuristic score, or implicit QC."""
    rendered = json.dumps(projection, ensure_ascii=False, indent=2, default=str)
    regular, bold, fallbacks = _register_fonts(rendered)
    styles = _styles(regular, bold, fallbacks)
    output = io.BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=25 * mm,
        bottomMargin=20 * mm,
        title=f'{projection["label"]} v{projection["sequence"]}',
        author='OpenLedger',
        subject=f'Frozen Persona version {projection["version_id"]}',
    )
    story = []

    def text(value, style='body'):
        story.append(_paragraph(value, styles[style]))
        story.append(Spacer(1, 2 * mm))

    def payload(value):
        for line in json.dumps(
            value, ensure_ascii=False, indent=2, default=str
        ).splitlines():
            text(line, 'small')

    def page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(colors.HexColor('#172533'))
        canvas.setFont(bold, 10)
        canvas.drawString(18 * mm, A4[1] - 14 * mm, 'O/  OPENLEDGER')
        canvas.setFont(regular, 8)
        canvas.drawRightString(
            A4[0] - 18 * mm,
            A4[1] - 14 * mm,
            f'{projection["label"]} · v{projection["sequence"]}',
        )
        canvas.setFillColor(colors.HexColor('#526578'))
        canvas.setFont(regular, 6)
        canvas.drawString(18 * mm, 12 * mm, 'Version ' + str(projection['version_id']))
        canvas.drawRightString(A4[0] - 18 * mm, 12 * mm, str(doc.page))
        canvas.restoreState()

    text(f'{projection["label"]} · Version {projection["sequence"]}', 'field')
    if projection['status'] == 'withdrawn':
        text(
            'WITHDRAWN — This version remains an audit record and is no longer designated as the final Persona.'
        )
    elif projection['status'] == 'superseded':
        text(
            'SUPERSEDED — A newer QC-approved Persona version is current. This historical evidence manifest is retained.'
        )
    elif projection['status'] != 'approved':
        text(
            'DRAFT — This version has no current QC-approved final designation. Export does not approve a Persona.'
        )
    text('Version: ' + projection['version_id'], 'small')
    text('Manifest SHA-256: ' + projection['content_hash'], 'small')
    text('Case: ' + projection['case_id'], 'small')
    text('Subject: ' + projection['persona_id'], 'small')
    text('Scope', 'field')
    payload(projection['scope'])
    text('Limitations and unknowns', 'field')
    for limitation in projection['limitations']:
        text(limitation)
    if not projection['limitations']:
        text('No additional limitations were recorded in this version.')
    text('Curated facts and account bindings', 'field')
    from maigret.web.pipeline_evidence import observation_evidence_role

    evidence = {}
    evidence_uses = {}
    for item in projection['items']:
        normalized = item.get('normalized', {})
        title = (
            normalized.get('predicate') or normalized.get('field_name') or item['kind']
        )
        text(str(title).replace('_', ' ').title(), 'field')
        text('Group: ' + item['group_id'], 'small')
        payload(normalized)
        text('Operator decision', 'small_bold')
        payload(item['decision'])
        assessment = item.get('assessment')
        if assessment:
            text('Frozen evidence assessment', 'small_bold')
            payload(assessment)
        else:
            text(
                'Numerical probability unavailable: no validated assessment is included in this version.',
                'small',
            )
        ids = []
        for observation in item.get('evidence', []):
            ids.append(str(observation['id']))
            evidence[str(observation['id'])] = observation
            use = {
                'group_id': item['group_id'],
                'role': observation_evidence_role(observation, normalized),
                'operator_disposition': observation.get('operator_disposition'),
                'source_state': observation.get('source_state'),
            }
            evidence_uses.setdefault(str(observation['id']), []).append(use)
            text(
                'Observation '
                + str(observation['id'])
                + ' — '
                + use['role'].replace('_', ' '),
                'small',
            )
            if use['operator_disposition']:
                text(
                    'Evidence disposition: ' + use['operator_disposition']['reason'],
                    'small',
                )
        text('Observation IDs: ' + ', '.join(ids), 'small')
    if not projection['items']:
        text('No facts were included in this version.')
    text('Frozen exclusions', 'field')
    payload(projection['exclusions'])
    text('Research requirements and dispositions', 'field')
    payload(projection['requirements'])
    story.append(PageBreak())
    text('Complete retained evidence register', 'field')
    text(
        f'{len(evidence)} distinct observations. All entries below come from this exact version manifest; no claim or source limit is applied.'
    )
    for observation_id, observation in evidence.items():
        text('Observation ' + observation_id, 'field')
        url = observation.get('source_url') or observation.get('original_url') or ''
        if url:
            from maigret.web.pipeline_routes import public_url

            if public_url(url):
                story.append(_source_url_paragraph(url, styles['small']))
        payload(
            {
                key: value
                for key, value in observation.items()
                if key not in {'operator_disposition', 'support_eligible'}
            }
        )
        text('Use of this observation in each curated group', 'small_bold')
        payload(evidence_uses[observation_id])
    text('QC audit', 'field')
    payload(projection['qc'])
    document.build(story, onFirstPage=page, onLaterPages=page)
    return output.getvalue()
