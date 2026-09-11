"""R7/R8 browser-facing regressions for the presentation-only contract."""

import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
PERSONA_TEMPLATE = ROOT / "maigret" / "web" / "templates" / "persona.html"
LIVE_TEMPLATE = ROOT / "maigret" / "web" / "templates" / "live.html"
RESULTS_TEMPLATE = ROOT / "maigret" / "web" / "templates" / "results.html"


def _map_snapshot(locations, *, width=640, height=360):
    template = PERSONA_TEMPLATE.read_text(encoding="utf-8")
    script = template.rsplit("<script>", 1)[1].split("</script>", 1)[0]
    script = script.replace("{{ map_locations | tojson }}", json.dumps(locations))
    script = script.replace("{{ map_tile_url | tojson }}", json.dumps("https://tiles.test/{z}/{x}/{y}.png"))
    harness = r'''
const snapshot = {created: 0, markers: [], setViews: [], fitBounds: [], invalidations: 0, timers: []};
const mapElement = {clientWidth: Number(process.argv[1]), clientHeight: Number(process.argv[2]), closest: () => null};
const element = () => ({append(){}, appendChild(){}, addEventListener(){}, classList:{toggle(){}}, setAttribute(){}, hidden:false});
global.window = {personaMap: null, clearTimeout(){}, setTimeout: callback => { snapshot.timers.push(callback); return snapshot.timers.length; }, requestAnimationFrame: callback => callback(), addEventListener(){}};
global.document = {querySelectorAll: () => [], getElementById: id => id === 'personaLocationMap' ? mapElement : null, createElement: element};
global.L = {
  map: () => { snapshot.created++; return {setView: point => snapshot.setViews.push(point), fitBounds: bounds => snapshot.fitBounds.push(bounds), invalidateSize: () => snapshot.invalidations++}; },
  tileLayer: () => ({addTo(){}}),
  marker: point => { snapshot.markers.push(point); return {addTo(){return this;}, bindPopup(){return this;}, bindTooltip(){return this;}}; }
};
window.L = global.L;
'''
    completed = subprocess.run(
        ["node", "-e", harness + script + "\nconsole.log(JSON.stringify(snapshot));", str(width), str(height)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def _location(latitude, longitude, index=0):
    return {
        "id": f"claim-{index}",
        "label": f"Location {index}",
        "latitude": latitude,
        "longitude": longitude,
        "field_name": "current_location",
        "confidence": 80,
    }


def test_persona_map_defers_hidden_initialization_and_handles_marker_counts():
    assert _map_snapshot([], width=640)["created"] == 0
    assert _map_snapshot([_location(-6.2, 106.8)], width=0)["created"] == 0

    one = _map_snapshot([_location(-6.2, 106.8)])
    assert len(one["markers"]) == 1
    assert one["setViews"] == [[-6.2, 106.8]]

    for count in (2, 10, 100):
        snapshot = _map_snapshot([_location(-6 + index / 100, 106 + index / 100, index) for index in range(count)])
        assert len(snapshot["markers"]) == count
        assert len(snapshot["fitBounds"]) == 1


def test_persona_map_filters_invalid_coordinates_groups_duplicates_and_uses_dateline_shortcut():
    snapshot = _map_snapshot([
        _location(-6.2, 106.8, 1), _location(-6.2, 106.8, 2),
        _location(91, 100, 3), _location(0, 181, 4), _location("nan", 100, 5),
    ])
    assert snapshot["markers"] == [[-6.2, 106.8]]

    dateline = _map_snapshot([_location(10, 179), _location(11, -179)])
    assert dateline["fitBounds"] == [[[10, 179], [11, 181]]]
    assert dateline["markers"] == [[10, 179], [11, 181]]


def test_live_source_stop_is_only_a_notice_and_runtime_phase_controls_terminal_badges():
    template = LIVE_TEMPLATE.read_text(encoding="utf-8")
    stopped_index = template.index("ev.type === 'stopped'")
    stopped_block = template[stopped_index:template.index("} else if (ev.type === 'budget_exhausted')", stopped_index)]
    assert "addRuntimeNotice" in stopped_block
    assert "statusEl.textContent" not in stopped_block
    assert "function runtimePhase" in template
    assert "phase === 'terminal'" in template
    assert "phase === 'finalizing'" in template
    assert "terminalStatus" in template
    assert "ev.type === 'lifecycle'" in template
    assert "stop_cause" in template and "cleanup_state" in template


def test_results_only_link_available_artifacts_and_remain_legacy_compatible():
    template = RESULTS_TEMPLATE.read_text(encoding="utf-8")
    assert "graph_artifact if graph_artifact is mapping" in template
    assert "descriptor.available" in template
    assert "descriptor.filename" in template
    assert "legacy_filename is string and legacy_filename" in template
    assert "unavailable" in template
