"""Execute the actual viewer with its DOM/vis boundary, checking paged topology."""

import shutil
import subprocess
from pathlib import Path

import pytest


def test_viewer_pages_negative_and_excluded_sources_without_dangling_edges():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required to execute the graph viewer boundary")
    viewer = Path(__file__).parents[1] / "maigret/web/static/pipeline-graph.js"
    harness = r"""
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const roles = ['supports', 'contradicts', 'absence_observation', 'collection_outcome', 'excluded_evidence', 'historical_evidence'];
const graph = {nodes: [{id:'subject:1',kind:'subject'}, {id:'group:g',kind:'claim',label:'Fact'}],
 edges:[{from:'subject:1',to:'group:g',group_id:'g',kind:'curated_fact'}],item_count:1,observation_count:240};
for(let i=0;i<240;i++) {
 graph.nodes.push({id:'obs:'+i,kind:'observation',label:'Source '+i});
 for(const kind of ['provenance', roles[i % roles.length]]) graph.edges.push({from:'obs:'+i,to:'group:g',group_id:'g',kind});
}
const controls = [], sets = [];
function element() {return {textContent:'',setAttribute(){},append(){},after(){},replaceChildren(){},addEventListener(name,handler){this[name]=handler}};}
const elements = new Map(['pipeline-graph-data','pipeline-graph','pipeline-graph-detail','pipeline-graph-status','pipeline-graph-search'].map(key=>[key,element()]));
elements.get('pipeline-graph-data').textContent=JSON.stringify(graph);
const document={getElementById:id=>elements.get(id),createElement:tag=>{const e=element();if(tag==='button')controls.push(e);return e;}};
const vis={DataSet: class{constructor(){this.rows=[];sets.push(this)}clear(){this.rows=[]}add(rows){this.rows.push(...rows)}},Network:class{fit(){}on(){}}};
vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), {document,vis,window:{vis}});
function valid(expected) {
 const ids = new Set(sets[0].rows.map(row=>row.id));
 assert.strictEqual(ids.size, expected+2);
 for(const edge of sets[1].rows) assert(ids.has(edge.from) && ids.has(edge.to), 'dangling '+edge.kind);
 for(const role of roles) assert(sets[1].rows.some(row=>row.kind===role && row.label===role.replaceAll('_',' ')), 'hidden '+role);
 assert(!sets[1].rows.some(row=>row.kind==='provenance'), 'redundant overlay in visual');
 assert(elements.get('pipeline-graph-status').textContent.includes('source observations'));
}
valid(200);
controls.find(button=>button.textContent==='Next sources').click();
valid(40);
assert(sets[0].rows.some(row=>row.id==='obs:239'));
assert.strictEqual(graph.edges.length,481,'full provenance JSON was mutated');
"""
    subprocess.run([node, "-e", harness, str(viewer)], check=True, capture_output=True)
