"""mapping_data/fs_cde.ttl is the FreeSurfer data-element file PyNIDM downloads
from this repository's master branch. PyNIDM drops every measure whose data
element it cannot find there, so the file must describe every element the
converter can emit, i.e. it must be what create_cde_graph() builds from
freesurfer-cdes.json."""
import json
from pathlib import Path

from rdflib import Graph
from rdflib.compare import graph_diff, to_isomorphic

from segstats_jsonld.fsutils import FS, cde_file, create_cde_graph

COMMITTED_TTL = Path(cde_file).parent / "fs_cde.ttl"
THICKNESS = "http://uri.interlex.org/base/ilx_0111689"


def test_committed_fs_cde_ttl_matches_the_json():
    committed = to_isomorphic(Graph().parse(COMMITTED_TTL, format="turtle"))
    generated = to_isomorphic(create_cde_graph())
    if committed != generated:
        _, only_committed, only_generated = graph_diff(committed, generated)
        raise AssertionError(
            f"fs_cde.ttl is out of date with freesurfer-cdes.json: {len(only_committed)} triples only "
            f"in the file, {len(only_generated)} only in create_cde_graph(). Regenerate it with "
            f"create_cde_graph().serialize(destination=..., format='turtle')")


def test_thickness_measures_are_measures_of_thickness():
    """MeanThickness (fs_000137) carried ilx_0738276, a datum type, as its
    measureOf in the JSON while the TTL had thickness; PyNIDM's thickness
    query selects on measureOf ilx_0111689."""
    with open(cde_file) as fp:
        cdes = json.load(fp)
    wrong = {value["id"]: value["measureOf"] for key, value in cdes.items()
             if key != "count" and "Thick" in eval(key, {"FS": FS}).measure
             and value["measureOf"] != THICKNESS}
    assert wrong == {}
