"""The -csv path, run from the command line.

-csv reads one row per subject; with -json_map it asks only the three
provenance questions, answered here on stdin.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from rdflib import Graph, Literal, Namespace, URIRef

from segstats_jsonld._pynidm import PYNIDM_MAJOR

REPO_ROOT = Path(__file__).resolve().parents[2]
PROV = Namespace("http://www.w3.org/ns/prov#")
SIO_SUBJECT = URIRef("http://semanticscience.org/ontology/sio.owl#Subject")
DCT_DESCRIPTION = URIRef("http://purl.org/dc/terms/description")
THALAMUS_CDE = "https://surfer.nmr.mgh.harvard.edu/fs_001806"
PROVENANCE_ANSWERS = "volumes from a csv test\n7.4.1\nLinux\n"
# PyNIDM 4.x's DD_to_nidm adds the subject-id column's isAbout as an
# rdflib.Namespace, which rdflib 6 refuses as an object ("must be an rdflib
# term"), so every -csv run fails inside PyNIDM 4.x. Strict: the mark fails
# as soon as that stops being true.
PYNIDM4_DD_TO_NIDM = pytest.mark.xfail(
    PYNIDM_MAJOR == 4, strict=True,
    reason="PyNIDM 4.x DD_to_nidm adds an rdflib.Namespace as an object; rdflib 6 refuses it")

INPUT_TTL = """
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix ndar: <https://ndar.nih.gov/api/datadictionary/v2/dataelement/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix nidm: <http://purl.org/nidash/nidm#> .
<http://iri.nidash.org/project> a nidm:Project, prov:Activity .
<http://iri.nidash.org/subject-51456> a prov:Agent, prov:Person ;
    ndar:src_subject_id "51456"^^xsd:string .
"""


def write_csv(tmp_path, subject="sub-0051456"):
    csv = tmp_path / "fs.csv"
    csv.write_text(f"id,Left-Thalamus-Proper\n{subject},7000\n")
    return csv


def write_json_map(tmp_path, csv):
    """The mapping -csv saves after its interactive questions."""
    json_map = tmp_path / "saved_map.json"
    json_map.write_text(json.dumps({
        f"DD(source='{csv}', variable='id')": {
            "source_variable": "id",
            "description": "subject/participant identifier",
            "valueType": "http://www.w3.org/2001/XMLSchema#string",
            "isAbout": {"url": "https://ndar.nih.gov/api/datadictionary/v2/dataelement/src_subject_id",
                        "label": "src_subject_id"}},
        f"DD(source='{csv}', variable='Left-Thalamus-Proper')": {
            "source_variable": "Left-Thalamus-Proper", "sameAs": THALAMUS_CDE},
    }))
    return json_map


def run(*args, answers=PROVENANCE_ANSWERS):
    return subprocess.run([sys.executable, "-m", "segstats_jsonld.fs_to_nidm", *args],
                          cwd=REPO_ROOT, capture_output=True, text=True, input=answers)


def md5(path):
    return hashlib.md5(path.read_bytes()).hexdigest()


@PYNIDM4_DD_TO_NIDM
def test_csv_with_nidm_attaches_the_row_to_the_input_subject(tmp_path):
    csv = write_csv(tmp_path)
    json_map = write_json_map(tmp_path, csv)
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL)

    result = run("-csv", str(csv), "-json_map", str(json_map), "-n", str(nidm))

    assert result.returncode == 0, result.stdout + result.stderr
    g = Graph().parse(nidm, format="turtle")
    (activity,) = g.subjects(DCT_DESCRIPTION, Literal("volumes from a csv test"))
    subjects = {g.value(assoc, PROV["agent"])
                for assoc in g.objects(activity, PROV["qualifiedAssociation"])
                if (assoc, PROV["hadRole"], SIO_SUBJECT) in g}
    assert subjects == {URIRef("http://iri.nidash.org/subject-51456")}
    values = {str(o) for _, p, o in g if str(p) == THALAMUS_CDE}
    assert values == {"7000"}
    assert (tmp_path / "fs_cde.ttl").exists()


# id column 1; measurement type 5 (volume); the first matching region; provenance
INTERACTIVE_ANSWERS = "1\n5\n1\n" + PROVENANCE_ANSWERS


def test_csv_lookup_miss_writes_nothing_and_asks_no_mapping_questions(tmp_path):
    """The rows' subjects are looked up before the interactive mapping, which
    writes <csv>_map.json; a miss used to leave that file behind under a
    message saying no output was written."""
    csv = write_csv(tmp_path, subject="sub-0099999")
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL)
    before = md5(nidm)

    result = run("-csv", str(csv), "-n", str(nidm), "--forcenidm", answers=INTERACTIVE_ANSWERS)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "Subject ID sub-0099999 not found in the input NIDM" in result.stderr
    assert "No output written" in result.stderr
    assert "Select which measurement type" not in result.stdout
    assert md5(nidm) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fs.csv", "nidm.ttl"]


def test_csv_lookup_miss_with_a_saved_map_writes_nothing(tmp_path):
    csv = write_csv(tmp_path, subject="sub-0099999")
    json_map = write_json_map(tmp_path, csv)
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL)
    before = md5(nidm)

    result = run("-csv", str(csv), "-json_map", str(json_map), "-n", str(nidm), "--forcenidm")

    assert result.returncode == 1, result.stdout + result.stderr
    assert "No output written" in result.stderr
    assert md5(nidm) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["fs.csv", "nidm.ttl", "saved_map.json"]


@PYNIDM4_DD_TO_NIDM
def test_csv_jsonld_output_writes_the_cde_file_beside_it(tmp_path):
    """-add_de's help says that without it the data elements go to fs_cde.ttl in
    the output directory; -csv -o -j used to write none."""
    csv = write_csv(tmp_path)
    json_map = write_json_map(tmp_path, csv)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = run("-csv", str(csv), "-json_map", str(json_map), "-o", str(out_dir / "fs.jsonld"), "-j")

    assert result.returncode == 0, result.stdout + result.stderr
    assert sorted(p.name for p in out_dir.iterdir()) == ["fs.jsonld", "fs_cde.ttl"]
