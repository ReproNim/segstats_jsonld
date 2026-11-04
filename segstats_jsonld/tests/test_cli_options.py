"""Command-line combinations of fs_to_nidm: which are refused, and where the
output of the accepted ones lands."""
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

from rdflib import Graph, Namespace, URIRef

TESTDATA = Path(__file__).resolve().parent / "testdata"
REPO_ROOT = Path(__file__).resolve().parents[2]
PROV = Namespace("http://www.w3.org/ns/prov#")
DCT_DESCRIPTION = URIRef("http://purl.org/dc/terms/description")
NDAR_ID = URIRef("https://ndar.nih.gov/api/datadictionary/v2/dataelement/src_subject_id")

INPUT_TTL = """
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix ndar: <https://ndar.nih.gov/api/datadictionary/v2/dataelement/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix nidm: <http://purl.org/nidash/nidm#> .
<http://iri.nidash.org/project> a nidm:Project, prov:Activity .
<http://iri.nidash.org/subject-51456> a prov:Agent, prov:Person ;
    ndar:src_subject_id "51456"^^xsd:string .
"""


def make_subject_dir(tmp_path, stats=("aseg.stats", "rh.aparc.stats")):
    subject_dir = tmp_path / "sub-0051456"
    (subject_dir / "stats").mkdir(parents=True)
    for name in stats:
        shutil.copy(TESTDATA / name, subject_dir / "stats" / name)
    # without a build stamp fs_to_nidm prompts for the FreeSurfer version
    (subject_dir / "scripts").mkdir()
    (subject_dir / "scripts" / "build-stamp.txt").write_text(
        "freesurfer-linux-centos7_x86_64-7.4.1-20230614-7eb8460\n")
    return subject_dir


def run(*args):
    return subprocess.run([sys.executable, "-m", "segstats_jsonld.fs_to_nidm", *args],
                          cwd=REPO_ROOT, capture_output=True, text=True, stdin=subprocess.DEVNULL)


def md5(path):
    return hashlib.md5(path.read_bytes()).hexdigest()


def stats_activities(g):
    return set(g.subjects(DCT_DESCRIPTION, None))


def test_jsonld_with_subject_dir_and_nidm_is_refused(tmp_path):
    """-s -n -j re-read the input for every stats file and wrote each result to
    the same <nidm>.json, so only the last stats file survived, with exit 0."""
    subject_dir = make_subject_dir(tmp_path)
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL)
    before = md5(nidm)

    result = run("-s", str(subject_dir), "-n", str(nidm), "--forcenidm", "-j")

    assert result.returncode == 2, result.stdout + result.stderr
    assert "-j/--jsonld" in result.stderr
    # the message says what to use instead
    assert "-o" in result.stderr and "Turtle" in result.stderr
    assert md5(nidm) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["nidm.ttl", "sub-0051456"]


def test_subject_dir_with_nidm_keeps_every_stats_file(tmp_path):
    """The Turtle form of the same run, which the refusal points to."""
    subject_dir = make_subject_dir(tmp_path)
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL)

    result = run("-s", str(subject_dir), "-n", str(nidm), "--forcenidm")

    assert result.returncode == 0, result.stdout + result.stderr
    g = Graph().parse(nidm, format="turtle")
    assert len(stats_activities(g)) == 2


def test_forcenidm_adds_the_subject_to_an_input_without_subjects(tmp_path):
    """The one case --forcenidm still creates an agent: the input NIDM has no
    subjects at all. Every stats file attaches to that one agent."""
    subject_dir = make_subject_dir(tmp_path)
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL.split("<http://iri.nidash.org/subject-51456>")[0])

    result = run("-s", str(subject_dir), "-n", str(nidm), "--forcenidm")

    assert result.returncode == 0, result.stdout + result.stderr
    g = Graph().parse(nidm, format="turtle")
    ids = list(g.objects(None, NDAR_ID))
    assert [str(i) for i in ids] == ["sub-0051456"]
    assert len(stats_activities(g)) == 2
    # the agent was created by this run, so it is never reported as found
    assert "Found subject ID" not in result.stdout


def test_lookup_miss_fails_and_writes_nothing(tmp_path):
    subject_dir = make_subject_dir(tmp_path)
    nidm = tmp_path / "nidm.ttl"
    nidm.write_text(INPUT_TTL)
    before = md5(nidm)

    result = run("-s", str(subject_dir), "-subjid", "sub-0099999", "-n", str(nidm), "--forcenidm")

    assert result.returncode == 1, result.stdout + result.stderr
    assert "Subject ID sub-0099999 not found in the input NIDM" in result.stderr
    assert md5(nidm) == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["nidm.ttl", "sub-0051456"]
