"""How add_seg_data finds the input subject the stats belong to.

The id given (-subjid, the -s directory name or a -csv cell) and the id
stored in the input NIDM (ndar:src_subject_id) are compared after the same
normalisation on both sides: a leading "sub-" and leading zeros are removed,
and the literal is compared as a string, so PyNIDM 4.x's "..."^^xsd:string
and PyNIDM 5's plain literal both match. BIDS directories carry "sub-" and
participants.tsv may or may not; ABIDE stores 51456 for sub-0051456.

When the input has subjects and none matches, the run fails, even with
--forcenidm: attaching the stats to a new agent would leave them unconnected
to the subject's demographics and acquisitions while the run succeeded.
"""
from types import SimpleNamespace

import pytest
from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef, XSD

from segstats_jsonld._pynidm import NIDM_SUBJECTID_URI, Constants
from segstats_jsonld.fs_to_nidm import SubjectLookupError, add_seg_data

NFO = Namespace("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
SUBJECT_ID = URIRef(NIDM_SUBJECTID_URI)
T1W = "sub-0050002_T1w.nii.gz"
STATS = SimpleNamespace(uri="http://example.org/entity/fsstats")
SPELLINGS = ["sub-0050002", "0050002", "50002"]


def _project_graph():
    graph = Graph()
    graph.add((URIRef("http://example.org/project"), RDF.type, Constants.NIDM["Project"]))
    return graph


def _add_subject(graph, name, stored_id, datatype=XSD.string):
    subject = URIRef(f"http://example.org/agent/{name}")
    graph.add((subject, RDF.type, Constants.PROV["Agent"]))
    graph.add((subject, RDF.type, Constants.PROV["Person"]))
    graph.add((subject, SUBJECT_ID, Literal(stored_id, datatype=datatype)))
    return subject


def _input_graph(stored_id, datatype):
    graph = _project_graph()
    subject = _add_subject(graph, "participant", stored_id, datatype)

    acq_activity = URIRef("http://example.org/activity/acq")
    assoc = BNode()
    graph.add((acq_activity, RDF.type, Constants.NIDM["Acquisition"]))
    graph.add((acq_activity, Constants.PROV["qualifiedAssociation"], assoc))
    graph.add((assoc, Constants.PROV["agent"], subject))
    graph.add((assoc, Constants.PROV["hadRole"], Constants.SIO["Subject"]))

    acquisition = URIRef("http://example.org/acquisition/t1w")
    graph.add((acquisition, RDF.type, Constants.NIDM["AcquisitionObject"]))
    graph.add((acquisition, Constants.PROV["wasGeneratedBy"], acq_activity))
    graph.add((acquisition, Constants.NIDM["hadAcquisitionModality"],
               Constants.NIDM["MagneticResonanceImaging"]))
    graph.add((acquisition, Constants.NIDM["hadImageUsageType"], Constants.NIDM["Anatomical"]))
    graph.add((acquisition, NFO["filename"], Literal(f"bids::sub-0050002/anat/{T1W}")))
    return graph, subject, acquisition


def _stats_subjects(graph):
    """Agents a FreeSurfer stats activity is associated with in the subject role."""
    agents = set()
    for activity in graph.subjects(Constants.DCT["description"], None):
        for assoc in graph.objects(activity, Constants.PROV["qualifiedAssociation"]):
            if (assoc, Constants.PROV["hadRole"], Constants.SIO["Subject"]) in graph:
                agents.update(graph.objects(assoc, Constants.PROV["agent"]))
    return agents


@pytest.mark.parametrize("datatype", [XSD.string, None], ids=["xsd-string", "plain"])
@pytest.mark.parametrize("stored_id", SPELLINGS)
@pytest.mark.parametrize("given_id", SPELLINGS)
def test_finds_the_subject_whatever_the_spelling_on_either_side(given_id, stored_id, datatype, capsys):
    graph, subject, acquisition = _input_graph(stored_id, datatype)

    add_seg_data(graph, {}, given_id, STATS, add_to_nidm=True, forceagent=True, acq_filename=[T1W])

    assert set(graph.subjects(SUBJECT_ID, None)) == {subject}
    assert _stats_subjects(graph) == {subject}
    assert (None, Constants.PROV["used"], acquisition) in graph
    assert "Found subject ID" in capsys.readouterr().out


def test_several_matching_subjects_is_an_error_naming_them():
    graph = _project_graph()
    first = _add_subject(graph, "first", "0050002")
    second = _add_subject(graph, "second", "sub-50002", None)

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    assert str(first) in str(err.value) and str(second) in str(err.value)
    assert _stats_subjects(graph) == set()
    assert len(set(graph.subjects(SUBJECT_ID, None))) == 2


@pytest.mark.parametrize("forceagent", [True, False], ids=["forcenidm", "no-forcenidm"])
def test_no_match_while_the_input_has_subjects_is_an_error_listing_them(forceagent):
    graph = _project_graph()
    _add_subject(graph, "a", "0050003")
    _add_subject(graph, "b", "sub-0050004", None)

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=forceagent)

    message = str(err.value)
    assert "sub-0050002" in message and "0050003" in message and "sub-0050004" in message
    # no agent was made for the requested id
    assert {str(o) for o in graph.objects(None, SUBJECT_ID)} == {"0050003", "sub-0050004"}
    assert _stats_subjects(graph) == set()


def test_forcenidm_creates_an_agent_only_when_the_input_has_no_subjects(capsys):
    graph = _project_graph()

    add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    (agent,) = _stats_subjects(graph)
    assert list(graph.objects(agent, SUBJECT_ID)) == [Literal("sub-0050002", datatype=XSD.string)]
    assert "Found subject ID" not in capsys.readouterr().out


def test_later_calls_reuse_the_agent_created_earlier_in_the_run(capsys):
    """-s with -n converts each stats file separately; the agent made for the
    first one must be reused, and not reported as found in the input. A later
    -csv row with another id still sees an input without subjects."""
    graph = _project_graph()
    created = set()

    add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True,
                 created_agents=created)
    add_seg_data(graph, {}, "sub-0050002", SimpleNamespace(uri="http://example.org/entity/second"),
                 add_to_nidm=True, forceagent=True, created_agents=created)
    add_seg_data(graph, {}, "sub-0050009", SimpleNamespace(uri="http://example.org/entity/third"),
                 add_to_nidm=True, forceagent=True, created_agents=created)

    assert len(created) == 2
    assert {str(o) for o in graph.objects(None, SUBJECT_ID)} == {"sub-0050002", "sub-0050009"}
    assert "Found subject ID" not in capsys.readouterr().out


@pytest.mark.parametrize("stored_id", ['A"1\\x', 'sub-0"\\'])
def test_quotes_and_backslashes_in_ids_are_compared_as_text(stored_id):
    graph = _project_graph()
    subject = _add_subject(graph, "odd", stored_id)
    _add_subject(graph, "other", "B02")

    add_seg_data(graph, {}, stored_id, STATS, add_to_nidm=True)

    assert _stats_subjects(graph) == {subject}


def test_an_id_shaped_like_a_query_matches_nothing():
    """Interpolated into SPARQL, this id made the filter true for every agent."""
    graph = _project_graph()
    _add_subject(graph, "alice", "A01")
    _add_subject(graph, "bob", "B02")

    with pytest.raises(SubjectLookupError):
        add_seg_data(graph, {}, 'Z99" || "1" = "1', STATS, add_to_nidm=True, forceagent=True)

    assert _stats_subjects(graph) == set()


# The choices below match ants_seg_to_nidm's lookup, so both converters find
# the same subject in the same input.

def test_an_agent_typed_only_prov_person_is_a_subject():
    graph = _project_graph()
    person = URIRef("http://example.org/agent/person")
    graph.add((person, RDF.type, Constants.PROV["Person"]))
    graph.add((person, SUBJECT_ID, Literal("0050002")))

    add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True)

    assert _stats_subjects(graph) == {person}


# Every node carrying a subject id is a subject, whatever its rdf:type and
# under either NDAR IRI. A participant that a stricter rule did not see made
# the input look subject-less, so --forcenidm attached the stats to a new agent
# and the run exited 0.
HTTP_SUBJECT_ID = URIRef(str(SUBJECT_ID).replace("https://", "http://", 1))
SUBJECT_KINDS = ["nidm-subject", "untyped", "http-ndar-iri"]


def _add_participant(graph, kind, stored_id):
    node = URIRef(f"http://example.org/participant/{kind}/{stored_id}")
    if kind == "nidm-subject":
        graph.add((node, RDF.type, Constants.NIDM["Subject"]))
    predicate = HTTP_SUBJECT_ID if kind == "http-ndar-iri" else SUBJECT_ID
    if kind == "http-ndar-iri":
        graph.add((node, RDF.type, Constants.PROV["Person"]))
    graph.add((node, predicate, Literal(stored_id)))
    return node


@pytest.mark.parametrize("kind", SUBJECT_KINDS)
def test_any_node_with_a_subject_id_is_found(kind):
    graph = _project_graph()
    node = _add_participant(graph, kind, "0050002")

    add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    assert _stats_subjects(graph) == {node}


@pytest.mark.parametrize("forceagent", [True, False], ids=["forcenidm", "no-forcenidm"])
@pytest.mark.parametrize("kind", SUBJECT_KINDS)
def test_a_miss_beside_any_node_with_a_subject_id_is_an_error(kind, forceagent):
    graph = _project_graph()
    _add_participant(graph, kind, "0050003")

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=forceagent)

    assert "0050003" in str(err.value)
    assert _stats_subjects(graph) == set()


def test_a_typed_and_an_untyped_node_with_the_same_id_are_two_matches():
    graph = _project_graph()
    typed = _add_subject(graph, "typed", "0050002")
    untyped = _add_participant(graph, "untyped", "50002")

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    assert str(typed) in str(err.value) and str(untyped) in str(err.value)


def _failing_lookup_input(case):
    graph = _project_graph()
    if case == "miss":
        _add_subject(graph, "a", "51456")
    elif case == "several":
        _add_subject(graph, "a", "50002")
        _add_subject(graph, "b", "sub-0050002")
    return graph  # "no-subjects": the project only, and no --forcenidm


@pytest.mark.parametrize("case", ["miss", "several", "no-subjects"])
def test_a_failed_lookup_leaves_the_graph_unchanged(case):
    """A library caller that catches SubjectLookupError and then serialises
    the graph must not find stray activities or descriptions in it."""
    graph = _failing_lookup_input(case)
    before = set(graph)

    with pytest.raises(SubjectLookupError):
        add_seg_data(graph, {"version": "7.4.1"}, "sub-0050002", STATS, add_to_nidm=True,
                     forceagent=case != "no-subjects")

    assert set(graph) == before


@pytest.mark.parametrize("value, expected", [
    ("sub-0050002", "50002"), ("0050002", "50002"), ("50002", "50002"),
    ("0", "0"), ("000", "0"), ("sub-00", "0"), ("sub-", ""), ("", ""),
    (Literal("007", datatype=XSD.string), "7"),
])
def test_normalise_subject_id(value, expected):
    from segstats_jsonld.fs_to_nidm import normalise_subject_id
    assert normalise_subject_id(value) == expected


def test_an_all_zero_id_matches_its_other_spellings():
    graph = _project_graph()
    subject = _add_subject(graph, "zero", "0")

    add_seg_data(graph, {}, "sub-000", STATS, add_to_nidm=True)

    assert _stats_subjects(graph) == {subject}


def test_two_ids_on_one_agent_are_one_match():
    graph = _project_graph()
    subject = _add_subject(graph, "both", "0050002")
    graph.add((subject, SUBJECT_ID, Literal("sub-0050002")))

    add_seg_data(graph, {}, "50002", STATS, add_to_nidm=True)

    assert _stats_subjects(graph) == {subject}


def test_no_subjects_and_no_forcenidm_is_an_error():
    graph = _project_graph()

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=False)

    assert "no subject agents" in str(err.value) and "--forcenidm" in str(err.value)
    assert list(graph.objects(None, SUBJECT_ID)) == []


# Several nodes with the same subject id: some delivered inputs (ADHD200 NYU,
# Peking_1, Pittsburgh) hold two prov:Agent+prov:Person nodes per subject, one
# owning the anatomical MRI acquisition and one associated only with an
# assessment. The node that owns the acquisition is the one the stats belong to.

def _add_anatomical_acquisition(graph, subject, name):
    activity = URIRef(f"http://example.org/activity/{name}")
    assoc = BNode()
    graph.add((activity, RDF.type, Constants.NIDM["Acquisition"]))
    graph.add((activity, Constants.PROV["qualifiedAssociation"], assoc))
    graph.add((assoc, Constants.PROV["agent"], subject))
    graph.add((assoc, Constants.PROV["hadRole"], Constants.SIO["Subject"]))
    acquisition = URIRef(f"http://example.org/acquisition/{name}")
    graph.add((acquisition, RDF.type, Constants.NIDM["AcquisitionObject"]))
    graph.add((acquisition, Constants.PROV["wasGeneratedBy"], activity))
    graph.add((acquisition, Constants.NIDM["hadAcquisitionModality"],
               Constants.NIDM["MagneticResonanceImaging"]))
    graph.add((acquisition, Constants.NIDM["hadImageUsageType"], Constants.NIDM["Anatomical"]))
    graph.add((acquisition, NFO["filename"], Literal(f"bids::sub-0050002/anat/{name}_T1w.nii.gz")))
    return acquisition


def _add_assessment(graph, subject, name):
    """An instrument-based assessment, which is not an anatomical acquisition."""
    activity = URIRef(f"http://example.org/activity/{name}")
    assoc = BNode()
    graph.add((activity, RDF.type, Constants.NIDM["Acquisition"]))
    graph.add((activity, Constants.PROV["qualifiedAssociation"], assoc))
    graph.add((assoc, Constants.PROV["agent"], subject))
    graph.add((assoc, Constants.PROV["hadRole"], Constants.SIO["Subject"]))
    assessment = URIRef(f"http://example.org/assessment/{name}")
    graph.add((assessment, RDF.type, Constants.NIDM["AcquisitionObject"]))
    graph.add((assessment, RDF.type, Constants.NIDM["AssessmentObject"]))
    graph.add((assessment, Constants.PROV["wasGeneratedBy"], activity))


def _two_nodes_with_one_id(owners):
    graph = _project_graph()
    nodes = [_add_subject(graph, "first", "0050002"), _add_subject(graph, "second", "0050002")]
    for node in nodes:
        _add_assessment(graph, node, f"assessment-{node.split('/')[-1]}")
    acquisitions = {node: _add_anatomical_acquisition(graph, node, f"acq-{node.split('/')[-1]}")
                    for n, node in enumerate(nodes) if n in owners}
    return graph, nodes, acquisitions


@pytest.mark.parametrize("owner", [0, 1], ids=["first-owns", "second-owns"])
def test_of_several_matches_the_one_owning_the_anatomical_acquisition_is_chosen(owner, capsys):
    graph, nodes, acquisitions = _two_nodes_with_one_id({owner})

    add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    chosen = nodes[owner]
    assert _stats_subjects(graph) == {chosen}
    assert (None, Constants.PROV["used"], acquisitions[chosen]) in graph
    assert len(set(graph.subjects(SUBJECT_ID, None))) == 2
    out = capsys.readouterr().out
    assert str(chosen) in out and "anatomical" in out
    assert str(nodes[1 - owner]) in out


@pytest.mark.parametrize("owners", [set(), {0, 1}], ids=["neither-owns", "both-own"])
def test_several_matches_fail_unless_exactly_one_owns_an_anatomical_acquisition(owners):
    graph, nodes, _ = _two_nodes_with_one_id(owners)
    before = set(graph)

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    message = str(err.value)
    assert str(nodes[0]) in message and str(nodes[1]) in message
    assert "anatomical" in message and "No output written" in message
    assert set(graph) == before


# find_subject is the lookup add_seg_data uses, public so that a BIDS app can
# run the same rule before it computes anything. It never changes the graph.

def _find(graph, *args, **kwargs):
    from segstats_jsonld.fs_to_nidm import find_subject
    before = set(graph)
    try:
        return find_subject(graph, *args, **kwargs)
    finally:
        assert set(graph) == before


@pytest.mark.parametrize("given_id", SPELLINGS)
def test_find_subject_returns_the_matching_node(given_id):
    graph, subject, _ = _input_graph("0050002", None)
    _add_subject(graph, "other", "0050003")

    assert _find(graph, given_id) == subject


@pytest.mark.parametrize("owner", [0, 1])
def test_find_subject_returns_the_acquisition_owner_of_several_matches(owner):
    graph, nodes, _ = _two_nodes_with_one_id({owner})

    assert _find(graph, "sub-0050002") == nodes[owner]


@pytest.mark.parametrize("case, forceagent", [
    ("miss", True), ("miss", False), ("several", True), ("several", False), ("no-subjects", False),
])
def test_find_subject_raises_when_the_subject_cannot_be_identified(case, forceagent):
    graph = _failing_lookup_input(case)

    with pytest.raises(SubjectLookupError):
        _find(graph, "sub-0050002", forceagent=forceagent)


def test_find_subject_returns_none_only_for_forcenidm_without_subjects():
    graph = _project_graph()

    assert _find(graph, "sub-0050002", forceagent=True) is None
    with pytest.raises(SubjectLookupError, match="--forcenidm"):
        _find(graph, "sub-0050002")


def test_find_subject_does_not_count_agents_created_earlier_as_input_subjects():
    graph = _project_graph()
    created = _add_subject(graph, "created", "sub-0050002")

    assert _find(graph, "50002", created_agents={created}) == created
    assert _find(graph, "sub-0050009", forceagent=True, created_agents={created}) is None
    with pytest.raises(SubjectLookupError):
        _find(graph, "sub-0050009", forceagent=True)


def test_add_seg_data_looks_the_subject_up_with_find_subject(monkeypatch):
    from segstats_jsonld import fs_to_nidm
    calls = []

    def fake(graph, subject_id, forceagent=False, created_agents=()):
        calls.append((subject_id, forceagent))
        raise SubjectLookupError("from find_subject")

    monkeypatch.setattr(fs_to_nidm, "find_subject", fake)
    graph, _, _ = _input_graph("0050002", None)
    with pytest.raises(SubjectLookupError, match="from find_subject"):
        add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)
    assert calls == [("sub-0050002", True)]
