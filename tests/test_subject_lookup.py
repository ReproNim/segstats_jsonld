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


def test_an_untyped_node_is_not_a_subject():
    graph = _project_graph()
    graph.add((URIRef("http://example.org/thing"), SUBJECT_ID, Literal("0050002")))

    add_seg_data(graph, {}, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    (agent,) = _stats_subjects(graph)
    assert agent != URIRef("http://example.org/thing")


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
