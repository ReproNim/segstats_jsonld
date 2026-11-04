"""add_seg_data finds the input subject whichever literal form its id has.

PyNIDM 4.x's bids2nidm writes ndar:src_subject_id as "..."^^xsd:string and
PyNIDM 5's as a plain literal. rdflib treats the two as different terms, so a
lookup that spells the datatype matches only the 4.x form; on 5.x input
--forcenidm then minted a second subject agent with no link to the input's
demographics or acquisitions, and the run still succeeded.
"""
from types import SimpleNamespace

import pytest
from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef, XSD

from segstats_jsonld._pynidm import NIDM_SUBJECTID_URI, Constants
from segstats_jsonld.fs_to_nidm import add_seg_data

NFO = Namespace("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")
T1W = "sub-0050002_T1w.nii.gz"


def _input_graph(stored_id, datatype):
    graph = Graph()
    graph.add((URIRef("http://example.org/project"), RDF.type, Constants.NIDM["Project"]))
    subject = URIRef("http://example.org/agent/participant")
    graph.add((subject, RDF.type, Constants.PROV["Agent"]))
    graph.add((subject, RDF.type, Constants.PROV["Person"]))
    graph.add((subject, URIRef(NIDM_SUBJECTID_URI), Literal(stored_id, datatype=datatype)))

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


@pytest.mark.parametrize("datatype", [XSD.string, None], ids=["xsd-string", "plain"])
@pytest.mark.parametrize("stored_id", ["sub-0050002", "0050002", "50002"])
def test_add_seg_data_finds_subject_whatever_the_literal_form(stored_id, datatype):
    graph, subject, acquisition = _input_graph(stored_id, datatype)

    add_seg_data(
        graph,
        {},
        "sub-0050002",
        SimpleNamespace(uri="http://example.org/entity/fsstats"),
        add_to_nidm=True,
        forceagent=True,
        acq_filename=[T1W],
    )

    assert set(graph.subjects(URIRef(NIDM_SUBJECTID_URI), None)) == {subject}
    assert (None, Constants.PROV["used"], acquisition) in graph
