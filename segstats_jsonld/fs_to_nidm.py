#!/usr/bin/env python
#!/usr/bin/env python
#**************************************************************************************
#**************************************************************************************
#  fs_to_nidm.py
#  License: GPL
#**************************************************************************************
#**************************************************************************************
# Date: June 6, 2019                 Coded by: Brainhack'ers
# Filename: fs_to_nidm.py
#
# Program description:  This program will load in a aseg.stats file from Freesurfer
# , augment the Freesurfer anatomical region designations with common data element
# anatomical designations, and save the statistics + region designations out as
# NIDM serializations (i.e. TURTLE, JSON-LD RDF)
#
#
#**************************************************************************************
# Development environment: Python - PyCharm IDE
#
#**************************************************************************************
# System requirements:  Python 3.X
# Libraries: PyNIDM,
#**************************************************************************************
# Start date: June 6, 2019
# Update history:
# DATE            MODIFICATION				Who
#
#
#**************************************************************************************
# Programmer comments:
#
#
#**************************************************************************************
#**************************************************************************************


from segstats_jsonld._pynidm import (
    DD,
    NIDM_SUBJECTID_LOCALPART,
    NIDM_SUBJECTID_URI,
    Constants,
    DD_to_nidm,
    getUUID,
    tupleKeysToSimpleKeys,
)


# standard library
from pickle import dumps
import os
import sys
from os.path import join,basename,splitext,isfile,dirname
from socket import getfqdn
import glob
import pandas as pd
import warnings
import contextlib
import logging

import prov.model as prov
import json
import urllib.request as ur
from urllib.parse import urlparse
from rapidfuzz import fuzz


from rdflib import Graph, RDF, URIRef, util, term,Namespace,Literal,BNode, XSD
from rdflib.serializer import Serializer
from rdflib.plugins.sparql import prepareQuery
from segstats_jsonld.fsutils import read_stats, convert_stats_to_nidm, convert_csv_stats_to_nidm,\
    create_cde_graph

from io import StringIO

import tempfile


from segstats_jsonld import mapping_data

FREESURFER_CDE = \
    'https://raw.githubusercontent.com/ReproNim/segstats_jsonld/master/segstats_jsonld/mapping_data/fs_cde.ttl'

MEASURE_OF_KEY = {
    "http://purl.obolibrary.org/obo/PATO_0001591":"curvature",
    "http://purl.obolibrary.org/obo/PATO_0001323":"area",
    "http://uri.interlex.org/base/ilx_0111689":"thickness",
    "http://uri.interlex.org/base/ilx_0738276":"scalar",
    "http://uri.interlex.org/base/ilx_0112559":"volume",
    "https://surfer.nmr.mgh.harvard.edu/folding":"folding",
    "https://surfer.nmr.mgh.harvard.edu/BrainSegVol-to-eTIV":"BrainSegVol-to-eTIV",
    "https://surfer.nmr.mgh.harvard.edu/MaskVol-to-eTIV":"MaskVol-to-eTIV"
}

# minimum match score for fuzzy matching NIDM terms
MIN_MATCH_SCORE = 30

def get_fs_cdes():
    '''
    This function will load the standard FS CDEs for each region/measure located at:
    https://raw.githubusercontent.com/ReproNim/segstats_jsonld/master/segstats_jsonld/mapping_data/fs_cde.ttl
    :return: dataframe of nidm:measureOf entries and freesurfer CDE graph
    '''

    fs_graph = Graph()

    try:
        fs_graph.parse(location=FREESURFER_CDE, format="turtle")
    except Exception:
        logging.info("Error opening %s FS CDE file..continuing" % FREESURFER_CDE)
        exit


    # query for uuid, label, datumtype, measureOf
    query = '''
        prefix fs: <https://surfer.nmr.mgh.harvard.edu/>
        prefix nidm: <http://purl.org/nidash/nidm#>
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>

        select distinct ?uuid ?measure ?structure ?label
        where {
  
        ?uuid a fs:DataElement ;
            nidm:measureOf ?measure ;
            rdfs:label ?label ;
            fs:structure ?structure .
        } order by ?uuid '''

    measureOf_query = '''
    
        prefix fs: <https://surfer.nmr.mgh.harvard.edu/>
        prefix nidm: <http://purl.org/nidash/nidm#>
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix xsd: <http://www.w3.org/2001/XMLSchema#>

        select distinct ?measure
        where {
  
        ?uuid a fs:DataElement ;
            nidm:measureOf ?measure .
        }
    
    
    '''
    # list to store results
    measureOf_results=[]
    # run measureOf query
    qres = fs_graph.query(measureOf_query)
    # get bound variable names from query
    columns = [str(var) for var in qres.vars]

    # append result as row to result list
    for row in qres:
        measureOf_results.append(list(row))

    # convert results list to Pandas DataFrame and return
    measureOf_df = pd.DataFrame(measureOf_results, columns=columns)

    return fs_graph, measureOf_df

def map_csv_variables_to_freesurfer_cdes(df,id_field,outdir,csv_file,json_map=None):
    '''
    This function will cycle through the column names in the dataframe df and ask the user to match them to
    one of the freesufer CDEs
    :param df: CSV file dataframe
    :param measureOf_df: freesurfer measuresOf
    :param fs_graph: RDFLIB graph of freesurfer CDEs
    :return:
    '''

    if json_map != None:
        variable_to_freesurfer_cde = json_map

    else:

        variable_to_freesurfer_cde = {}

        # step 1: load freesurfer CDEs graph and get dataframe of selected properties
        print("---------------------------------------------------------------------------------------")
        print("\nLoading Freesurfer measurement types...")
        fs_graph,measureOf_df = get_fs_cdes()


        # step 3: ask the user whether all the data in this file comes from a single measurement type
        # such as all volumes or all thicknesses.  If so, we don't have to ask about each one
        option = 1
        print()
        for key, value in MEASURE_OF_KEY.items():
            print("%d: %s" % (option, value))
            option = option + 1
        print("%d: mixed measurement types" %option)

        selection = input("Select which measurement type the variables in your CSV file refer to or"
                          " select %d for mixed types: \t" %option)

        # Make sure user selected one of the options.  If not present user with selection input again
        while (not selection.isdigit()) or (int(selection) > int(option)):
            # Wait for user input
            selection = input("Please select an option (1:%d) from above: \t" % option)

        # store type of measurement this is
        if selection == option:
            measurement_type = "mixed"
        else:
            measurement_type = list(MEASURE_OF_KEY)[int(selection)-1]

        # step 4: cycle through remaining variables if not id_field and ask what region this measurement is for
        # if user selected a mixed type then we have to ask for the measurement type then the freesurfer region
        # otherwise we can just query for the regions x measurement type

        if measurement_type != "mixed":
            # WIP: hack for now....need to do this better
            if measurement_type == 'http://uri.interlex.org/base/ilx_0111689':
                datum = 'http://uri.interlex.org/base/ilx_0738265'
            else:
                datum = 'http://uri.interlex.org/base/ilx_0738276'
            # query for rdfs:label for measureOf == measurement_type
            query = '''
                prefix fs: <https://surfer.nmr.mgh.harvard.edu/>
                prefix nidm: <http://purl.org/nidash/nidm#>
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix xsd: <http://www.w3.org/2001/XMLSchema#>
    
                select distinct ?uuid ?label
                where {
      
                    ?uuid a fs:DataElement ;
                        nidm:measureOf ?measureOf ;
                        nidm:datumType ?datumType ;
                        rdfs:label ?label .
                } order by ?label
            
            '''

            # run query; the IRIs are bound, not pasted into the query text
            qres = fs_graph.query(query, initBindings={"measureOf": URIRef(measurement_type),
                                                        "datumType": URIRef(datum)})

            # dictionary to store freesurfer labels and UUIDs for measurement type
            freesurfer_region_labels = {}
            # append result as row to result list
            for row in qres:
                freesurfer_region_labels[str(row[1]).lower()] = row[0]

        # WIP: NEED TO ADD ADDITIONAL CODE WHEN TYPES ARE MIXED IN CSV FILE


        # dictionary storing freesurfer region UUID to CSV file variable mappings
        current_tuple = str(DD(source=csv_file, variable=id_field))
        variable_to_freesurfer_cde[current_tuple] = {}
        variable_to_freesurfer_cde[current_tuple]['source_variable'] = id_field
        variable_to_freesurfer_cde[current_tuple]['description'] = "subject/participant identifier"
        variable_to_freesurfer_cde[current_tuple]['valueType'] = URIRef(Constants.XSD["string"])
        variable_to_freesurfer_cde[current_tuple]['isAbout'] = {}
        variable_to_freesurfer_cde[current_tuple]['isAbout']['url'] = NIDM_SUBJECTID_URI
        variable_to_freesurfer_cde[current_tuple]['isAbout']['label'] = NIDM_SUBJECTID_LOCALPART

        # loop through variables in CSV
        for column in df.columns:
            current_tuple = str(DD(source=csv_file, variable=column))
            if column == id_field:
                continue
            elif current_tuple in variable_to_freesurfer_cde.keys():
                continue
            else:
                variable_to_freesurfer_cde[current_tuple] = {}
                variable_to_freesurfer_cde[current_tuple]['source_variable'] = column
                search_term = column
            # now present regions to use based on caseless fuzzy match of labels to variable
            go_loop = True
            while go_loop:
                option = 1
                print("---------------------------------------------------------------------------------------")
                print()
                print("Freesurfer Measure Association")
                print("Query String: %s \n" % search_term)

                # look for fuzzy matching freesurfer label
                match_scores = {}
                for key, value in freesurfer_region_labels.items():
                    match_scores[key] = {}
                    match_scores[key]['score'] = fuzz.token_sort_ratio(search_term.lower(), key)
                    match_scores[key]['uuid'] = value


                options = {}
                # present matches to user and give option to re-query
                for key, subdict in match_scores.items():
                    if match_scores[key]['score'] > MIN_MATCH_SCORE:
                        print("%d: Label: %s " % (option, key))
                        options[str(option)] = key
                        option = option + 1

                # Add option to change query string
                print("\n%d: Change query string from: \"%s\" \n" % (option, search_term))
                # Wait for user input
                selection = input("Please select an option (1:%d) from above: \t" % option)

                # Make sure user selected one of the options.  If not present user with selection input again
                while (not selection.isdigit()) or (int(selection) > int(option)):
                    # Wait for user input
                    selection = input("Please select an option (1:%d) from above: \t" % option)
                # check if selection is to re-run query with new search term
                if int(selection) == (option):
                    # ask user for new search string
                    print("---------------------------------------------------------------------------------------")
                    search_term = input("Please input new search string for CSV column: %s \t:" % column)
                    print("---------------------------------------------------------------------------------------")
                else:
                    # user selected a freesurfer term for this region
                    # store FS CDE UUID and CSV file variable mapping
                    variable_to_freesurfer_cde[current_tuple]['sameAs'] = match_scores[options[selection]]['uuid']
                    print("\nUser selection: %s, Freesurfer CDE UUID: %s" %(options[selection],
                                    match_scores[options[selection]]['uuid']))
                    go_loop = False

            # store variable_to_freesufer_cde mappings for later use
            with open(join(dirname(outdir), splitext(basename(csv_file))[0]+'_map.json'), 'w') as fp:
                json.dump(variable_to_freesurfer_cde, fp)

    # now ask user to supply some provenance information
    provenance={}
    print("---------------------------------------------------------------------------------------")
    print("\nNow you will be asked some questions intended to capture some provenance about the freesufer "
          "data you are storing in NIDM.  Ideally you should run this tool using the Freesurfer subject's "
          "directory or the aparc+aseg.mgz file.  Then provenance would be captures automatically for you.\n\n")
    provenance['description'] = input("Please input a textual description of how these data were created: \t")
    provenance['version'] = input("Please input the Freesufer version number used: \t")
    provenance['sysname'] = input("Please input operating system used: \t")



    # return variable_to_freesurfer_cde or maybe return the CDE graph?
    return variable_to_freesurfer_cde,provenance


def url_validator(url):
    '''
    Tests whether url is a valide url
    :param url: url to test
    :return: True for valid url else False
    '''
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc, result.path])

    except:
        return False

def select_acquisition(rows, acq_filenames, subjid, from_csv=False):
    '''
    Pick the anatomical AcquisitionObjects the stats were computed from.

    :param rows: query results with .acqObj and .filename (the input's nfo:filename)
    :param acq_filenames: file names of the images FreeSurfer used (recon-all
        averages repeated runs), or None
    :param subjid: subject identifier, for messages
    :param from_csv: the stats come from a -csv row, where -acq_filename is not accepted,
        so the warning names a remedy that is
    :return: list of AcquisitionObjects to link; empty to leave the stats unlinked

    A subject can have several anatomical acquisitions (sessions, runs, acq-
    variants). With acq_filenames, each name must match exactly one acquisition,
    and all of them are linked. Without it, a single candidate is linked. Anything
    else is left unlinked with a warning rather than linked to a guess.
    '''
    if not acq_filenames:
        candidates = sorted({r.acqObj for r in rows})
        if len(candidates) == 1:
            return candidates
        if not candidates:
            warnings.warn(f"No anatomical AcquisitionObject found for subject {subjid}; "
                          f"the stats are not linked to an acquisition")
        else:
            if from_csv:
                remedy = ("to link them, convert this subject's stats with -s or -f and name the images "
                          "with -acq_filename")
            else:
                remedy = "-acq_filename names the ones used"
            warnings.warn(f"{len(candidates)} anatomical AcquisitionObjects found for subject {subjid}; "
                          f"the stats are not linked to an acquisition ({remedy})")
        return []
    chosen = []
    for name in acq_filenames:
        want = os.path.basename(name)
        matches = sorted({r.acqObj for r in rows
                          if r.filename is not None and str(r.filename).split('/')[-1] == want})
        if len(matches) != 1:
            warnings.warn(f"{len(matches)} anatomical AcquisitionObjects with file name {want} found for "
                          f"subject {subjid}; the stats are not linked to an acquisition")
            return []
        chosen.append(matches[0])
    return sorted(set(chosen))


class SubjectLookupError(LookupError):
    """The subject the stats belong to cannot be identified in the input NIDM."""


def normalise_subject_id(subject_id):
    '''
    The form two subject ids are compared in: without a leading "sub-" and
    without leading zeros, so that sub-0051456, 0051456 and 51456 are the same
    subject. BIDS directories carry "sub-"; participants.tsv, and so the
    input NIDM, may or may not, and some datasets drop the zero padding.
    '''
    text = str(subject_id)
    if text.startswith("sub-"):
        text = text[len("sub-"):]
    stripped = text.lstrip("0")
    # an id of only zeros keeps one, so "0" and "00" stay comparable
    return stripped if stripped or not text else "0"


# ndar:src_subject_id as PyNIDM writes it, and under the http:// form of the
# same NDAR IRI, which other NIDM producers use.
SUBJECT_ID_PREDICATES = (
    URIRef(NIDM_SUBJECTID_URI),
    URIRef(NIDM_SUBJECTID_URI.replace("https://", "http://", 1)),
)


def _input_subject_agent(nidmdoc, subjid, forceagent=False, created_agents=None):
    '''
    Return the subject in nidmdoc whose id is subjid, comparing both sides with
    normalise_subject_id. A subject is any node carrying ndar:src_subject_id under
    the https:// or the http:// NDAR IRI, whatever its rdf:type, so a participant
    typed only nidm:Subject, or not typed, is never mistaken for an input without
    subjects. The
    literal is compared as text, so PyNIDM 4.x's "..."^^xsd:string and
    PyNIDM 5's plain literal both match.

    An agent with several ids counts once. No match: if the input NIDM has
    subject agents, the run cannot tell which one the stats belong to, and
    SubjectLookupError is raised whatever forceagent says; if it has none,
    forceagent creates an agent with that id, and without it SubjectLookupError
    is raised. Several matches also raise. These are the rules of
    ants_seg_to_nidm's lookup, so both converters find the same subject.

    :param created_agents: agents created by earlier calls in this run (-s
        with -n converts each stats file separately; -csv one row at a time).
        They are reused when they match, and do not count as input subjects.
    '''
    if created_agents is None:
        created_agents = set()
    subject_ids = {}
    for predicate in SUBJECT_ID_PREDICATES:
        for agent, sid in nidmdoc.subject_objects(predicate):
            subject_ids.setdefault(agent, set()).add(str(sid))

    wanted = normalise_subject_id(subjid)
    matches = sorted(agent for agent, ids in subject_ids.items()
                     if any(normalise_subject_id(i) == wanted for i in ids))

    if len(matches) > 1:
        found = "; ".join("%s (%s)" % (agent, ", ".join(sorted(subject_ids[agent]))) for agent in matches)
        raise SubjectLookupError("Subject ID %s matches %d subject agents in the input NIDM: %s. "
                                 "No output written" % (subjid, len(matches), found))
    if matches:
        agent = matches[0]
        if agent in created_agents:
            print('Using the agent created earlier in this run for subject ID %s (agent: %s)' % (subjid, agent))
        else:
            print('Found subject ID %s in NIDM file as %s (agent: %s)'
                  % (subjid, ", ".join(sorted(subject_ids[agent])), agent))
        return agent

    input_ids = sorted(i for agent, ids in subject_ids.items() if agent not in created_agents for i in ids)
    if input_ids:
        raise SubjectLookupError("Subject ID %s not found in the input NIDM, whose subject IDs are: %s. "
                                 "IDs are compared without a leading \"sub-\" and leading zeros. "
                                 "No output written" % (subjid, ", ".join(input_ids)))
    if forceagent is not False:
        print('Subject ID %s: the input NIDM has no subjects; creating an agent for it (--forcenidm)' % subjid)
        warnings.warn(f"Subject ID {subjid} not found in the input NIDM, which has no subjects; the stats "
                      f"are attached to a new agent with that ID (--forcenidm)")
        agent = Namespace("http://iri.nidash.org/")[getUUID()]
        nidmdoc.add((agent, RDF.type, Constants.PROV['Agent']))
        nidmdoc.add((agent, URIRef(NIDM_SUBJECTID_URI), Literal(subjid, datatype=XSD.string)))
        created_agents.add(agent)
        return agent
    raise SubjectLookupError("Subject ID %s not found in the input NIDM, which has no subject agents; "
                             "no output written (--forcenidm adds a new agent for it)" % subjid)


def _check_csv_subjects(nidm_file, subject_ids, forceagent):
    """
    Raise SubjectLookupError if any of the -csv rows' subject ids would fail the
    lookup add_seg_data makes, without asking anything or writing anything. Runs
    the same lookup, in row order, on a throwaway copy of the input NIDM.
    """
    graph = Graph()
    graph.parse(nidm_file, format=util.guess_format(nidm_file))
    created = set()
    with contextlib.redirect_stdout(StringIO()), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for subjid in subject_ids:
            _input_subject_agent(graph, subjid, forceagent, created)


def add_seg_data(nidmdoc, header, subjid, fs_stats_entity_id, add_to_nidm=False, forceagent=False, description=None, acq_filename=None, created_agents=None, from_csv=False):
    '''
    WIP: this function creates a NIDM file of brain volume data and if user supplied a NIDM-E file it will add brain volumes to the
    NIDM-E file for the matching subject ID
    :param nidmdoc:
    :param header:
    :param subjid:
    :param fs_stats_entity_id:
    :param add_to_nidm:
    :param forceagent:
    :param description:
    :param acq_filename: file names of the anatomical images the stats came from, used to
        pick their AcquisitionObjects in the input NIDM (see select_acquisition)
    :param created_agents: set of subject agents created by earlier calls in the same run
        (see _input_subject_agent); updated in place
    :return:
    :raises SubjectLookupError: with add_to_nidm, when the subject cannot be identified
    '''

    #for each of the header items create a dictionary where namespaces are freesurfer
    niiri=Namespace("http://iri.nidash.org/")
    nidmdoc.bind("niiri",niiri)
    # add namespace for subject id
    ndar = Namespace(Constants.NDAR)
    nidmdoc.bind("ndar",ndar)
    dct = Namespace(Constants.DCT)
    nidmdoc.bind("dct",dct)
    sio = Namespace(Constants.SIO)
    nidmdoc.bind("sio",sio)
    nidm = Namespace(Constants.NIDM)
    nidmdoc.bind("nidm",nidm)

    # identify the input subject before anything is added, so a lookup that
    # fails leaves the graph as it was
    if add_to_nidm:
        participant_agent = _input_subject_agent(nidmdoc, subjid, forceagent, created_agents)

    software_activity = niiri[getUUID()]
    nidmdoc.add((software_activity,RDF.type,Constants.PROV['Activity']))
    # if the freesurfer statistics are coming from a CSV file then the user was asked how these results
    # were created and thus we store the user-supplied description instead of the generic "Freesurfer
    # segmentation statistics"
    if description is None:
        nidmdoc.add((software_activity,Constants.DCT["description"],Literal("Freesurfer segmentation statistics")))
    else:
        nidmdoc.add((software_activity, Constants.DCT["description"], Literal(description)))

    fs = Namespace(Constants.FREESURFER)

    # add freesurfer aparc+aseg header items to NIDM file.  If we have statistics from a CSV file instead of from
    # freesurfer results directly then we can ignore the description field which is stored above.
    for key,value in header.items():
        if key == 'description':
            continue
        nidmdoc.add((software_activity,fs[key],Literal(value)))

    #create software agent and associate with software activity
    #search and see if a software agent exists for this software, if so use it, if not create it
    for software_uid in nidmdoc.subjects(predicate=Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE,object=URIRef(Constants.FREESURFER) ):
        software_agent = software_uid
        break
    else:
        software_agent = niiri[getUUID()]
    nidmdoc.add((software_agent,RDF.type,Constants.PROV['Agent']))
    neuro_soft=Namespace(Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE)
    nidmdoc.add((software_agent,Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE,URIRef(Constants.FREESURFER)))
    nidmdoc.add((software_agent,RDF.type,Constants.PROV["SoftwareAgent"]))
    association_bnode = BNode()
    nidmdoc.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
    nidmdoc.add((association_bnode,RDF.type,Constants.PROV['Association']))
    nidmdoc.add((association_bnode,Constants.PROV['hadRole'],Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE))
    nidmdoc.add((association_bnode,Constants.PROV['agent'],software_agent))


    if not add_to_nidm:
        # create a new agent for subjid
        participant_agent = niiri[getUUID()]
        nidmdoc.add((participant_agent,RDF.type,Constants.PROV['Agent']))
        nidmdoc.add((participant_agent,RDF.type,Constants.PROV['Person']))
        nidmdoc.add((participant_agent,URIRef(Constants.NDAR["src_subject_id"]),Literal(subjid, datatype=XSD.string)))

    else:
            # When extending an existing NIDM file, link the segmentation software activity to the
            # anatomical acquisition used to produce the stats (mirrors FSL workflow).
            query_acq = prepareQuery(
                """
                PREFIX prov:  <http://www.w3.org/ns/prov#>
                PREFIX nidm:  <http://purl.org/nidash/nidm#>
                PREFIX nfo:   <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
                SELECT DISTINCT ?acqObj ?filename
                WHERE {
                    ?acqObj a nidm:AcquisitionObject ;
                    prov:wasGeneratedBy/prov:qualifiedAssociation/prov:agent ?subject ;
                    nidm:hadAcquisitionModality nidm:MagneticResonanceImaging ;
                    nidm:hadImageUsageType nidm:Anatomical .
                    OPTIONAL { ?acqObj nfo:filename ?filename }
                }
                """
            )
            res = list(nidmdoc.query(query_acq, initBindings={"subject": participant_agent}))
            for acq_obj in select_acquisition(res, acq_filename, subjid, from_csv=from_csv):
                nidmdoc.add((software_activity, Constants.PROV['used'], acq_obj))

    #create a blank node and qualified association with prov:Agent for participant
    association_bnode = BNode()
    nidmdoc.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
    nidmdoc.add((association_bnode,RDF.type,Constants.PROV['Association']))
    nidmdoc.add((association_bnode,Constants.PROV['hadRole'],Constants.SIO["Subject"]))
    nidmdoc.add((association_bnode,Constants.PROV['agent'],participant_agent))

    # add association between FSStatsCollection and computation activity
    nidmdoc.add((URIRef(fs_stats_entity_id.uri),Constants.PROV['wasGeneratedBy'],software_activity))

    if add_to_nidm:
        # get project uuid from NIDM doc and make association with software_activity
        query = """
                                    prefix nidm: <http://purl.org/nidash/nidm#>
                                    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
                                    select distinct ?project
                                    where {
    
                                        ?project rdf:type nidm:Project .
    
                                    }"""

        qres = nidmdoc.query(query)
        for row in qres:
            project_activity = row['project']
            #nidmdoc.add((software_activity, Constants.DCT["isPartOf"], row['project']))

    else:
        project_activity = niiri[getUUID()]
        nidmdoc.add((project_activity, RDF.type, Constants.NIDM['Project']))

    # create session for this
    session_activity = niiri[getUUID()]
    nidmdoc.add((session_activity, RDF.type, Constants.PROV['Activity']))
    nidmdoc.add((session_activity, RDF.type, Constants.NIDM['Session']))


    nidmdoc.add((session_activity, Constants.DCT["isPartOf"], project_activity))
    # associate freesurfer segmentation activity with session
    nidmdoc.add((software_activity, Constants.DCT["isPartOf"], session_activity))




def read_buildstamp(subdir):
    """
    if provided with a freesurfer subject directory, check whether the build_stamp.txt file
    exists, and if so, extract the freesurfer version.
    :param subdir: path to subject directory, e.g. args.subject_dir
    """
    if os.path.exists(subdir):
        try:
            with open(subdir + '/scripts/build-stamp.txt', 'r') as f:
                freesurfer_version = f.readlines()[0]
        # except a FileNotFound error
        except OSError as e:
            freesurfer_version = input(
                """
                Could not find a build timestamp in the supplied subject directory.
                The used freesurfer version can not be extracted. Please enter the
                version of freesurfer you are using, if available: """
                or "")
    return freesurfer_version







def test_connection(remote=False):
    """helper function to test whether an internet connection exists.
    Used for preventing timeout errors when scraping interlex."""
    import socket
    remote_server = 'www.google.com' if not remote else remote # TODO: maybe improve for China
    try:
        # does the host name resolve?
        host = socket.gethostbyname(remote_server)
        # can we establish a connection to the host name?
        con = socket.create_connection((host, 80), 2)
        return True
    except:
        print("Can't connect to a server...")
        pass
    return False




def main():

    import argparse
    parser = argparse.ArgumentParser(prog='fs_to_nidm.py',
                                     description='This program reads FreeSurfer stats files (e.g. aseg.stats), '
                                     'maps the FreeSurfer regions and measures to common data elements, and writes '
                                     'the statistics as NIDM (Turtle or JSON-LD), either as new files (-o) or added '
                                     'to an existing NIDM file (-n).')
    #DBK: added mutually exclusive arguments to support pulling a named stats file (e.g. aseg.stats) as a URL such as
    #data hosted in an amazon bucket or from a mounted filesystem where you don't have access to the original
    #subjects directory.

    group = parser.add_mutually_exclusive_group(required=True)
    group2 = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-s', '--subject_dir', dest='subject_dir', type=str,
                        help='Path to Freesurfer subject directory')
    group.add_argument('-f', '--seg_file', dest='segfile', type=str,help='Path or URL to a specific Freesurfer '
                            'stats file. Check supported_files for list of supported stats files.')
    group.add_argument('-csv', '--csv_file', dest='csvfile', type=str, help='Path to CSV file which includes '
                            'a header row with 1 column containing subject IDs and the other columns are variables '
                            'indicating the Freesurfer-derived region measure (e.g. volume, surface area, etc.  If '
                            'you use this mode of running this software you will be asked to match your region '
                            'variables to standard Freesurfer region / measure labels presented to you (so this '
                            'program will end up being interactive.  In future WIP we will add the ability to export '
                            'a json sidecar file with those mappings for automated runs of future CSV files with the '
                            'same set of variables.')
    parser.add_argument('-subjid','--subjid',dest='subjid',required=False, help='Subject identifier to be used in '
                            'the NIDM files. Required with -f/--seg_file. With -s/--subject_dir it defaults to the '
                            'name of the subject directory, and when given it overrides the subject directory name '
                            '(set it when that directory is not named after the subject, e.g. sub-01/freesurfer). '
                            'Earlier releases ignored -subjid with -s and always used the directory name. '
                            'Ignored with -csv, where the ID column names each subject')
    group2.add_argument('-o', '--output', dest='output_dir', metavar='OUTPUT', type=str, required=False, default=None,
                        help='Where to write a new NIDM file. With -s, the directory that gets one '
                             '<stats>_nidm.ttl (or .jsonld) per stats file; with -f or -csv, the output file. '
                             'Exactly one of -o/--output and -n/--nidm is required')
    group2.add_argument('-n','--nidm', dest='nidm_file', type=str, required=False,default=None,
                        help='Existing NIDM file to add the stats to; it is rewritten in place (with -j the '
                             'result goes to <NIDM_FILE>.json instead). Exactly one of -o/--output and '
                             '-n/--nidm is required')
    parser.add_argument('-j', '--jsonld', dest='jsonld', action='store_true', default = False,
                        help='If flag set then NIDM file will be written as JSONLD instead of TURTLE. With -n '
                             'the JSON-LD goes to <NIDM_FILE>.json. Not allowed with -s and -n together')
    parser.add_argument('-add_de', '--add_de', dest='add_de', action='store_true', default = None,
                        help='If flag set then the data element dictionary is added to the NIDM file, else it is '
                             'written to a separate file, fs_cde.ttl, in the output directory (or the directory '
                             'of the -n NIDM file)')
    parser.add_argument('-forcenidm','--forcenidm', action='store_true',required=False,
                        help='With -n: if the NIDM file has no subjects at all, add a new subject agent with the '
                             'subject ID and attach the stats to it. If the file has subjects and none matches the '
                             'subject ID (compared without a leading "sub-" and leading zeros), the run fails '
                             'with or without this option')
    parser.add_argument('-json_map', '--json_map', dest='json_map', required=False, default=None,
                        help='If storing freesurfer segmentation data stored in a CSV file and you\'ve previously '
                             'stored the variable to Freesurfer CDE mappings, this can be reused.  Otherwise '
                             'this argument is ignored')
    parser.add_argument('-acq_filename', '--acq_filename', dest='acq_filename', nargs='+', required=False, default=None,
                        help='With -s or -f and -n: file names (or paths) of the anatomical images the stats were '
                             'computed from (several for repeated runs). The stats are linked to the input NIDM '
                             'acquisitions with those file names if each name matches exactly one of the subject\'s '
                             'anatomical acquisitions, and left unlinked otherwise. Without it the stats are linked '
                             'only when the subject has a single anatomical acquisition')
    args = parser.parse_args()



    # test whether user supplied stats file directly and if so they the subject id must also be supplied so we
    # know which subject the stats file is for
    if args.segfile and (args.subjid is None):
        parser.error("-f/--seg_file requires -subjid/--subjid to be set!")
    # -acq_filename names one subject's images: with -csv it would apply to every row's
    # subject, and without -n there is no input acquisition to link to
    if args.acq_filename and args.csvfile:
        parser.error("-acq_filename applies to -s/--subject_dir or -f/--seg_file, not -csv/--csv_file")
    if args.acq_filename and not args.nidm_file:
        parser.error("-acq_filename requires -n/--nidm")
    # -s with -n converts each stats file against a fresh read of the -n file;
    # in Turtle the result is written back to that file, so the next stats file
    # builds on it, but -j writes <nidm>.json instead, so every stats file but
    # the last one would be lost
    if args.jsonld and args.subject_dir and args.nidm_file:
        parser.error("-j/--jsonld cannot be combined with -s/--subject_dir and -n/--nidm: only the last "
                     "stats file would be kept. Omit -j to add the stats to the -n file as Turtle, or use "
                     "-o OUTPUT_DIR with -j to write one JSON-LD file per stats file")

    try:
        _convert(args)
    except SubjectLookupError as err:
        # nothing has been written when the lookup fails
        sys.exit("fs_to_nidm.py: error: %s" % err)


def _convert(args):
    """Run the conversion main() parsed and checked the arguments for."""
    # subject agents this run creates (--forcenidm), shared by the per-stats-file
    # and per-row calls of add_seg_data
    created_agents = set()

    # if output_dir doesn't exist then create it
    #if args.output_dir is not None:
    #    out_path = os.path.dirname(args.output_dir)
    #    if not os.path.exists(out_path):
    #        os.makedirs(out_path)

    # WIP: trying to find a way to reference data in module. This does not feel kosher but works
    #datapath = mapping_data.__path__._path[0] + '/'
    # changed by DBK
    datapath = mapping_data.__path__[0] + '/'
    supported_files=[
        'rh.BA_exvivo.stats', 'rh.aparc.pial.stats', 'lh.aparc.pial.stats',
        'rh.aparc.DKTatlas.stats', 'lh.aparc.stats', 'brainvol.stats', 'rh.aparc.a2009s.stats',
        'rh.aparc.stats', 'lh.w-g.pct.stats', 'lh.aparc.a2009s.stats', 'lh.BA_exvivo.stats',
        'lh.BA_exvivo.thresh.stats', 'rh.w-g.pct.stats', 'rh.BA_exvivo.thresh.stats',
        'entowm.stats', 'vsinus.stats', 'lh.aparc.DKTatlas.stats', 'wmparc.stats', 'aseg.stats'
    ]


    # if we set -s or --subject_dir as parameter on command line...
    if args.subject_dir is not None:
        stats_dir = os.path.join(args.subject_dir, "stats")
        if not any(basename(f) in supported_files for f in glob.glob(os.path.join(stats_dir, "*.stats"))):
            sys.exit("fs_to_nidm.py: error: no supported stats file in %s (looked for: %s); "
                     "no output written" % (stats_dir, ", ".join(sorted(supported_files))))
        # get the freesurfer version for later use
        freesurfer_version = read_buildstamp(args.subject_dir)
        # files=['aseg.stats']
        # -subjid names the subject when the directory is not named after it
        # (e.g. <out>/sub-<id>/freesurfer); otherwise use the directory name
        subjid = args.subjid if args.subjid else os.path.basename(os.path.normpath(args.subject_dir))
        for stats_file in glob.glob(os.path.join(args.subject_dir,"stats","*.stats")):
            if basename(stats_file) in supported_files:
                print("Processing stats file: %s" %stats_file)
                #read in stats file
                [measures, header] = read_stats(stats_file)
                [e, doc] = convert_stats_to_nidm(measures)
                g = create_cde_graph()

                # for measures we need to create NIDM structures using anatomy mappings
                # If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
                if args.nidm_file is None:

                    print("Creating NIDM file...")
                    # If user did not choose to add this data to an existing NIDM file then create a new one for the CSV data

                    # convert provtoolbox rdf stats graph to rdflib graph for ease of working with it
                    g2 = Graph()
                    g2.parse(source=StringIO(doc.serialize(format='rdf', rdf_format='ttl')),format='ttl')

                    if args.add_de is not None:
                        nidmdoc = g+g2
                    else:
                        nidmdoc = g2

                    # WIP: more thought needed for version that works with adding to existing NIDM file versus creating a new NIDM file....
                    add_seg_data(nidmdoc=nidmdoc,header=header,subjid=subjid,fs_stats_entity_id=e.identifier, 
                                acq_filename=args.acq_filename)

                    #serialize NIDM file
                    if args.jsonld is not False:
                        # with open(join(args.output_dir,splitext(basename(stats_file))[0]+'.json'),'w') as f:
                        #with open(join(args.output_dir,splitext(basename(stats_file))[0]+'_nidm.json'),'w') as f:
                        print("Writing NIDM file...")
                        nidmdoc.serialize(join(args.output_dir,splitext(basename(stats_file))[0]+'_nidm.jsonld'),format='json-ld',indent=4)
                    else:
                        # with open(join(args.output_dir,splitext(basename(stats_file))[0]+'.ttl'),'w') as f:
                        # with open(join(args.output_dir,splitext(basename(stats_file))[0]+'_nidm.ttl'),'w') as f:
                        print("Writing NIDM file...")
                        nidmdoc.serialize(join(args.output_dir,splitext(basename(stats_file))[0]+'_nidm.ttl'),format='turtle')

                    # added to support separate cde serialization
                    if args.add_de is None:
                        # serialize cde graph
                        g.serialize(destination=join((args.output_dir),"fs_cde.ttl"),format='turtle')

                    # doc.save_DotGraph(join(args.output_dir,splitext(basename(stats_file))[0] + ".pdf"), format="pdf")
                # we adding these data to an existing NIDM file
                else:
                    #read in NIDM file with rdflib
                    g1 = Graph()
                    g1.parse(args.nidm_file,format=util.guess_format(args.nidm_file))

                    # convert nidm stats graph to rdflib
                    g2 = Graph()
                    g2.parse(source=StringIO(doc.serialize(format='rdf',rdf_format='turtle')),format='turtle')

                    if args.add_de is not None:
                        print("Combining graphs...")
                        nidmdoc = g + g1 + g2
                    else:
                        nidmdoc = g1 + g2

                    if args.forcenidm is not False:
                        add_seg_data(nidmdoc=nidmdoc,header=header,subjid=subjid,fs_stats_entity_id=e.identifier, 
                                    acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents, forceagent=True)
                    else:
                        add_seg_data(nidmdoc=nidmdoc,header=header,subjid=subjid,fs_stats_entity_id=e.identifier, 
                                    acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents)


                    #serialize NIDM file
                    print("Writing Augmented NIDM file...")
                    if args.jsonld is not False:
                        nidmdoc.serialize(destination=args.nidm_file + '.json',format='json-ld')
                    else:
                        nidmdoc.serialize(destination=args.nidm_file,format='turtle')

                    if args.add_de is None:
                        # serialize cde graph
                        output_path = dirname(args.output_dir) if args.output_dir else dirname(args.nidm_file)
                        g.serialize(destination=join(output_path,"fs_cde.ttl"),format='turtle')

    # else if the user didn't set subject_dir on command line then they must have set a segmentation file directly
    elif args.segfile is not None:

        # here we're supporting amazon bucket-style file URLs where the expectation is the last parameter of the
        # see if we have a valid url
        url = url_validator(args.segfile)
        # if user supplied a url as a segfile
        if url is not False:

            # check to see if the supplied segfile is in supported_files
            if not any(ext in args.segfile for ext in supported_files):
                print("ERROR! Only Freesurfer stats files currently supported are: \n")
                print(supported_files)
                print("You supplied to following URL which must contain the string from one of the supported files: %s " %args.segfile)
                exit()



            #try to open the url and get the pointed to file
            try:
                #open url and get file
                opener = ur.urlopen(args.segfile)
                # write temporary file to disk and use for stats
                temp = tempfile.NamedTemporaryFile(delete=False)
                temp.write(opener.read())
                temp.close()
                stats_file = temp.name
            except:
                print("ERROR! Can't open url: %s" %args.segfile)
                exit()

            # since all of the above worked, all we need to do is set the output file name to be the
            # args.subjid + "_" + [everything after the last / in the supplied URL]
            url_parts = urlparse(args.segfile)
            path_parts = url_parts[2].rpartition('/')
            output_filename = args.subjid + "_" + splitext(path_parts[2])[0]

        # else this must be a path to a stats file
        else:
            if isfile(args.segfile):
                stats_file = args.segfile
                # set outputfilename to be the args.subjid + "_" + args.segfile
                output_filename = args.subjid + "_" + splitext(basename(args.segfile))[0]
            else:
                print("ERROR! Can't open stats file: %s " %args.segfile)
                exit()




        #read in stats file
        [measures, header] = read_stats(stats_file)
        [e, doc] = convert_stats_to_nidm(measures)
        g = create_cde_graph()
        # doc.serialize("/Users/dbkeator/Downloads/doc.ttl",format='rdf',rdf_format='turtle')
        # with open("/Users/dbkeator/Downloads/g.ttl",'w') as outfile:
        #    outfile.write(g.serialize(format='ttl').decode('utf-8'))

        # for measures we need to create NIDM structures using anatomy mappings
        # If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
        if args.nidm_file is None:

            print("Creating NIDM file...")
            # If user did not choose to add this data to an existing NIDM file then create a new one for the CSV data

            # convert nidm stats graph to rdflib
            g2 = Graph()
            g2.parse(source=StringIO(doc.serialize(format='rdf',rdf_format='turtle')),format='turtle')

            if args.add_de is not None:
                nidmdoc = g+g2
            else:
                nidmdoc = g2

            add_seg_data(nidmdoc=nidmdoc,header=header,subjid=args.subjid,fs_stats_entity_id=e.identifier, 
                        acq_filename=args.acq_filename)


            #serialize NIDM file
            print("Writing NIDM file...")
            if args.jsonld is not False:
                # nidmdoc.serialize(destination=join(args.output_dir,output_filename) + '.ttl',format='jsonld')
                nidmdoc.serialize(destination=join(args.output_dir),format='json-ld', indent=4)
            else:
                # nidmdoc.serialize(destination=join(args.output_dir,output_filename) + '.ttl',format='turtle')
                nidmdoc.serialize(destination=join(args.output_dir),format='turtle')

            # added to support separate cde serialization
            if args.add_de is None:
                # serialize cde graph
                g.serialize(destination=join(dirname(args.output_dir),"fs_cde.ttl"),format='turtle')

            #nidmdoc.save_DotGraph(join(args.output_dir,output_filename + ".pdf"), format="pdf")
        # we adding these data to an existing NIDM file
        else:
            #read in NIDM file with rdflib
            g1 = Graph()
            g1.parse(args.nidm_file,format=util.guess_format(args.nidm_file))

            # convert nidm stats graph to rdflib
            g2 = Graph()
            g2.parse(source=StringIO(doc.serialize(format='rdf',rdf_format='turtle')),format='turtle')

            if args.add_de is not None:
                print("Combining graphs...")
                nidmdoc = g + g1 + g2
            else:
                nidmdoc = g1 + g2

            if args.forcenidm is not False:
                add_seg_data(nidmdoc=nidmdoc,header=header,subjid=args.subjid,fs_stats_entity_id=e.identifier, 
                            acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents, forceagent=True)
            else:
                add_seg_data(nidmdoc=nidmdoc,header=header,subjid=args.subjid,fs_stats_entity_id=e.identifier, 
                            acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents)


            #serialize NIDM file
            print("Writing Augmented NIDM file...")
            if args.jsonld is not False:
                nidmdoc.serialize(destination=args.nidm_file + '.json',format='json-ld')
            else:
                nidmdoc.serialize(destination=args.nidm_file,format='turtle')

            if args.add_de is None:
                # serialize cde graph
                g.serialize(destination=join(dirname(args.nidm_file), "fs_cde.ttl"), format='turtle')
    elif args.csvfile is not None:
        # added to support CSV files with volume data from freesurfer
        # problem is the variable names aren't guaranteed to match freesurfer names so we have to do some
        # interactions with the user...
        
        # step 1: read CSV file and get header row
        print("---------------------------------------------------------------------------------------")
        print("Loading CSV file....")
        df = pd.read_csv(args.csvfile)

        # load previous mappings...assumes complete mappings
        if args.json_map is not None:
            print("Loading user-supplied JSON mapping file....")
            with open(args.json_map) as json_file:
                json_map = json.load(json_file)

            # loop through keys and see which is the subject ID field
            stop_loops=False
            for key,value in json_map.items():
                if stop_loops:
                    break
                for subkey,subvalue in json_map[key].items():
                    if subkey == 'isAbout':
                        if json_map[key][subkey]['url'] == NIDM_SUBJECTID_URI:
                            id_field = key.split("variable")[1].split("=")[1].split(")")[0].lstrip(
                            "'").rstrip("'")
                            stop_loops = True
                            break

            #id_field = list(json_map.keys())[list(json_map.values()).
            #    index(Constants.NIDM_SUBJECTID.uri)]

        # step 2: ask which column has subject ID or load json_map if provided
        else:
            json_map = None
            option = 1
            for column in df.columns:
                print("%d: %s" % (option, column))
                option = option + 1
            selection = input("Please select the subject ID field from the list above: ")
            # Make sure user selected one of the options.  If not present user with selection input again
            while (not selection.isdigit()) or (int(selection) > int(option)):
                # Wait for user input
                selection = input("Please select the subject ID field from the list above: \t" % option)
            id_field = df.columns[int(selection) - 1]


        # re-read data file with constraint that key field is read as string
        df = pd.read_csv(args.csvfile, dtype={id_field: str})

        # look every row's subject up before the interactive mapping, which saves
        # <csv>_map.json, so a run that cannot find a subject writes nothing
        if args.nidm_file:
            _check_csv_subjects(args.nidm_file, [str(v) for v in df[id_field]], args.forcenidm)

        # step 3: interact with the user about the variables in this CSV file.
        if args.nidm_file:
            var_to_freesurfer_cde, provenance = map_csv_variables_to_freesurfer_cdes(df=df, id_field=id_field,
                    json_map=json_map,outdir=args.nidm_file,csv_file=args.csvfile)

        else:
            var_to_freesurfer_cde,provenance = map_csv_variables_to_freesurfer_cdes(df=df,id_field=id_field,
                    json_map=json_map,outdir=args.output_dir,csv_file=args.csvfile)

        # step 4: create the Freesurfer CDE graph

        g = create_cde_graph()

        # step 4b: create the personal data elements mapping CSV variables to the Freesurfer CDEs

        pde = DD_to_nidm(var_to_freesurfer_cde)


        first_row=True
        nidm_file = args.nidm_file
        print("Creating NIDM file...")

        if (nidm_file is None):
            nidmdoc = Graph()
            niiri = Namespace("http://iri.nidash.org/")

            project_activity = niiri[getUUID()]
            nidmdoc.add((project_activity, RDF.type, Constants.PROV['Activity']))
            nidmdoc.add((project_activity, RDF.type, Constants.NIDM['Project']))
            nidmdoc.add((project_activity, Constants.DCT["description"],
                         Literal("Freesurfer segmentation statistics from CSV file")))
            nidmdoc.add((project_activity, Constants.NFO["filename"],
                         Literal(basename(args.csvfile))))
            nidmdoc.add((project_activity, Constants.PROV["Location"],
                         Literal(dirname(args.csvfile))))

        # step 5: now we need to cycle through each row of the CSV file and store the data in graph
        for index, row in df.iterrows():
            # here we need to create the nidm statements to store the volume measures, the PDE entities
            # to stores the mappings between the variables and the freesurfer CDEs and the activity provenance
            # information.

            subjid = str(row[id_field])

            print("\tProcessing subject: %s..." %subjid)

            [e, doc] = convert_csv_stats_to_nidm(row,var_to_freesurfer_cde,filename=basename(args.csvfile),id_column=id_field)

            # add session for this row and associations with project
            #session_activity = niiri[getUUID()]
            #nidmdoc.add((session_activity, RDF.type, Constants.PROV['Activity']))
            #nidmdoc.add((session_activity, RDF.type, Constants.NIDM['Session']))
            #nidmdoc.add((session_activity, Constants.DCT["isPartOf"], project_activity))
            ## associate freesurfer segmentation activity with session
            #nidmdoc.add((URIRef(e.identifier.uri), Constants.DCT["isPartOf"], session_activity))


            # If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
            if (nidm_file is None) and (first_row):


                # convert nidm stats graph to rdflib
                g2 = Graph()
                g2.parse(source=StringIO(doc.serialize(format='rdf', rdf_format='turtle')), format='turtle')

                if args.add_de is not None:
                    nidmdoc = nidmdoc + g + g2 + pde
                else:
                    nidmdoc = nidmdoc + g2 + pde

                add_seg_data(nidmdoc=nidmdoc, header=provenance, subjid=subjid, fs_stats_entity_id=e.identifier, 
                            acq_filename=args.acq_filename, description=provenance['description'], from_csv=True)

                first_row = False
            # then we can continue to iterate over the rows in the CSV file, adding to the nidmdoc graph
            # then serialize when we're done
            elif (nidm_file is None) and (not first_row):

                # this is not the first row.  We have the first row of data stored in nidmdoc Graph.
                # so we'll just add this new data to the existing graph
                # convert nidm stats graph to rdflib
                g2 = Graph()
                g2.parse(source=StringIO(doc.serialize(format='rdf', rdf_format='turtle')), format='turtle')

                nidmdoc = nidmdoc + g2

                add_seg_data(nidmdoc=nidmdoc, header=provenance, subjid=subjid, fs_stats_entity_id=e.identifier, 
                            acq_filename=args.acq_filename, description=provenance['description'], from_csv=True)

            # we adding these data to an existing NIDM file
            elif (nidm_file is not None) and (first_row):
                # read in NIDM file with rdflib
                g1 = Graph()
                g1.parse(nidm_file, format=util.guess_format(nidm_file))

                # convert nidm stats graph to rdflib
                g2 = Graph()
                g2.parse(source=StringIO(doc.serialize(format='rdf', rdf_format='turtle')), format='turtle')

                if args.add_de is not None:
                    print("Combining graphs...")
                    nidmdoc = g + g1 + g2 + pde
                else:
                    nidmdoc = g1 + g2 + pde

                if args.forcenidm is not False:
                    add_seg_data(nidmdoc=nidmdoc, header=provenance, subjid=subjid, fs_stats_entity_id=e.identifier, 
                                acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents, forceagent=True, description=provenance['description'], from_csv=True)
                else:
                    add_seg_data(nidmdoc=nidmdoc, header=provenance, subjid=subjid, fs_stats_entity_id=e.identifier, 
                                acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents, description=provenance['description'], from_csv=True)

                first_row = False

            elif (nidm_file is not None) and (not first_row):

                # this is not the first row.  We have the first row of data stored in nidmdoc Graph.
                # so we'll just add this new data to the existing graph
                # convert nidm stats graph to rdflib
                g2 = Graph()
                g2.parse(source=StringIO(doc.serialize(format='rdf', rdf_format='turtle')), format='turtle')

                nidmdoc = nidmdoc + g2

                if args.forcenidm is not False:
                    add_seg_data(nidmdoc=nidmdoc, header=provenance, subjid=subjid, fs_stats_entity_id=e.identifier, 
                                acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents, forceagent=True, description=provenance['description'], from_csv=True)
                else:
                    add_seg_data(nidmdoc=nidmdoc, header=provenance, subjid=subjid, fs_stats_entity_id=e.identifier, 
                                acq_filename=args.acq_filename, add_to_nidm=True, created_agents=created_agents, description=provenance['description'], from_csv=True)



        if (nidm_file is None):

            # serialize NIDM file
            print("Writing NIDM file...")
            if args.jsonld is not False:
                # nidmdoc.serialize(destination=join(args.output_dir,output_filename) + '.ttl',format='jsonld')
                nidmdoc.serialize(destination=args.output_dir, format='json-ld', indent=4)
            else:
                # nidmdoc.serialize(destination=join(args.output_dir,output_filename) + '.ttl',format='turtle')
                nidmdoc.serialize(destination=args.output_dir, format='turtle')

            # added to support separate cde serialization
            if args.add_de is None:
                # serialize cde graph
                g.serialize(destination=join(dirname(args.output_dir), "fs_cde.ttl"), format='turtle')

        else:

            # serialize NIDM file
            print("Writing Augmented NIDM file...")
            if args.jsonld is not False:
                nidmdoc.serialize(destination=nidm_file + '.json', format='json-ld')
            else:
                nidmdoc.serialize(destination=nidm_file, format='turtle')

            if args.add_de is None:
                # serialize cde graph
                g.serialize(destination=join(dirname(args.nidm_file), "fs_cde.ttl"), format='turtle')


if __name__ == "__main__":
    main()
