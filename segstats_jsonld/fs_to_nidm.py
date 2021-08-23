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


from nidm.core import Constants
from nidm.experiment.Core import getUUID


# standard library
from pickle import dumps
import os
from os.path import join,basename,splitext,isfile,dirname
from socket import getfqdn
import glob
import pandas as pd

import prov.model as prov
import json
import urllib.request as ur
from urllib.parse import urlparse
from rapidfuzz import fuzz


from rdflib import Graph, RDF, URIRef, util, term,Namespace,Literal,BNode, XSD
from rdflib.serializer import Serializer
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

def map_csv_variables_to_freesurfer_cdes(df):
    '''
    This function will cycle through the column names in the dataframe df and ask the user to match them to
    one of the freesufer CDEs
    :param df: CSV file dataframe
    :param measureOf_df: freesurfer measuresOf
    :param fs_graph: RDFLIB graph of freesurfer CDEs
    :return:
    '''
    
    # step 1: load freesurfer CDEs graph and get dataframe of selected properties
    fs_graph,measureOf_df = get_fs_cdes()

    # step 2: ask which column has subject ID
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
        # query for rdfs:label for measureOf == measurement_type
        query = '''
            prefix fs: <https://surfer.nmr.mgh.harvard.edu/>
            prefix nidm: <http://purl.org/nidash/nidm#>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            prefix xsd: <http://www.w3.org/2001/XMLSchema#>

            select distinct ?uuid ?label
            where {
  
                ?uuid a fs:DataElement ;
                    nidm:measureOf <%s> ;
                    nidm:datumType <http://uri.interlex.org/base/ilx_0738276> ;
                    rdfs:label ?label .
            } order by ?label
        
        ''' % (measurement_type)

        # run query
        qres = fs_graph.query(query)

        # dictionary to store freesurfer labels and UUIDs for measurement type
        freesurfer_region_labels = {}
        # append result as row to result list
        for row in qres:
            freesurfer_region_labels[str(row[1]).lower()] = row[0]

    # WIP: NEED TO ADD ADDITIONAL CODE WHEN TYPES ARE MIXED IN CSV FILE


    # dictionary storing freesurfer region UUID to CSV file variable mappings
    variable_to_freesurfer_cde = {}

    # loop through variables in CSV
    for column in df.columns:
        if column == id_field:
            continue
        else:
            search_term = column
        # now present regions to use based on caseless fuzzy match of labels to variable
        go_loop = True
        while go_loop:
            option = 1
            print()
            print("Freesurfer Measure Association")
            print("Query String: %s " % search_term)

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
            print("%d: Change query string from: \"%s\"" % (option, search_term))
            # Wait for user input
            selection = input("Please select an option (1:%d) from above: \t" % option)

            # Make sure user selected one of the options.  If not present user with selection input again
            while (not selection.isdigit()) or (int(selection) > int(option)):
                # Wait for user input
                selection = input("Please select an option (1:%d) from above: \t" % option)
            # check if selection is to re-run query with new search term
            if int(selection) == (option):
                # ask user for new search string
                search_term = input("Please input new search string for CSV column: %s \t:" % column)
                print("---------------------------------------------------------------------------------------")
            else:
                # user selected a freesurfer term for this region
                # store FS CDE UUID and CSV file variable mapping
                variable_to_freesurfer_cde[column] = match_scores[options[selection]]['uuid']
                print("\nUser selection: %s, Freesurfer CDE UUID: %s" %(options[selection],
                                match_scores[options[selection]]['uuid']))
                go_loop = False

    # now ask user to supply some provenance information
    provenance={}
    print("\n Now you will be asked some questions intended to capture some provenance about the freesufer"
          "data you are storing in NIDM.  Ideally you should run this tool using the Freesurfer subject's "
          "directory or the aparc+aseg.mgz file.  Then provenance would be captures automatically for you.\n\n")
    provenance['description'] = input("Please input a textual description of how these data were created: \t")
    provenance['version'] = input("Please input the Freesufer version number used: \t")
    provenance['os'] = input("Please input operating system used: \t")

    # store variable_to_freesufer_cde mappings for later use
    

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

def add_seg_data(nidmdoc,header,subjid,fs_stats_entity_id, add_to_nidm=False, forceagent=False):
    '''
    WIP: this function creates a NIDM file of brain volume data and if user supplied a NIDM-E file it will add brain volumes to the
    NIDM-E file for the matching subject ID
    :param nidmdoc:
    :param header:
    :param add_to_nidm:
    :return:
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


    software_activity = niiri[getUUID()]
    nidmdoc.add((software_activity,RDF.type,Constants.PROV['Activity']))
    nidmdoc.add((software_activity,Constants.DCT["description"],Literal("Freesurfer segmentation statistics")))
    fs = Namespace(Constants.FREESURFER)

    for key,value in header.items():
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
        nidmdoc.add((participant_agent,URIRef(Constants.NIDM_SUBJECTID.uri),Literal(subjid, datatype=XSD.string)))

    else:
        # query to get agent id for subjid
        #find subject ids and sessions in NIDM document
            query = """
                    PREFIX ndar:<https://ndar.nih.gov/api/datadictionary/v2/dataelement/>
                    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX prov:<http://www.w3.org/ns/prov#>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                    select distinct ?agent
                    where {

                        ?agent rdf:type prov:Agent ;
                        ndar:src_subject_id \"%s\"^^xsd:string .

                    }""" % subjid
            #print(query)
            qres = nidmdoc.query(query)
            if len(qres) == 0:
                print('Subject ID (%s) was not found in existing NIDM file...' %subjid)
                ##############################################################################
                # added to account for issues with some BIDS datasets that have leading 00's in subject directories
                # but not in participants.tsv files.
                if (len(subjid) - len(subjid.lstrip('0'))) != 0:
                    print('Trying to find subject ID without leading zeros....')
                    query = """
                        PREFIX ndar:<https://ndar.nih.gov/api/datadictionary/v2/dataelement/>
                        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX prov:<http://www.w3.org/ns/prov#>
                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                        select distinct ?agent
                        where {

                            ?agent rdf:type prov:Agent ;
                            ndar:src_subject_id \"%s\"^^xsd:string .

                        }""" % subjid.lstrip('0')
                    #print(query)
                    qres2 = nidmdoc.query(query)
                    if len(qres2) == 0:
                        print("Still can't find subject id after stripping leading zeros...")
                    else:
                        for row in qres2:
                            print('Found subject ID after stripping zeros: %s in NIDM file (agent: %s)' %(subjid.lstrip('0'),row[0]))
                            participant_agent = row[0]
                #######################################################################################
                if (forceagent is not False) and (len(qres2)==0):
                    print('Explicitly creating agent in existing NIDM file...')
                    participant_agent = niiri[getUUID()]
                    nidmdoc.add((participant_agent,RDF.type,Constants.PROV['Agent']))
                    nidmdoc.add((participant_agent,URIRef(Constants.NIDM_SUBJECTID.uri),Literal(subjid, datatype=XSD.string)))
                elif (forceagent is False) and (len(qres)==0) and (len(qres2)==0):
                    print('Not explicitly adding agent to NIDM file, no output written')
                    exit()
            else:
                 for row in qres:
                    print('Found subject ID: %s in NIDM file (agent: %s)' %(subjid,row[0]))
                    participant_agent = row[0]

    #create a blank node and qualified association with prov:Agent for participant
    association_bnode = BNode()
    nidmdoc.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
    nidmdoc.add((association_bnode,RDF.type,Constants.PROV['Association']))
    nidmdoc.add((association_bnode,Constants.PROV['hadRole'],Constants.SIO["Subject"]))
    nidmdoc.add((association_bnode,Constants.PROV['agent'],participant_agent))

    # add association between FSStatsCollection and computation activity
    nidmdoc.add((URIRef(fs_stats_entity_id.uri),Constants.PROV['wasGeneratedBy'],software_activity))

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
        nidmdoc.add((software_activity, Constants.DCT["isPartOf"], row['project']))


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
                                     description='''This program will load in a aseg.stats file from Freesurfer
                                        , augment the Freesurfer anatomical region designations with common data element
                                        anatomical designations, and save the statistics + region designations out as
                                        NIDM serializations (i.e. TURTLE, JSON-LD RDF))''',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    #DBK: added mutually exclusive arguments to support pulling a named stats file (e.g. aseg.stats) as a URL such as
    #data hosted in an amazon bucket or from a mounted filesystem where you don't have access to the original
    #subjects directory.

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-s', '--subject_dir', dest='subject_dir', type=str,
                        help='Path to Freesurfer subject directory')
    group.add_argument('-f', '--seg_file', dest='segfile', type=str,help='Path or URL to a specific Freesurfer'
                            'stats file. Note, currently supported is aseg.stats, lh/rh.aparc.stats')
    group.add_argument('-csv', '--csv_file', dest='csvfile', type=str, help='Path to CSV file which includes '
                            'a header row with 1 column containing subject IDs and the other columns are variables'
                            'indicating the Freesurfer-derived region measure (e.g. volume, surface area, etc.  If '
                            'you use this mode of running this software you will be asked to match your region '
                            'variables to standard Freesurfer region / measure labels presented to you (so this '
                            'program will end up being interactive.  In future WIP we will add the ability to export'
                            'a json sidecar file with those mappings for automated runs of future CSV files with the '
                            'same set of variables.')
    parser.add_argument('-subjid','--subjid',dest='subjid',required=False, help='If a path to a URL or a stats file'
                            'is supplied via the -f/--seg_file parameters then -subjid parameter must be set with'
                            'the subject identifier to be used in the NIDM files')
    parser.add_argument('-o', '--output', dest='output_dir', type=str,
                        help='Output filename with full path', required=True)
    parser.add_argument('-j', '--jsonld', dest='jsonld', action='store_true', default = False,
                        help='If flag set then NIDM file will be written as JSONLD instead of TURTLE')
    parser.add_argument('-add_de', '--add_de', dest='add_de', action='store_true', default = None,
                        help='If flag set then data element data dictionary will be added to nidm file else it will written to a'
                            'separate file as fsl_cde.ttl in the output directory (or same directory as nidm file if -n paramemter'
                            'is used.')
    parser.add_argument('-n','--nidm', dest='nidm_file', type=str, required=False,
                        help='Optional NIDM file to add segmentation data to.')
    parser.add_argument('-forcenidm','--forcenidm', action='store_true',required=False,
                        help='If adding to NIDM file this parameter forces the data to be added even if the participant'
                             'doesnt currently exist in the NIDM file.')
    args = parser.parse_args()



    # test whether user supplied stats file directly and if so they the subject id must also be supplied so we
    # know which subject the stats file is for
    if args.segfile and (args.subjid is None):
        parser.error("-f/--seg_file requires -subjid/--subjid to be set!")

    # if output_dir doesn't exist then create it
    out_path = os.path.dirname(args.output_dir)
    if not os.path.exists(out_path):
        os.makedirs(out_path)

    # WIP: trying to find a way to reference data in module. This does not feel kosher but works
    #datapath = mapping_data.__path__._path[0] + '/'
    # changed by DBK
    datapath = mapping_data.__path__[0] + '/'
    # WIP: For right now we're only converting aseg.stats but ultimately we'll want to do this for all stats files
    supported_files=['aseg.stats','lh.aparc.stats','rh.aparc.stats']

    # if we set -s or --subject_dir as parameter on command line...
    if args.subject_dir is not None:
        # get the freesurfer version for later use
        freesurfer_version = read_buildstamp(args.subject_dir)
        # files=['aseg.stats']
        # get subject id from args.subject_dir
        subjid = os.path.basename(args.subject_dir)
        for stats_file in glob.glob(os.path.join(args.subject_dir,"stats","*.stats")):
            if basename(stats_file) in supported_files:
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
                    add_seg_data(nidmdoc=nidmdoc,header=header,subjid=subjid,fs_stats_entity_id=e.identifier)

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
                        add_seg_data(nidmdoc=nidmdoc,header=header,subjid=subjid,fs_stats_entity_id=e.identifier,add_to_nidm=True, forceagent=True)
                    else:
                        add_seg_data(nidmdoc=nidmdoc,header=header,subjid=subjid,fs_stats_entity_id=e.identifier,add_to_nidm=True)


                    #serialize NIDM file
                    print("Writing Augmented NIDM file...")
                    if args.jsonld is not False:
                        nidmdoc.serialize(destination=args.nidm_file + '.json',format='jsonld')
                    else:
                        nidmdoc.serialize(destination=args.nidm_file,format='turtle')

                    if args.add_de is None:
                        # serialize cde graph
                        g.serialize(destination=join(dirname(args.output_dir),"fs_cde.ttl"),format='turtle')

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

            add_seg_data(nidmdoc=nidmdoc,header=header,subjid=args.subjid,fs_stats_entity_id=e.identifier)


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
                add_seg_data(nidmdoc=nidmdoc,header=header,subjid=args.subjid,fs_stats_entity_id=e.identifier,add_to_nidm=True, forceagent=True)
            else:
                add_seg_data(nidmdoc=nidmdoc,header=header,subjid=args.subjid,fs_stats_entity_id=e.identifier,add_to_nidm=True)


            #serialize NIDM file
            print("Writing Augmented NIDM file...")
            if args.jsonld is not False:
                nidmdoc.serialize(destination=args.nidm_file + '.json',format='jsonld')
            else:
                nidmdoc.serialize(destination=args.nidm_file,format='turtle')

            if args.add_de is None:
                # serialize cde graph
                g.serialize(destination=join(dirname(args.output_dir),"fs_cde.ttl"),format='turtle')
    elif args.csvfile is not None:
        # added to support CSV files with volume data from freesurfer
        # problem is the variable names aren't guaranteed to match freesurfer names so we have to do some
        # interactions with the user...
        
        # step 1: read CSV file and get header row
        df = pd.read_csv(args.csvfile)
        


        # step 2: interact with the user about the variables in this CSV file.

        var_to_freesurfer_cde,provenance = map_csv_variables_to_freesurfer_cdes(df)

        # step 3: create the Freesurfer CDE graph

        g = create_cde_graph()

        # step 4: now we need to cycle through each row of the CSV file and store the data in graph
        for index, row in df.iterrows():
            # here we need to create the nidm statements to store the volume measures, the PDE entities
            # to stores the mappings between the variables and the freesurfer CDEs and the activity provenance
            # information.

            [e, doc] = convert_csv_stats_to_nidm(row,var_to_freesurfer_cde)


if __name__ == "__main__":
    main()
