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

import prov.model as prov
import json
import urllib.request as ur
from urllib.parse import urlparse

from rdflib import Graph, RDF, URIRef, util, term,Namespace,Literal,BNode, XSD
from rdflib.serializer import Serializer
from segstats_jsonld.fsutils import read_stats, convert_stats_to_nidm, create_cde_graph

from io import StringIO

import tempfile


from segstats_jsonld import mapping_data


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

                    # convert nidm stats graph to rdflib
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
                        g.serialize(destination=join(dirname(args.output_dir),"fs_cde.ttl"),format='turtle')

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



if __name__ == "__main__":
    main()
