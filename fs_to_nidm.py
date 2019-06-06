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
from nidm.experiment.Utils import read_nidm, map_variables_to_terms, getSubjIDColumn
from nidm.experiment.Core import getUUID
from nidm.experiment.Core import Core
from prov.model import QualifiedName,PROV_ROLE, ProvDocument, PROV_ATTR_USED_ENTITY
from prov.model import Namespace as provNamespace

# standard library
from pickle import dumps
from datetime import datetime as dt
import hashlib
import os
import pwd
from socket import getfqdn
import uuid

import prov.model as prov
import rdflib
import sys


# create namespace references to terms used
foaf = prov.Namespace("foaf", "http://xmlns.com/foaf/0.1/")
dcterms = prov.Namespace("dcterms", "http://purl.org/dc/terms/")
fs = prov.Namespace("fs", "http://www.incf.org/ns/nidash/fs#")
nidm = prov.Namespace("nidm", "http://www.incf.org/ns/nidash/nidm#")
niiri = prov.Namespace("niiri", "http://iri.nidash.org/")
obo = prov.Namespace("obo", "http://purl.obolibrary.org/obo/")
nif = prov.Namespace("nif", "http://neurolex.org/wiki/")
crypto = prov.Namespace("crypto", "http://www.w3.org/2000/10/swap/crypto#")
crypto = prov.Namespace("crypto",
                        ("http://id.loc.gov/vocabulary/preservation/"
                         "cryptographicHashFunctions/"))



def add_seg_data(nidmdoc, measure, header, tableinfo, json_map,   png_file=None, output_file=None, root_act=None, nidm_graph=None):
    '''
    WIP: this function creates a NIDM file of brain volume data and if user supplied a NIDM-E file it will add
    :param nidmdoc:
    :param measure:
    :param json_map:
    :param png_file:
    :param root_act:
    :param nidm_graph:
    :return:
    '''

    #read in json_map




    #dictionary to store activities for each software agent
    software_agent={}
    software_activity={}
    participant_agent={}
    entity={}

    #this function can be used for both creating a brainvolumes NIDM file from scratch or adding brain volumes to
    #existing NIDM file.  The following logic basically determines which route to take...

    #if an existing NIDM graph is passed as a parameter then add to existing file
    if nidm_graph is None:
        first_row=True
        #iterate over measure dictionary
        for measures in measure:

            #key is
            print(measures)

            #store other data from row with columns_to_term mappings
            for row_variable,row_data in csv_row.iteritems():

                #check if row_variable is subject id, if so check whether we have an agent for this participant
                if row_variable==id_field:
                    #store participant id for later use in processing the data for this row
                    participant_id = row_data
                    #if there is no agent for the participant then add one
                    if row_data not in participant_agent.keys():
                        #add an agent for this person
                        participant_agent[row_data] = nidmdoc.graph.agent(QualifiedName(provNamespace("nidm",Constants.NIDM),getUUID()),other_attributes=({Constants.NIDM_SUBJECTID:row_data}))
                    continue
                else:

                    #get source software matching this column deal with duplicate variables in source_row and pandas changing duplicate names
                    software_key = source_row.columns[[column_index(df,row_variable)]]._values[0].split(".")[0]

                    #see if we already have a software_activity for this agent
                    if software_key not in software_activity.keys():

                        #create an activity for the computation...simply a placeholder for more extensive provenance
                        software_activity[software_key] = nidmdoc.graph.activity(QualifiedName(provNamespace("nidm",Constants.NIDM),getUUID()),other_attributes={Constants.NIDM_PROJECT_DESCRIPTION:"brain volume computation"})

                        if root_act is not None:
                            #associate activity with activity of brain volumes creation (root-level activity)
                            software_activity[software_key].add_attributes({QualifiedName(provNamespace("dct",Constants.DCT),'isPartOf'):root_act})

                        #associate this activity with the participant
                        nidmdoc.graph.association(activity=software_activity[software_key],agent=participant_agent[participant_id],other_attributes={PROV_ROLE:Constants.NIDM_PARTICIPANT})
                        nidmdoc.graph.wasAssociatedWith(activity=software_activity[software_key],agent=participant_agent[participant_id])

                        #check if there's an associated software agent and if not, create one
                        if software_key not in software_agent.keys():
                            #create an agent
                            software_agent[software_key] = nidmdoc.graph.agent(QualifiedName(provNamespace("nidm",Constants.NIDM),getUUID()),other_attributes={'prov:type':QualifiedName(provNamespace(Core.safe_string(None,string=str("Neuroimaging Analysis Software")),Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE),""),
                                                                    QualifiedName(provNamespace(Core.safe_string(None,string=str("Neuroimaging Analysis Software")),Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE),""):software_key } )
                            #create qualified association with brain volume computation activity
                            nidmdoc.graph.association(activity=software_activity[software_key],agent=software_agent[software_key],other_attributes={PROV_ROLE:QualifiedName(provNamespace(Core.safe_string(None,string=str("Neuroimaging Analysis Software")),Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE),"")})
                            nidmdoc.graph.wasAssociatedWith(activity=software_activity[software_key],agent=software_agent[software_key])

                    #check if we have an entity for storing this particular variable for this subject and software else create one
                    if software_activity[software_key].identifier.localpart + participant_agent[participant_id].identifier.localpart not in entity.keys():
                        #create an entity to store brain volume data for this participant
                        entity[software_activity[software_key].identifier.localpart + participant_agent[participant_id].identifier.localpart] = nidmdoc.graph.entity( QualifiedName(provNamespace("nidm",Constants.NIDM),getUUID()))
                        #add wasGeneratedBy association to activity
                        nidmdoc.graph.wasGeneratedBy(entity=entity[software_activity[software_key].identifier.localpart + participant_agent[participant_id].identifier.localpart], activity=software_activity[software_key])

                    #get column_to_term mapping uri and add as namespace in NIDM document
                    entity[software_activity[software_key].identifier.localpart + participant_agent[participant_id].identifier.localpart].add_attributes({QualifiedName(provNamespace(Core.safe_string(None,string=str(row_variable)), column_to_terms[row_variable.split(".")[0]]["url"]),""):row_data})
                    #print(project.serializeTurtle())


            #just for debugging.  resulting graph is too big right now for DOT graph creation so here I'm simply creating
            #a DOT graph for the processing of 1 row of the brain volumes CSV file so we can at least visually see the
            #model
            if png_file is not None:
                if first_row:
                    #serialize NIDM file
                    #with open(args.output_file,'w') as f:
                    #   print("Writing NIDM file...")
                    #   f.write(nidmdoc.serializeTurtle())
                    if png_file:
                        nidmdoc.save_DotGraph(str(output_file + ".pdf"), format="pdf")
                    first_row=False


max_text_len = 1024000

def safe_encode(x, as_literal=True):
    """Encodes a python value for prov
"""
    if x is None:
        value = "Unknown"
        if as_literal:
            return prov.Literal(value, prov.XSD['string'])
        else:
            return value
    try:
        if isinstance(x, (str, unicode)):
            if os.path.exists(x):
                value = 'file://%s%s' % (getfqdn(), x)
                if not as_literal:
                    return value
                try:
                    return prov.URIRef(value)
                except AttributeError:
                    return prov.Literal(value, prov.XSD['anyURI'])
            else:
                if len(x) > max_text_len:
                    value = x[:max_text_len - 13] + ['...Clipped...']
                else:
                    value = x
                if not as_literal:
                    return value
                return prov.Literal(value, prov.XSD['string'])
        if isinstance(x, (int,)):
            if not as_literal:
                return x
            return prov.Literal(int(x), prov.XSD['integer'])
        if isinstance(x, (float,)):
            if not as_literal:
                return x
            return prov.Literal(x, prov.XSD['float'])
        if not as_literal:
            return dumps(x)
        return prov.Literal(dumps(x), nidm['pickle'])
    except TypeError as e:
        value = "Could not encode: " + str(e)
        if not as_literal:
            return value
        return prov.Literal(value, prov.XSD['string'])


def read_stats(filename):
    """Convert stats file to a structure
"""
    header = {}
    tableinfo = {}
    measures = []
    rowmeasures = []
    with open(filename, 'rt') as fp:
        lines = fp.readlines()
        for line in lines:
            if line == line[0]:
                continue
            #parse commented header
            if line.startswith('#'):
                fields = line.split()[1:]
                if len(fields) < 2:
                    continue
                tag = fields[0]
                if tag == 'TableCol':
                    col_idx = int(fields[1])
                    if col_idx not in tableinfo:
                        tableinfo[col_idx] = {}
                    tableinfo[col_idx][fields[2]] = ' '.join(fields[3:])
                    if tableinfo[col_idx][fields[2]] == "StructName":
                        struct_idx = col_idx
                elif tag == "Measure":
                    fields = ' '.join(fields[1:]).split(', ')
                    measures.append({'structure': fields[0],
                                     'name': fields[1],
                                     'description': fields[2],
                                     'value': fields[3],
                                     'units': fields[4],
                                     'source': 'Header'})
                elif tag == "ColHeaders":
                    if len(fields) != len(tableinfo):
                        for idx, fieldname in enumerate(fields[1:]):
                            if idx + 1 in tableinfo:
                                continue
                            tableinfo[idx + 1] = {'ColHeader': fieldname,
                                                  'Units': 'unknown',
                                                  'FieldName': fieldname}
                    else:
                        continue
                else:
                    header[tag] = ' '.join(fields[1:])
            else:
                #read values
                row = line.split()
                values = {}
                measures.append({'structure': row[struct_idx-1],
                                 'items': [],
                                 'source': 'Table'}),
                for idx, value in enumerate(row):
                    if idx + 1 == struct_idx:
                        continue
                    measures[-1]['items'].append({
                        'name': tableinfo[idx + 1]['ColHeader'],
                        'description': tableinfo[idx + 1]['FieldName'],
                        'value': value,
                        'units': tableinfo[idx + 1]['Units'],
                        })
    return header, tableinfo, measures

def parse_stats(g, fs_stat_file, entity_uri):
    """Convert stats file to a nidm object
"""

    header, tableinfo, measures = read_stats(fs_stat_file)

    get_id = lambda : niiri[uuid.uuid1().hex]
    a0 = g.activity(get_id(), startTime=dt.isoformat(dt.utcnow()))
    user_agent = g.agent(get_id(),
                         {prov.PROV["type"]: prov.PROV["Person"],
                          prov.PROV["label"]: pwd.getpwuid(os.geteuid()).pw_name,
                          foaf["name"]: pwd.getpwuid(os.geteuid()).pw_name})
    g.wasAssociatedWith(a0, user_agent, None, None,
                        {prov.PROV["Role"]: "LoggedInUser"})
    stat_collection = g.collection(get_id())
    stat_collection.add_extra_attributes({prov.PROV['type']: fs['FreeSurferStatsCollection']})
    # header elements
    statheader_collection = g.entity(get_id())
    attributes = {prov.PROV['type']: fs['StatFileHeader']}
    for key, value in header.items():
        attributes[fs[key.replace('.c', '-c')]] = value
    statheader_collection.add_extra_attributes(attributes)
    # measures
    struct_info = {}
    measure_list = []
    measure_graph = rdflib.ConjunctiveGraph()
    measure_graph.namespace_manager.bind('fs', fs.get_uri())
    measure_graph.namespace_manager.bind('nidm', nidm.get_uri())
    unknown_units = set(('unitless', 'NA'))
    for measure in measures:
        obj_attr = []
        struct_uri = fs[measure['structure'].replace('.', '-')]
        if measure['source'] == 'Header':
            measure_name = measure['name']
            if measure_name not in measure_list:
                measure_list.append(measure_name)
                measure_uri = fs[measure_name].rdf_representation()
                measure_graph.add((measure_uri,
                                   rdflib.RDF['type'],
                                   fs['Measure'].rdf_representation()))
                measure_graph.add((measure_uri,
                                   rdflib.RDFS['label'],
                                   rdflib.Literal(measure['description'])))
                measure_graph.add((measure_uri,
                                   nidm['unitsLabel'].rdf_representation(),
                                   rdflib.Literal(measure['units'])))
            obj_attr.append((nidm["anatomicalAnnotation"], struct_uri))
            if str(measure['units']) in unknown_units and \
                    '.' not in measure['value']:
                valref = prov.Literal(int(measure['value']), prov.XSD['integer'])
            else:
                valref= prov.Literal(float(measure['value']), prov.XSD['float'])
            obj_attr.append((fs[measure_name], valref))
        elif measure['source'] == 'Table':
            obj_attr.append((nidm["anatomicalAnnotation"], struct_uri))
            for column_info in measure['items']:
                measure_name = column_info['name']
                if column_info['units'] in unknown_units and \
                   '.' not in column_info['value']:
                    valref = prov.Literal(int(column_info['value']),
                                          prov.XSD['integer'])
                else:
                    valref= prov.Literal(float(column_info['value']),
                                         prov.XSD['float'])
                obj_attr.append((fs[measure_name], valref))
                if measure_name not in measure_list:
                    measure_list.append(measure_name)
                    measure_uri = fs[measure_name].rdf_representation()
                    measure_graph.add((measure_uri,
                                       rdflib.RDF['type'],
                                       fs['Measure'].rdf_representation()))
                    measure_graph.add((measure_uri,
                                       rdflib.RDFS['label'],
                                       rdflib.Literal(column_info['description'])))
                    measure_graph.add((measure_uri,
                                       nidm['unitsLabel'].rdf_representation(),
                                       rdflib.Literal(column_info['units'])))
        id = get_id()
        if struct_uri in struct_info:
            euri = struct_info[struct_uri]
            euri.add_extra_attributes(obj_attr)
        else:
            euri = g.entity(id, obj_attr)
            struct_info[struct_uri] = euri
        g.hadMember(stat_collection, id)
    g.hadMember(stat_collection, statheader_collection)
    g.derivation(stat_collection, entity_uri)
    g.wasGeneratedBy(stat_collection, a0)
    return g, measure_graph




def main(argv):

    import argparse
    parser = argparse.ArgumentParser(prog='fs_to_nidm.py',
                                     description='''This program will load in a aseg.stats file from Freesurfer
                                        , augment the Freesurfer anatomical region designations with common data element
                                        anatomical designations, and save the statistics + region designations out as
                                        NIDM serializations (i.e. TURTLE, JSON-LD RDF))''')
    parser.add_argument('-s', '--subject_dir', dest='subject_dir', type=str, required=True,
                        help='Path to Freesurfer subject directory')
    parser.add_argument('-j','--json_map', dest='json_file',type=str, required=True,
                        help='JSON mapping file which maps Freesurfer aseg anatomy terms to commond data elements')
    parser.add_argument('-o', '--output_dir', dest='output_file', type=str,
                        help='Output directory')
    parser.add_argument('--n','--nidm', dest='nidm_file', type=str, required=False,
                        help='Optional NIDM file to add segmentation data to.')

    args = parser.parse_args()


    [header, tableinfo, measures] = read_stats(os.path.join(args.subject_dir,"stats","aseg.stats"))

    #for measures we need to create NIDM structures using anatomy mappings
    #If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
    if args.nidm_file is None:

        print("Creating NIDM file...")
        #If user did not choose to add this data to an existing NIDM file then create a new one for the CSV data

        #create an empty NIDM graph
        nidmdoc = Core()
        root_act = nidmdoc.graph.activity(QualifiedName(provNamespace("nidm",Constants.NIDM),getUUID()),other_attributes={Constants.NIDM_PROJECT_DESCRIPTION:"Freesurfer segmentation statistics"})

        #this function sucks...more thought needed for version that works with adding to existing NIDM file versus creating a new NIDM file....
        add_seg_data(nidmdoc=nidmdoc,measure=measures,header=header, tableinfo=tableinfo, json_map=args.json_file)



        #serialize NIDM file
        with open(args.output_file,'w') as f:
            print("Writing NIDM file...")
            f.write(nidmdoc.serializeJSONLD())
            nidmdoc.save_DotGraph(str(args.output_file + ".pdf"), format="pdf")


if __name__ == "__main__":
   main(sys.argv[1:])
