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
from nidm.experiment.Core import Core
from prov.model import QualifiedName,PROV_ROLE, ProvDocument, PROV_ATTR_USED_ENTITY
from prov.model import Namespace as provNamespace

# standard library
from pickle import dumps
from datetime import datetime as dt
import hashlib
import os
from os.path import join,basename,splitext,dirname,realpath
import pwd
from socket import getfqdn
import uuid
import glob

import prov.model as prov
import rdflib
from rdflib import RDFS
import sys
import json



def add_seg_data(nidmdoc, measure, header, json_map, png_file=None, output_file=None, root_act=None, nidm_graph=None):
    '''
    WIP: this function creates a NIDM file of brain volume data and if user supplied a NIDM-E file it will add brain volumes to the
    NIDM-E file for the matching subject ID
    :param nidmdoc:
    :param measure:
    :param header:
    :param json_map:
    :param png_file:
    :param root_act:
    :param nidm_graph:
    :return:
    '''

    niiri=prov.Namespace("niiri","http://iri.nidash.org/")
    #this function can be used for both creating a brainvolumes NIDM file from scratch or adding brain volumes to
    #existing NIDM file.  The following logic basically determines which route to take...

    #if an existing NIDM graph is passed as a parameter then add to existing file
    if nidm_graph is None:
        first_row=True

        #for each of the header items create a dictionary where namespaces are freesurfer
        #software_activity = nidmdoc.graph.activity(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={Constants.NIDM_PROJECT_DESCRIPTION:"Freesurfer segmentation statistics"})
        software_activity = nidmdoc.graph.activity(niiri[getUUID()],other_attributes={Constants.NIDM_PROJECT_DESCRIPTION:"Freesurfer segmentation statistics"})
        for key,value in header.items():
            software_activity.add_attributes({QualifiedName(provNamespace("fs",Constants.FREESURFER),key):value})

        #create software agent and associate with software activity
        #software_agent = nidmdoc.graph.agent(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={
        software_agent = nidmdoc.graph.agent(niiri[getUUID()],other_attributes={
            QualifiedName(provNamespace("Neuroimaging_Analysis_Software",Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE),""):Constants.FREESURFER ,
            prov.PROV_TYPE:prov.PROV["SoftwareAgent"]} )
        #create qualified association with brain volume computation activity
        nidmdoc.graph.association(activity=software_activity,agent=software_agent,other_attributes={PROV_ROLE:Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE})
        nidmdoc.graph.wasAssociatedWith(activity=software_activity,agent=software_agent)

        #print(nidmdoc.serializeTurtle())

        #Debugging...save JSON file of FS stats measures
        #with open('measure.json', 'w') as fp:
        #    json.dump(measure, fp)



        #datum_entity=nidmdoc.graph.entity(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={
        datum_entity=nidmdoc.graph.entity(niiri[getUUID()],other_attributes={
                    prov.PROV_TYPE:QualifiedName(provNamespace("nidm","http://purl.org/nidash/nidm#"),"FSStatsCollection")})
        nidmdoc.graph.wasGeneratedBy(software_activity,datum_entity)

        #iterate over measure dictionary where measures are the lines in the FS stats files which start with '# Measure' and
        #the whole table at the bottom of the FS stats file that starts with '# ColHeaders
        for measures in measure:

            #check if we have a CDE mapping for the anatomical structure referenced in the FS stats file
            if measures["structure"] in json_map['Anatomy']:

                #for the various fields in the FS stats file row starting with '# Measure'...
                for items in measures["items"]:
                    # if the
                    if items['name'] in json_map['Measures'].keys():

                        if not json_map['Anatomy'][measures["structure"]]['label']:
                            continue
                        #region_entity=nidmdoc.graph.entity(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={prov.PROV_TYPE:
                        region_entity=nidmdoc.graph.entity(niiri[getUUID()],other_attributes={prov.PROV_TYPE:
                                QualifiedName(provNamespace("measurement_datum","http://uri.interlex.org/base/ilx_0738269#"),"")
                                })

                        #construct the custom CDEs to describe measurements of the various brain regions
                        region_entity.add_attributes({QualifiedName(provNamespace("isAbout","http://uri.interlex.org/ilx_0381385#"),""):json_map['Anatomy'][measures["structure"]]['isAbout'],
                                    Constants.NIDM_PROJECT_DESCRIPTION:json_map['Anatomy'][measures["structure"]]['definition'],
                                    QualifiedName(provNamespace("isMeasureOf","http://uri.interlex.org/ilx_0381389#"),""):QualifiedName(provNamespace("GrayMatter",
                                    "http://uri.interlex.org/ilx_0104768#"),""),
                                    QualifiedName(provNamespace("rdfs","http://www.w3.org/2000/01/rdf-schema#"),"label"):json_map['Anatomy'][measures["structure"]]['label']})

                        #if there's a laterality designation
                        if 'hasLaterality' in json_map['Anatomy'][measures["structure"]].keys():
                            region_entity.add_attributes({ QualifiedName(provNamespace("hasLaterality","http://uri.interlex.org/ilx_0381387#"),""):json_map['Anatomy'][measures["structure"]]['hasLaterality']})
                            #QualifiedName(provNamespace("hasUnit","http://uri.interlex.org/ilx_0381384#"),""):json_map['Anatomy'][measures["structure"]]['units'],
                            #print("%s:%s" %(key,value))

                        region_entity.add_attributes({QualifiedName(provNamespace("hasMeasurementType","http://uri.interlex.org/ilx_0381388#"),""):
                                json_map['Measures'][items['name']]["measureOf"], QualifiedName(provNamespace("hasDatumType","http://uri.interlex.org/ilx_0738262#"),""):
                                json_map['Measures'][items['name']]["datumType"]})

                        datum_entity.add_attributes({region_entity.identifier:items['value']})






            #key is
            #print(measures)





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
                    fields = ' '.join(fields).replace('CortexVol ', 'CortexVol, ').split()
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



def remap2json(xlsxfile,
               fs_stat_file,
               outfile = None,
               ):
    """
    TODO insightful docstring

    Query ReproNimCDEs and Freesurfer stat files, return json mapping

    xslxfile: path to xslx file with ReproNimCDEs
    typeoffile: one of [segstats, ...]
    outfilename: name for resulting json to be written to
    :return:

    (xslx file to be found (under development):
    https://docs.google.com/spreadsheets/d/1VcpNj1deZ7dF8XM6yXt5VWCNVVQkCnV9Y48wvMFYw0g/edit#gid=1737769619
    example:
    jsonmap = remap2json(xslxfile='ReproNimCDEs.xlsx',
                         fs_stat_file='aseg.stats)

    """
    import io
    import requests
    import json
    import pandas as pd
    import numpy as np
    import xlrd

    # read in the xlxs file
    xls = pd.ExcelFile(xlsxfile)
    mapping = pd.read_excel(xls, 'Subcortical Volumes', header=[0,1])
    corticals = pd.read_excel(xls, 'Cortical Structures', header=[0,1])


    #mapping = pd.read_excel(xslxfile, header=[0,1])
    # rename the URIs so that they resolve, scrape definition
    definition_anat = []
    print("""
        Scraping anatomical definitions from interlex. This might take a few minutes,
        depending on your internet connection.
        """)
    for i, row in mapping.iterrows():
        if row['Federated DE']['URI'] is not np.nan:
            # this fixes the ilx link to resolve to scicrunch
            url = 'ilx_'.join(row['Federated DE']['URI'].split('ILX:')) + '.ttl'
            r = requests.get(url)
            file = io.StringIO(r.text)
            lines = file.readlines()
            for line in lines:
                if 'definition' in line[:14]:
                    definition_anat.append(line.split('"')[1])
        else:
            definition_anat.append('NA')

    definition_cort = []
    for i, row in corticals.iterrows():
        if row['APARC Structures - Assuming Cortical Areas (not sulci)']['Interlex Label'] is not np.nan:
            url = row['APARC Structures - Assuming Cortical Areas (not sulci)']['URI'] + '.ttl'
            r = requests.get(url)
            file = io.StringIO(r.text)
            lines = file.readlines()
            for line in lines:
                if 'definition' in line[:14]:
                    definition_cort.append(line.split('"')[1])
                    break
                elif line == lines[-1]:
                    # some structures do not have definitions yet, append empty strings
                    definition_cort.append("")
        else:
            definition_cort.append('NA')

    mapping['definition'] = definition_anat
    corticals['definition'] = definition_cort
    print("""Done collecting definitions.""")
    d = {}
    print("""creating json mapping from anatomicals...""")
    for i, row in mapping.iterrows():
        # store missing values as empty strings, not NaNs that json can't parse
        label = row['Atlas Segmentation Label'].values[0] if row['Atlas Segmentation Label'].values[0] is not np.nan else ""
        url = row['Structure']['URI'] if row['Structure']['URI'] is not np.nan else ""
        isAbout = row['Structure']['Preferred'] if row['Structure']['Preferred'] is not np.nan else ""
        hasLaterality = row['Laterality']['ILX:0106135'] if row['Laterality']['ILX:0106135'] is not np.nan else ""
        l = row['Federated DE']['Name'] if row['Federated DE']['Name'] is not np.nan else ""
        d[label] = {"url": url,
                    "isAbout": isAbout,
                    "hasLaterality": hasLaterality,
                    "definition": row['definition'][0],
                    "label": l
                    }
    print("Done. Creating json mapping from cortical structures...")
    for i, row in corticals.iterrows():
        label = row['APARC Structures - Assuming Cortical Areas (not sulci)']['Label'] \
            if row['APARC Structures - Assuming Cortical Areas (not sulci)']['Label'] is not np.nan else ""
        url = row['APARC Structures - Assuming Cortical Areas (not sulci)']['URI'] \
            if row['APARC Structures - Assuming Cortical Areas (not sulci)']['URI'] is not np.nan else ""
        isAbout = row['APARC Structures - Assuming Cortical Areas (not sulci)']['Preferred'] \
            if row['APARC Structures - Assuming Cortical Areas (not sulci)']['Preferred']  is not np.nan else ""
        l = row['APARC Structures - Assuming Cortical Areas (not sulci)']['Interlex Label'] \
            if row['APARC Structures - Assuming Cortical Areas (not sulci)']['Interlex Label'] is not np.nan else ""
        d[label] = {"url": url,
                    "isAbout": isAbout,
                    "definition": row['definition'][0],
                    "label": l
                    }
    # read the measures output of a of a read_stats() call. Depending on the header in the file,
    # include present measures in json
    print("Reading in FS stat file...")
    [header, tableinfo, measures] = read_stats(fs_stat_file)
    d2 = {}
    print("""Creating measures json mapping...""")

    for fi, ind1, ind2 in [(mapping, 'Atlas Segmentation Label', 0),
                           (corticals, 'APARC Structures - Assuming Cortical Areas (not sulci)', 'Label')]:
        for i, row in fi.iterrows():
            anatomical = row[ind1][ind2]
            for c in measures:
                if c['structure'] == anatomical:
                    for dic in c['items']:
                        # iterate over the list of dicts in items
                        if dic['name'] == 'normMean':
                            d2['normMean'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738264#'
                                        }
                        if dic['name'] == 'normStdDev':
                            d2['normStdDev'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738265#'
                                        }
                        if dic['name'] == 'normMax':
                            d2['normMax'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738267#'
                                        }
                        if dic['name'] == 'NVoxels':
                            d2['NVoxels'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0102597#'
                                        }
                        if dic['name'] == 'Volume_mm3':
                            d2['Volume_mm3'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0112559#'
                                        }
                        if dic['name'] == 'normMin':
                            d2['normMin'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738266#'
                                         }
                        if dic['name'] == 'normRange':
                            d2['normRange'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0105536#',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738268#'
                                        }
                        if dic['name'] == 'NumVert':
                            d2['NumVert'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0738270#', #vertex
                                        "datumType": 'http://uri.interlex.org/base/ilx_0102597#' # count
                                        }
                        if dic['name'] == 'SurfArea':
                            d2['SurfArea'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0738271#', #surface
                                        "datumType": 'http://uri.interlex.org/base/ilx_0100885#' #area
                                        }
                        if dic['name'] == 'GrayVol':
                            d2['GrayVol'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0104768#', # gray matter
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738276#' #scalar
                                        }
                        if dic['name'] == 'ThickAvg':
                            d2['ThickAvg'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0111689#', #thickness
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738264#' #mean
                                        }
                        if dic['name'] == 'ThickStd':
                            d2['ThickStd'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0111689#', #thickness
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738265#' #stddev
                                        }
                        if dic['name'] == 'MeanCurv':
                            d2['MeanCurv'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0738272#', #mean curvature
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738276#' #scalar
                                        }
                        if dic['name'] == 'GausCurv':
                            d2['GausCurv'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0738273#', #gaussian curvature
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738276#' #scalar
                                        }
                        if dic['name'] == 'FoldInd':
                            d2['FoldInd'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0738274#', #foldind
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738276#' #scalar
                                        }
                        if dic['name'] == 'CurvInd':
                            d2['CurvInd'] = {
                                        "measureOf": 'http://uri.interlex.org/base/ilx_0738275#', #curvind
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738276#' #scalar
                                        }
                        if dic['name'] == 'nuMean':
                            d2['nuMean'] = {
                                        "measureOf": 'TODO',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738264#' #mean
                                        }
                        if dic['name'] == 'nuStdDev':
                            d2['nuStdDev'] = {
                                        "measureOf":'TODO',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738265#' #stddev
                                        }
                        if dic['name'] == 'nuMin':
                            d2['nuMin'] = {
                                        "measureOf":'TODO',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738266#' #min
                                        }
                        if dic['name'] == 'nuMax':
                            d2['nuMax'] = {
                                        "measureOf":'TODO',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738267#' #max
                                        }
                        if dic['name'] == 'nuRange':
                            d2['nuRange'] = {
                                        "measureOf":'TODO',
                                        "datumType": 'http://uri.interlex.org/base/ilx_0738268#' #range
                                        }
    print("Done.")
    # join anatomical and measures dictionaries
    biggie = {'Anatomy': d,
              'Measures': d2}

    if outfile:
        with open(outfile, 'w') as f:
            json.dump(biggie, f, indent=4)

    return [header, tableinfo, measures,biggie]




def main(argv):

    import argparse
    parser = argparse.ArgumentParser(prog='fs_to_nidm.py',
                                     description='''This program will load in a aseg.stats file from Freesurfer
                                        , augment the Freesurfer anatomical region designations with common data element
                                        anatomical designations, and save the statistics + region designations out as
                                        NIDM serializations (i.e. TURTLE, JSON-LD RDF))''')
    parser.add_argument('-s', '--subject_dir', dest='subject_dir', type=str, required=True,
                        help='Path to Freesurfer subject directory')
    parser.add_argument('-jmap', '--json_map', dest='json_map', required=False, default=False,
                        help='Optional JSON mapping file of anatomy region designation to CDE terms (see cde_map dir'
                             'anatomy_cde.json). If provided no scraping of CDEs from InterLex is done.')
    parser.add_argument('-o', '--output_dir', dest='output_dir', type=str, required=True,
                        help='Output directory')
    parser.add_argument('-j', '--jsonld', dest='jsonld', action='store_true', default = False, required=False,
                        help='If parameter added, NIDM file will be written as JSONLD instead of TURTLE')
    parser.add_argument('-pdf','--pdf',dest='pdf',action='store_true',default=False, required=False,
                        help='If parameter added, PDF of NIDM graph will be output')
    #WIP: parser.add_argument('--n','--nidm', dest='nidm_file', type=str, required=False,
    #                    help='Optional NIDM file to add segmentation data to.')

    args = parser.parse_args()

    #if user added -jmap parameter
    if args.json_map is not False:
        #read json map into json_map structure
        with open(args.json_map) as json_file:
            json_map = json.load(json_file)


    #WIP: For right now we're only converting aseg.stats but ultimately we'll want to do this for all stats files
    files=['aseg.stats','lh.aparc.stats','rh.aparc.stats']
    #files=['lh.aparc.stats']
    for stats_file in glob.glob(os.path.join(args.subject_dir,"stats","*.stats")):
        #WIP, currently only mapping stats files in 'files' list above
        if basename(stats_file) in files:
            #if user added -jmap parameter
            if args.json_map is not False:
                #then parse stats file
                [header, tableinfo, measures] = read_stats(stats_file)

            else:
                #live scraping of InterLex for anatomy CDEs
                [header, tableinfo, measures,json_map] = remap2json(xlsxfile=join(dirname(realpath(sys.argv[0])),'mapping_data','ReproNimCDEs.xlsx'),
                                 fs_stat_file=os.path.join(args.subject_dir,"stats",stats_file))

            #for measures we need to create NIDM structures using anatomy mappings
            #If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
            #if args.nidm_file is None:

            print("Creating NIDM file...")

            #create an empty NIDM graph
            nidmdoc = Core()

            #print(nidmdoc.serializeTurtle())

            #WIP: more thought needed for version that works with adding to existing NIDM file versus creating a new NIDM file....
            add_seg_data(nidmdoc=nidmdoc,measure=measures,header=header, json_map=json_map)

            #serialize NIDM file
            if args.jsonld is not False:
                with open(join(args.output_dir,splitext(basename(stats_file))[0]+'.json'),'w') as f:
                    print("Writing NIDM file...")
                    f.write(nidmdoc.serializeJSONLD())
            else:
                with open(join(args.output_dir,splitext(basename(stats_file))[0]+'.ttl'),'w') as f:
                    print("Writing NIDM file...")
                    f.write(nidmdoc.serializeTurtle())

            if args.pdf is not False:
                nidmdoc.save_DotGraph(join(args.output_dir,splitext(basename(stats_file))[0] + ".pdf"), format="pdf")


if __name__ == "__main__":
   main(sys.argv[1:])
