#!/usr/bin/env python
"""Upload a FreeSurfer directory structure as RDF to a SPARQL triplestore
"""

# standard library
from cPickle import dumps
from datetime import datetime as dt
import hashlib
import os
import pwd
from socket import getfqdn
import uuid

import prov.model as prov
import rdflib


def hash_infile(afile, crypto=hashlib.md5, chunk_len=8192):
    """ Computes hash of a file using 'crypto' module"""
    hex = None
    if os.path.isfile(afile):
        crypto_obj = crypto()
        fp = file(afile, 'rb')
        while True:
            data = fp.read(chunk_len)
            if not data:
                break
            crypto_obj.update(data)
        fp.close()
        hex = crypto_obj.hexdigest()
    return hex

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

# map FreeSurfer filename parts
fs_file_map = [('T1', [nif["nlx_inv_20090243"]]), # 3d T1 weighted scan
               ('lh', [(nidm["anatomicalAnnotation"], obo["UBERON_0002812"])]), # left cerebral hemisphere
               ('rh', [(nidm["anatomicalAnnotation"], obo["UBERON_0002813"])]), # right cerebral hemisphere
               ('BA.', [(nidm["anatomicalAnnotation"], obo["UBERON_0013529"])]), # Brodmann area
               ('BA1.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006099"])]), # Brodmann area 1
               ('BA2.', [(nidm["anatomicalAnnotation"], obo["UBERON_0013533"])]), # Brodmann area 2
               ('BA3a.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006100"]), # Brodmann area 3a
                          (nidm["anatomicalAnnotation"], obo["FMA_74532"])]), # anterior
               ('BA3b.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006100"]), # Brodmann area 3b
                          (nidm["anatomicalAnnotation"], obo["FMA_74533"])]), # posterior
               ('BA44.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006481"])]), # Brodmann area 44
               ('BA45.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006482"])]), # Brodmann area 45
               ('BA4a.', [(nidm["anatomicalAnnotation"], obo["UBERON_0013535"]), # Brodmann area 4a
                          (nidm["anatomicalAnnotation"], obo["FMA_74532"])]), # anterior
               ('BA4p.', [(nidm["anatomicalAnnotation"], obo["UBERON_0013535"]), # Brodmann area 4p
                          (nidm["anatomicalAnnotation"], obo["FMA_74533"])]), # posterior
               ('BA6.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006472"])]), # Brodmann area 6
               ('V1.', [(nidm["anatomicalAnnotation"], obo["UBERON_0002436"])]),
               ('V2.', [(nidm["anatomicalAnnotation"], obo["UBERON_0006473"])]),
               ('MT', [(nidm["anatomicalAnnotation"], fs["MT_area"])]),
               ('entorhinal', [(nidm["snatomicalAnnotation"], obo["UBERON_0002728"])]),
               ('exvivo', [(nidm["snnotationSource"], fs["Exvivo"])]),
               ('label', [(fs["fileType"], fs["LabelFile"])]),
               ('annot', [(fs["fileType"], fs["AnnotationFile"])]),
               ('cortex', [(nidm["anatomicalAnnotation"], obo["UBERON_0000956"])]),
               ('.stats', [(fs["fileType"], fs["StatisticFile"])]),
               ('aparc.annot', [(nidm["atlasName"], fs["DefaultParcellation"])]),
               ('aparc.a2009s', [(nidm["atlasName"], fs["A2009sParcellation"])]),
               ('.ctab', [(fs["fileType"], fs["ColorTable"])])
               ]

# files or directories that should be ignored
ignore_list = ['bak', 'src', 'tmp', 'trash', 'touch']

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

def create_entity(graph, fs_subject_id, filepath, hostname):
    """ Create a PROV entity for a file in a FreeSurfer directory
"""
    # identify FreeSurfer terms based on directory and file names
    _, filename = os.path.split(filepath)
    relpath = filepath.split(fs_subject_id)[1].lstrip(os.path.sep)
    fstypes = relpath.split('/')[:-1]
    additional_types = relpath.split('/')[-1].split('.')

    file_md5_hash = hash_infile(filepath, crypto=hashlib.md5)
    file_sha512_hash = hash_infile(filepath, crypto=hashlib.sha512)
    if file_md5_hash is None:
        print('Empty file: %s' % filepath)

    url = "file://%s%s" % (hostname, filepath)
    obj_attr = [(prov.PROV["label"], filename),
                (fs["relative_path"], "%s" % relpath),
                (prov.PROV["location"], prov.URIRef(url)),
                (crypto["md5"], "%s" % file_md5_hash),
                (crypto["sha512"], "%s" % file_sha512_hash)
                ]

    for key in fstypes:
        obj_attr.append((nidm["tag"], key))
    for key in additional_types:
        obj_attr.append((nidm["tag"], key))

    for key, uris in fs_file_map:
        if key in filename:
            if key.rstrip('.').lstrip('.') not in fstypes + additional_types:
                obj_attr.append((nidm["tag"], key.rstrip('.').lstrip('.')))
            for uri in uris:
                if isinstance(uri, tuple):
                    obj_attr.append((uri[0], uri[1]))
                else:
                    obj_attr.append((prov.PROV["type"], uri))
    id = uuid.uuid1().hex
    return graph.entity(niiri[id], obj_attr)


def encode_fs_directory(g, basedir, project_id, subject_id, n_items=100000):
    """ Convert a FreeSurfer directory to a PROV graph
"""
    # directory collection/catalog
    collection_hash = uuid.uuid1().hex
    fsdir_collection = g.collection(niiri[collection_hash])
    fsdir_collection.add_extra_attributes({prov.PROV['type']: fs['SubjectDirectory'],
                                           nidm['tag']: project_id,
                                           fs['subjectID']: subject_id})
    directory_id = g.entity(niiri[uuid.uuid1().hex])
    hostname = getfqdn()
    url = "file://%s%s" % (hostname, os.path.abspath(basedir))
    directory_id.add_extra_attributes({prov.PROV['location']: prov.URIRef(url)})
    g.wasDerivedFrom(fsdir_collection, directory_id)

    a0 = g.activity(niiri[uuid.uuid1().hex], startTime=dt.isoformat(dt.utcnow()))
    user_agent = g.agent(niiri[uuid.uuid1().hex],
                         {prov.PROV["type"]: prov.PROV["Person"],
                          prov.PROV["label"]: pwd.getpwuid(os.geteuid()).pw_name,
                          foaf["name"]: pwd.getpwuid(os.geteuid()).pw_name})
    g.wasAssociatedWith(a0, user_agent, None, None,
                        {prov.PROV["Role"]: "LoggedInUser"})
    g.wasGeneratedBy(fsdir_collection, a0)

    i = 0
    for dirpath, dirnames, filenames in os.walk(os.path.realpath(basedir)):
        for filename in sorted(filenames):
            if filename.startswith('.'):
                continue
            i += 1
            if i > n_items:
                break
            file2encode = os.path.realpath(os.path.join(dirpath, filename))
            if not os.path.isfile(file2encode):
                print "%s not a file" % file2encode
                continue
            ignore_key_found = False
            for key in ignore_list:
                if key in file2encode:
                    ignore_key_found = True
                    continue
            if ignore_key_found:
                continue
            try:
                entity = create_entity(g, subject_id, file2encode, hostname)
                g.hadMember(fsdir_collection, entity.get_identifier())
                rdf_g = entity.rdf().serialize(format='turtle')
                '''
query = """
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX fs: <http://www.incf.org/ns/nidash/fs#>
PREFIX crypto: <http://www.w3.org/2000/10/swap/crypto#>
PREFIX nidm: <http://www.incf.org/ns/nidash/nidm#>
select ?e ?relpath ?path where
{?e fs:fileType fs:StatisticFile;
fs:relativePath ?relpath;
prov:atLocation ?path .
FILTER NOT EXISTS {
?e nidm:tag "curv" .
}
}
"""
results = rdf_g.query(query)
'''
                if 'StatisticFile' in rdf_g and 'curv' not in rdf_g:
                    g, measure_graph = parse_stats(g, file2encode, entity)
                    if os.path.exists('fsterms.ttl'):
                        measure_graph.parse('fsterms.ttl', format='turtle')
                    measure_graph.serialize('fsterms.ttl', format='turtle')
            except IOError, e:
                print e
    return g


def to_graph(subject_specific_dir, project_id, output_dir, new_id=None):
    # location of FreeSurfer $SUBJECTS_DIR
    basedir = os.path.abspath(subject_specific_dir)
    subject_id = basedir.rstrip(os.path.sep).split(os.path.sep)[-1]

    graph = prov.ProvBundle()
    graph.add_namespace(foaf)
    graph.add_namespace(dcterms)
    graph.add_namespace(fs)
    graph.add_namespace(nidm)
    graph.add_namespace(niiri)
    graph.add_namespace(obo)
    graph.add_namespace(nif)
    graph.add_namespace(crypto)

    graph = encode_fs_directory(graph, basedir, project_id, subject_id)
    provn = graph.get_provn()
    old_id = subject_id
    if new_id:
        provn = provn.replace(subject_id, new_id)
        subject_id = new_id
    filename = os.path.join(output_dir, '%s_%s.provn' % (subject_id,
                                                         project_id))
    with open(filename, 'wt') as fp:
        fp.writelines(provn)
    filename_ttl = os.path.join(output_dir, '%s_%s.ttl' % (subject_id,
                                                           project_id))
    graph.rdf().serialize(filename_ttl, format='turtle')
    if new_id:
        map_graph = rdflib.Graph()
        map_graph.namespace_manager.bind('fs', fs.get_uri())
        map_graph.namespace_manager.bind('nidm', nidm.get_uri())
        map_graph.add((fs[new_id].rdf_representation(),
                       nidm['sameSubjectAs'].rdf_representation(),
                       fs[old_id].rdf_representation()))
        if os.path.exists('mapper.ttl'):
            map_graph.parse('mapper.ttl', format='turtle')
        map_graph.serialize('mapper.ttl', format='turtle')
    return graph, old_id

def upload_graph(graph, endpoint=None, uri=None, old_id=None, new_id=None,
                 max_stmts=100):
    import requests
    from time import sleep
    from requests.auth import HTTPDigestAuth

    # connection params for secure endpoint
    if endpoint is None:
        endpoint = 'http://metadata.incf.net:8890/sparql'

    # session defaults
    session = requests.Session()
    session.headers = {'Accept': 'text/html'} # HTML from SELECT queries

    counter = 0
    stmts = graph.rdf().serialize(format='nt').splitlines()
    N = len(stmts)
    max_tries = 10
    while (counter < N):
        endcounter = min(N, counter + max_stmts)
        query = """
INSERT DATA
{GRAPH <%s>
{
%s
}
}
""" % (uri, '\n'.join(stmts[counter:endcounter]))
        if new_id is not None:
            query = query.replace(old_id, new_id)
        data = {'query': query}
        result = session.post(endpoint, data=data)
        num_tries = 0
        while result.status_code != requests.codes.ok and num_tries < max_tries:
            sleep(5)
            result = session.post(endpoint, data=data)
            num_tries += 1
        if num_tries == max_tries:
            raise IOError('Could not upload some statements: %s' %
                          result.status_code)
        counter = endcounter
    print('Submitted %d statemnts' % N)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(prog='fs_upload_to_triplesore.py',
                                     description=__doc__)
    parser.add_argument('-s', '--subject_dir', type=str, required=True,
                        help='Path to subject directory to upload')
    parser.add_argument('-p', '--project_id', type=str, required=True,
                        help='Project tag to use for the subject directory.')
    parser.add_argument('-e', '--endpoint', type=str,
                        help='SPARQL endpoint to use for update')
    parser.add_argument('-g', '--graph_iri', type=str,
                        help='Graph IRI to store the triples')
    parser.add_argument('-o', '--output_dir', type=str,
                        help='Output directory')
    parser.add_argument('-a', '--anonymize', dest="anonymize",
                        action="store_true", help='Anonymize subject ids')
    parser.add_argument('-u', '--upload', dest="upload",
                        action="store_true", help='Upload to triplestore')
    parser.add_argument('-n', '--num_statements', dest="max_stmts", type=int,
                        default=100,
                        help=('Maximum statements to upload to triplestore in '
                              'one request'))
    parser.add_argument('-c', '--csv', dest="csv_file", type=str,
                        help='CSV file for additional participant metadata')
    parser.add_argument('--id_col_name', dest="col_name", type=str,
                        help='Column name for subject id in CSV file')

    args = parser.parse_args()
    if args.output_dir is None:
        args.output_dir = os.getcwd()

    new_id = None
    if args.anonymize:
        new_id = uuid.uuid4().hex
    graph, old_id = to_graph(args.subject_dir, args.project_id, args.output_dir,
                             new_id=new_id)
    if args.upload:
        upload_graph(graph, endpoint=args.endpoint, uri=args.graph_iri,
                     old_id=old_id, new_id=new_id, max_stmts=args.max_stmts)
