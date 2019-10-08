#!/usr/bin/env python
"""Utilities for extracting information from freesurfer stats files

"""

import json
import os
from collections import namedtuple
from pathlib import Path
import uuid
Structure = namedtuple('Structure', field_names=['niiri', 'isAbout',
                                                 'hasLaterality',
                                                 'label', 'source_info'])


def read_stats(filename):
    """Convert stats file to a structure
    """
    header = {}
    tableinfo = {}
    measures = []

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
            prefix = ''
            if 'lh.' in str(filename):
                prefix = 'lh-'
            if 'rh.' in str(filename):
                prefix = 'rh-'
            if row[struct_idx-1].lower() == 'unknown':
                prefix = ''
            structure_template = {'structure': prefix + row[struct_idx-1],
                                  'source': 'Table'}
            for idx, value in enumerate(row):
                if idx + 1 == struct_idx or tableinfo[idx + 1]['ColHeader'] == 'Index':
                    continue
                struct = structure_template.copy()
                struct.update(**{
                    'name': tableinfo[idx + 1]['ColHeader'],
                    'description': tableinfo[idx + 1]['FieldName'],
                    'value': value,
                    'units': tableinfo[idx + 1]['Units'],
                    })
                measures.append(struct)
    measureset = set()
    structureset = set()
    structmeasureset = set()
    for item in measures:
        measureset.add((item['name'], item['description'], item['units']))
        structureset.add((item['structure']))
        structmeasureset.add((item['structure'], item['name'], item['units']))
    return measures, structureset, measureset, structmeasureset, (header, tableinfo)


def get_normative_measure(measure):
    normative = {'measureOf': None,
                 'datumType': None,
                 'hasUnit': None,
                 }
    fs_uri = 'https://surfer.nmr.mgh.harvard.edu/fswiki/terms/'
    if 'Ratio' in measure[1]:
        normative['measureOf'] = fs_uri + measure[0]
        normative['datumType'] = 'http://purl.obolibrary.org/obo/STATO_0000184'
    elif 'SNR' in measure[1]:
        normative['measureOf'] = fs_uri + measure[1]
        normative[
            'datumType'] = 'http://purl.obolibrary.org/obo/STATO_0000184'
    elif 'Vol' in measure[0] or 'Vol' in measure[1] or 'Volume' in measure[1]:
        normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0112559'
        normative['datumType'] = 'http://uri.interlex.org/base/ilx_0738276'
        normative['hasUnit'] = measure[2]
    elif 'Number' in measure[1] or 'number' in measure[1]:
        normative['datumType'] = 'http://uri.interlex.org/base/ilx_0102597'
        if 'defect holes' in measure[1]:
            normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0112559'
            normative['hasUnit'] = fs_uri + 'defect-hole'
        elif 'oxel' in measure[1]:
            normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0112559'
            normative['hasUnit'] = 'voxel'
        elif 'ertices' in measure[1]:
            normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0112559'
            normative['hasUnit'] = 'vertex'
        else:
            raise ValueError(f'Could not determine units for {measure}')
    elif 'Intensity' in measure[1] or 'Itensity' in measure[1]:
        normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0738276'
        if "Mean" in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738264"
        elif "StdDev" in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738265"
        elif "Min" in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738266"
        elif "Max" in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738267"
        elif "Range" in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738268"
        else:
            raise ValueError(f'Could not parse {measure}')
        normative['hasUnit'] = fs_uri + measure[2]
    elif 'Thick' in measure[0]:
        normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0738276'
        if "Mean" in measure[0] or 'Avg' in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738264"
        elif "Std" in measure[0]:
            normative['datumType'] = "http://uri.interlex.org/base/ilx_0738265"
        else:
            raise ValueError(f'Could not parse {measure}')
        normative['hasUnit'] = measure[2]
    elif 'Area' in measure[0]:
        normative['measureOf'] = 'http://purl.obolibrary.org/obo/PATO_0001323'
        normative['datumType'] = 'http://uri.interlex.org/base/ilx_0738276'
        normative['hasUnit'] = measure[2]
    elif 'Index' in measure[1]:
        if 'Curv' in measure[0]:
            normative['measureOf'] = 'http://purl.obolibrary.org/obo/PATO_0001591'
            normative['datumType'] = 'http://purl.obolibrary.org/obo/NCIT_C25390'
        elif 'Fold' in measure[0]:
            normative['measureOf'] = fs_uri + 'folding'
            normative['datumType'] = 'http://purl.obolibrary.org/obo/NCIT_C25390'
        else:
            raise ValueError(f'Unknown Index type {measure}')
    elif 'Curv' in measure[0]:
        normative['measureOf'] = 'http://purl.obolibrary.org/obo/PATO_0001591'
        normative['datumType'] = 'http://uri.interlex.org/base/ilx_0738276'
        normative['hasUnit'] = measure[2]
    elif 'SegId' in measure[0]:
        normative['measureOf'] = 'http://uri.interlex.org/base/ilx_0100965'
        normative['datumType'] = 'http://uri.interlex.org/base/ilx_0738276'
    else:
        raise ValueError(f'Could not parse {measure}')
    return {k:v for k, v in normative.items() if v is not None}


def get_normative_structures(structure):
    """Returns a structure from the ReproNimCDE if available
    """
    import pandas as pd
    import numpy as np
    normative = {'isAbout': '<UNKNOWN>',
                 'hasLaterality': None,
                 }
    location = Path(os.path.dirname(__file__))
    df = pd.read_excel(location / 'mapping_data/ReproNimCDEs.xlsx',
                       header=[0, 1])
    labels = df[('Atlas Segmentation Label', 'Unnamed: 6_level_1')].str
    start_indices = labels.find(structure).values
    indices = np.nonzero(start_indices > -1)
    indices = indices[0]
    if len(indices):
        idx = indices[start_indices[indices].argsort()[0]]
        uberon = df[('Structure', 'Preferred')].iloc[idx]
        if str(uberon) != 'nan':
            uberon = uberon.replace('UBERON:', 'http://purl.obolibrary.org/obo/UBERON_')
            normative['isAbout'] = uberon
        laterality = df[('Laterality', 'ILX:0106135')].iloc[idx]
        normative['hasLaterality'] = laterality
    else:
        normative['isAbout'] = f'<UNKNOWN - {structure}>'
    if normative['hasLaterality'] is None:
        if 'lh' in structure or 'Left' in structure:
            normative['hasLaterality'] = 'Left'
        elif 'rh' in structure or 'Right' in structure:
            normative['hasLaterality'] = 'Right'
        else:
            normative['hasLaterality'] = f'<UNKNOWN - {structure}>'
    if normative['hasLaterality'] == 'None':
        normative['hasLaterality'] = None
    return {k:v for k, v in normative.items() if v is not None}


def collate_fs_items(freesurfer_stats_dir):
    structures = set()
    measureset = set()
    structmeasureset = set()
    for fl in Path(freesurfer_stats_dir).glob('*.stats'):
        if 'curv' in str(fl):
            print(f'skipping {fl}')
            continue
        print(f'reading {fl}')
        _, structures_in_file, measureset_in_file, structmeasureset_in_file, _ \
            = read_stats(fl)
        structures = structures.union(structures_in_file)
        measureset = measureset.union(measureset_in_file)
        structmeasureset = structmeasureset.union(structmeasureset_in_file)
    return structures, measureset, structmeasureset


def get_niiri_index(fs_map, structure, name, units):
    """Generates index for retrieve consistent niiri

    >>> get_niiri_index(fs_map, 'lhCortex', 'lhCortexVol', 'mm^3')
    "(('datumType', 'http://uri.interlex.org/base/ilx_0738276'), ('hasLaterality', 'Left'), ('hasUnit', 'mm^3'), ('isAbout', 'http://purl.obolibrary.org/obo/UBERON_0000956'), ('measureOf', 'http://uri.interlex.org/base/ilx_0112559'))"
    """
    meta_datum = fs_map['Structures'][structure].copy()
    meta_datum.update(**fs_map['Measures'][str((name, units))])
    niiri_key = str(tuple(sorted(meta_datum.items())))
    return niiri_key


def create_fs_mapper(s, m, measures=None, map_file=None):
    """Create FreeSurfer to ReproNim mapping information

    >>> from segstats_jsonld.fsutils import collate_fs_items, create_fs_mapper
    >>> s, m, ms = collate_fs_items('/software/work/test_mb/out/freesurfer_subjects/satra/stats/')
    >>> fs_map = create_fs_mapper(s, m, ms)
    """

    if map_file is None:
        map_file = Path(os.path.dirname(__file__)) / 'mapping_data' / 'freesurfermap.json'

    with open(map_file, 'r') as fp:
        fs_map = json.load(fp)

    niiri_file = map_file.parent / 'freesurfer-niiri.json'
    with open(niiri_file, 'r') as fp:
        niirimap = json.load(fp)

    # fs_map = {'Structures': {}, 'Measures': {}}
    # niirimap = {'fs2commonMap': {}, 'niiriMap': {}}

    measure_mapper = fs_map['Measures']
    for ms in sorted(m):
        if str(ms) not in measure_mapper:
            measure_mapper[str((ms[0], ms[2]))] = get_normative_measure(ms)
    structure_mapper = fs_map['Structures']
    for st in sorted(s):
        if str(st) not in structure_mapper:
            structure_mapper[str(st)] = get_normative_structures(st)

    if measures is not None:
        for row in measures:
            niiri_key = get_niiri_index(fs_map, row[0], row[1], row[2])
            niirimap['fs2commonMap'][str((row[0], row[1], row[2]))] = niiri_key
            if niiri_key not in niirimap['niiriMap']:
                niirimap['niiriMap'][niiri_key] = 'x' + uuid.uuid4().hex

    with open(map_file, 'w') as fp:
        json.dump(fs_map, fp, indent=2, sort_keys=True)
    with open(niiri_file, 'w') as fp:
        json.dump(niirimap, fp, indent=2, sort_keys=True)

    return fs_map, niirimap
