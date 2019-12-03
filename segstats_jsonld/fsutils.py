#!/usr/bin/env python
"""Utilities for extracting information from freesurfer stats files

"""

import json
import os
from collections import namedtuple
from pathlib import Path
import rdflib as rl
from requests import get

FS = namedtuple("FS", ["structure", "hemi", "measure", "unit"])
cde_file = Path(os.path.dirname(__file__)) / "mapping_data" / "freesurfer-cdes.json"
map_file = Path(os.path.dirname(__file__)) / "mapping_data" / "freesurfermap.json"
lut_file = Path(os.path.dirname(__file__)) / "mapping_data" / "FreeSurferColorLUT.txt"


def get_segid(filename, structure):
    """Should return the segmentation label of the freesurfer structure

    :param filename:
    :param structure:
    :return:
    """
    structure = structure.replace("&", "_and_")
    filename = str(filename)
    label = structure
    if "lh" in filename:
        hemi = "lh"
    if "rh" in filename:
        hemi = "rh"
    if (
        "aparc.stats" in filename
        or "a2005" in filename
        or "DKT" in filename
        or "aparc.pial" in filename
        or "w-g.pct" in filename
    ):
        label = f"ctx-{hemi}-{structure}"
    if "a2009" in filename:
        label = f"ctx_{hemi}_{structure}"
    if "BA" in filename:
        label = structure.split("_exvivo")[0]
    try:
        with open(lut_file, "rt") as fp:
            for line in fp.readlines():
                vals = line.split()
                if len(vals) > 2 and vals[1] == label:
                    return int(vals[0])
    except UnboundLocalError:
        print(f"{filename} - {structure}")
        raise
    return None


def make_label(info):
    label = []
    for key in ["hemi", "structure", "measure"]:
        val = info.get(key, None)
        if val:
            label.append(val)
    if not ("unitless" in info["unit"] or "NA" in info["unit"]):
        label.append(f"({info['unit']})")
    if len(label) > 1 and label[0] in label[1]:
        label = label[1:]
    return " ".join(label)


def read_stats(filename, error_on_new_key=True):
    """Convert stats file to a structure
    """
    header = {}
    tableinfo = {}
    measures = []

    with open(cde_file, "r") as fp:
        fs_cde = json.load(fp)
    with open(filename, "rt") as fp:
        lines = fp.readlines()
    updated = False
    for line in lines:
        if line == line[0]:
            continue
        # parse commented header
        if line.startswith("#"):
            fields = line.split()[1:]
            if len(fields) < 2:
                continue
            tag = fields[0]
            if tag == "TableCol":
                col_idx = int(fields[1])
                if col_idx not in tableinfo:
                    tableinfo[col_idx] = {}
                tableinfo[col_idx][fields[2]] = " ".join(fields[3:])
                if tableinfo[col_idx][fields[2]] == "StructName":
                    struct_idx = col_idx
            elif tag == "Measure":
                fields = " ".join(fields).replace("CortexVol ", "CortexVol, ").split()
                fields = " ".join(fields[1:]).split(", ")
                hemi = None
                if fields[0].startswith("lh"):
                    hemi = "Left"
                if fields[0].startswith("rh"):
                    hemi = "Right"
                fskey = FS(
                    structure=fields[0], hemi=hemi, measure=fields[1], unit=fields[4]
                )
                if str(fskey) not in fs_cde:
                    fs_cde["count"] += 1
                    extra_info = dict(
                        id=f"{fs_cde['count']:0>6d}",
                        structure_id=None,
                        label=make_label(dict(measure=fields[2], unit=fields[4])),
                        description=" ".join((fields[2], "(" + fields[4] + ")")),
                        key_source="Header",
                    )
                    fs_cde[str(fskey)] = extra_info
                    updated = True
                    if error_on_new_key:
                        raise ValueError(
                            f"Your file ({filename}) is adding a new key: {fskey}."
                        )
                measures.append((f'{fs_cde[str(fskey)]["id"]}', fields[3]))
            elif tag == "ColHeaders":
                if len(fields) != len(tableinfo):
                    for idx, fieldname in enumerate(fields[1:]):
                        if idx + 1 in tableinfo:
                            continue
                        tableinfo[idx + 1] = {
                            "ColHeader": fieldname,
                            "Units": "unknown",
                            "FieldName": fieldname,
                        }
                else:
                    continue
            else:
                header[tag] = " ".join(fields[1:])
        else:
            # read values
            row = line.split()
            segid = None
            hemi = None
            if "lh." in str(filename) or "Left" in row[struct_idx - 1]:
                hemi = "Left"
                segid = get_segid(filename, row[struct_idx - 1])
            if "rh." in str(filename) or "Right" in row[struct_idx - 1]:
                hemi = "Right"
                segid = get_segid(filename, row[struct_idx - 1])
            if row[struct_idx - 1].lower() == "unknown":
                hemi = None
            for idx, value in enumerate(row):
                if idx + 1 == struct_idx or tableinfo[idx + 1]["ColHeader"] == "Index":
                    continue
                if tableinfo[idx + 1]["ColHeader"] == "SegId":
                    segid = int(value)
                    continue
                fskey = FS(
                    structure=row[struct_idx - 1],
                    hemi=hemi,
                    measure=tableinfo[idx + 1]["ColHeader"],
                    unit=tableinfo[idx + 1]["Units"],
                )
                if str(fskey) not in fs_cde:
                    fs_cde["count"] += 1
                    extra_info = dict(
                        id=f"{fs_cde['count']:0>6d}",
                        structure_id=segid,
                        label=make_label(
                            dict(
                                structure=row[struct_idx - 1],
                                hemi=hemi or None,
                                measure=tableinfo[idx + 1]["ColHeader"],
                                unit=tableinfo[idx + 1]["Units"],
                            )
                        ),
                        description=" ".join(
                            [
                                val
                                for val in [
                                    hemi
                                    if hemi and hemi not in row[struct_idx - 1]
                                    else "",
                                    row[struct_idx - 1],
                                    tableinfo[idx + 1]["FieldName"],
                                    "(" + tableinfo[idx + 1]["Units"] + ")",
                                ]
                                if val
                            ]
                        ),
                        key_source="Table",
                    )
                    fs_cde[str(fskey)] = extra_info
                    updated = True
                    if error_on_new_key:
                        raise ValueError(
                            f"Your file ({filename}) is adding a new key: {fskey}."
                        )
                measures.append((f'{fs_cde[str(fskey)]["id"]}', value))
    if updated:
        with open(cde_file, "w") as fp:
            json.dump(fs_cde, fp, indent=2)
    return measures, header


def collate_fs_items(freesurfer_stats_dir, error_on_new_key=True):
    """Collect values from all stats files

    :param freesurfer_stats_dir: string - Freesurfer stats directory
    :param error_on_new_key: bool - Raise error if a new key is found
    :return: list - A list of tuples (values, header information)
    """
    result = []
    for fl in Path(freesurfer_stats_dir).glob("*.stats"):
        if "curv" in str(fl):
            print(f"skipping {fl}")
            continue
        print(f"reading {fl}")
        m, h = read_stats(fl, error_on_new_key=error_on_new_key)
        result.append((fl, m, h))
    return result


def get_normative_measure(measure):
    normative = {"measureOf": None, "datumType": None, "hasUnit": None}
    fs_uri = "fs:"
    if "Ratio" in measure[1]:
        normative["measureOf"] = fs_uri + measure[0]
        normative["datumType"] = "http://purl.obolibrary.org/obo/STATO_0000184"
    elif "SNR" in measure[1]:
        normative["measureOf"] = fs_uri + measure[1]
        normative["datumType"] = "http://purl.obolibrary.org/obo/STATO_0000184"
    elif "Vol" in measure[0] or "Vol" in measure[1] or "Volume" in measure[1]:
        normative["measureOf"] = "http://uri.interlex.org/base/ilx_0112559"
        normative["datumType"] = "http://uri.interlex.org/base/ilx_0738276"
        normative["hasUnit"] = measure[2]
    elif "Number" in measure[1] or "number" in measure[1]:
        normative["datumType"] = "http://uri.interlex.org/base/ilx_0102597"
        if "defect holes" in measure[1]:
            normative["measureOf"] = "http://uri.interlex.org/base/ilx_0112559"
            normative["hasUnit"] = fs_uri + "defect-hole"
        elif "oxel" in measure[1]:
            normative["measureOf"] = "http://uri.interlex.org/base/ilx_0112559"
            normative["hasUnit"] = "voxel"
        elif "ertices" in measure[1]:
            normative["measureOf"] = "http://uri.interlex.org/base/ilx_0112559"
            normative["hasUnit"] = "vertex"
        else:
            raise ValueError(f"Could not determine units for {measure}")
    elif "Intensity" in measure[1] or "Itensity" in measure[1]:
        normative["measureOf"] = "http://uri.interlex.org/base/ilx_0738276"
        if "Mean" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738264"
        elif "StdDev" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738265"
        elif "Min" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738266"
        elif "Max" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738267"
        elif "Range" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738268"
        else:
            raise ValueError(f"Could not parse {measure}")
        normative["hasUnit"] = fs_uri + measure[2]
    elif "Thick" in measure[0]:
        normative["measureOf"] = "http://uri.interlex.org/base/ilx_0738276"
        if "Mean" in measure[0] or "Avg" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738264"
        elif "Std" in measure[0]:
            normative["datumType"] = "http://uri.interlex.org/base/ilx_0738265"
        else:
            raise ValueError(f"Could not parse {measure}")
        normative["hasUnit"] = measure[2]
    elif "Area" in measure[0]:
        normative["measureOf"] = "http://purl.obolibrary.org/obo/PATO_0001323"
        normative["datumType"] = "http://uri.interlex.org/base/ilx_0738276"
        normative["hasUnit"] = measure[2]
    elif "Index" in measure[1]:
        if "Curv" in measure[0]:
            normative["measureOf"] = "http://purl.obolibrary.org/obo/PATO_0001591"
            normative["datumType"] = "http://purl.obolibrary.org/obo/NCIT_C25390"
        elif "Fold" in measure[0]:
            normative["measureOf"] = fs_uri + "folding"
            normative["datumType"] = "http://purl.obolibrary.org/obo/NCIT_C25390"
        else:
            raise ValueError(f"Unknown Index type {measure}")
    elif "Curv" in measure[0]:
        normative["measureOf"] = "http://purl.obolibrary.org/obo/PATO_0001591"
        normative["datumType"] = "http://uri.interlex.org/base/ilx_0738276"
        normative["hasUnit"] = measure[2]
    elif "SegId" in measure[0]:
        normative["measureOf"] = "http://uri.interlex.org/base/ilx_0100965"
        normative["datumType"] = "http://uri.interlex.org/base/ilx_0738276"
    else:
        raise ValueError(f"Could not parse {measure}")
    return {k: v for k, v in normative.items() if v is not None}


def get_normative_structures(structure):
    """Returns a structure from the ReproNimCDE if available
    """
    import pandas as pd
    import numpy as np

    normative = {"isAbout": "<UNKNOWN>", "hasLaterality": None}
    location = Path(os.path.dirname(__file__))
    df = pd.read_excel(location / "mapping_data/ReproNimCDEs.xlsx", header=[0, 1])
    labels = df[("Atlas Segmentation Label", "Unnamed: 6_level_1")].str
    start_indices = labels.find(structure).values
    indices = np.nonzero(start_indices > -1)
    indices = indices[0]
    if len(indices):
        idx = indices[start_indices[indices].argsort()[0]]
        uberon = df[("Structure", "Preferred")].iloc[idx]
        if str(uberon) != "nan":
            uberon = uberon.replace("UBERON:", "http://purl.obolibrary.org/obo/UBERON_")
            normative["isAbout"] = uberon
        laterality = df[("Laterality", "ILX:0106135")].iloc[idx]
        normative["hasLaterality"] = laterality
    else:
        normative["isAbout"] = f"<UNKNOWN - {structure}>"
    if normative["hasLaterality"] is None:
        if "lh" in structure or "Left" in structure:
            normative["hasLaterality"] = "Left"
        elif "rh" in structure or "Right" in structure:
            normative["hasLaterality"] = "Right"
        else:
            normative["hasLaterality"] = f"<UNKNOWN - {structure}>"
    if normative["hasLaterality"] == "None":
        normative["hasLaterality"] = None
    return {k: v for k, v in normative.items() if v is not None}


def get_niiri_index(fs_map, structure, name, units):
    """Generates index for retrieve consistent niiri

    >>> get_niiri_index(fs_map, 'lhCortex', 'lhCortexVol', 'mm^3')
    "(('datumType', 'http://uri.interlex.org/base/ilx_0738276'), ('hasLaterality', 'Left'), ('hasUnit', 'mm^3'), ('isAbout', 'http://purl.obolibrary.org/obo/UBERON_0000956'), ('measureOf', 'http://uri.interlex.org/base/ilx_0112559'))"
    """
    meta_datum = fs_map["Structures"][structure].copy()
    meta_datum.update(**fs_map["Measures"][str((name, units))])
    niiri_key = str(tuple(sorted(meta_datum.items())))
    return niiri_key


def get_label(url):
    if "fswiki" in url:
        return url.split("/")[-1]
    g = rl.Graph()
    res = get(url, headers={"Accept": "application/rdf+xml"})
    g.parse(data=res.text, format="application/rdf+xml")
    if "interlex" in url:
        url = url.replace("/base/ilx", "/base/ontologies/ilx")
    query = "SELECT ?label WHERE { <%s> rdfs:label ?label . }" % url
    query2 = (
        "SELECT ?label WHERE { <%s> <http://purl.obolibrary.org/obo/IAO_0000136>/rdfs:label ?label . }"
        % url
    )
    try:
        label = str(g.query(query).bindings.pop()["label"])
    except IndexError:
        try:
            label = str(g.query(query2).bindings.pop()["label"])
        except IndexError:
            label = url
    return label


def hemiless(key):
    return (
        key.replace("-lh-", "-")
        .replace("-rh-", "-")
        .replace("_lh_", "-")
        .replace("_rh_", "-")
        .replace("rh", "")
        .replace("lh", "")
        .replace("Left-", "")
        .replace("Right-", "")
    )


def create_fs_mapper():
    """Create FreeSurfer to ReproNim mapping information
    """

    with open(map_file, "r") as fp:
        fs_map = json.load(fp)

    with open(cde_file, "r") as fp:
        fs_cde = json.load(fp)

    mmap = fs_map["Measures"]
    if "('SegId', 'NA')" in mmap:
        del mmap["('SegId', 'NA')"]

    for key in fs_cde:
        if key == "count":
            continue
        key_tuple = eval(key)
        if key_tuple.measure in mmap:
            continue
        # Deal with measures
        old_key = str((key_tuple.measure, key_tuple.unit))
        if old_key in mmap:
            value = mmap[old_key]
            value["fsunit"] = key_tuple.unit
            del mmap[old_key]
            mmap[key_tuple.measure] = value
        if key_tuple.measure not in mmap:
            mmap[key_tuple.measure] = get_normative_measure(
                (key_tuple.measure, fs_cde[key]["description"], key_tuple.unit)
            )
            mmap[key_tuple.measure]["fsunit"] = key_tuple.unit

    for key in mmap:
        for subkey, value in mmap[key].items():
            if value.startswith("https://surfer.nmr.mgh.harvard.edu/fswiki/terms/"):
                mmap[key][subkey] = mmap[key][subkey].replace(
                    "https://surfer.nmr.mgh.harvard.edu/fswiki/terms/", "fs:"
                )

    smap = fs_map["Structures"]
    for key in fs_cde:
        if key == "count":
            continue
        key_tuple = eval(key)
        # Deal with structures
        hkey = hemiless(key_tuple.structure)
        # take care of older keys
        if key_tuple.structure in smap and "fskey" not in smap[key_tuple.structure]:
            value = smap[key_tuple.structure]
            del smap[key_tuple.structure]
            if hkey not in smap:
                smap[hkey] = dict(
                    isAbout=value["isAbout"]
                    if value["isAbout"] and "UNKNOWN" not in value["isAbout"]
                    else None,
                    fskey=[key_tuple.structure],
                )
            else:
                if "fskey" not in smap[hkey]:
                    smap[hkey]["fskey"] = [hkey]
                if "hasLaterality" in smap[hkey]:
                    del smap[hkey]["hasLaterality"]
                if "UNKNOWN" not in value["isAbout"]:
                    if smap[hkey]["isAbout"] is not None:
                        assert smap[hkey]["isAbout"] == value["isAbout"]
                    smap[hkey]["isAbout"] = value["isAbout"]
                if key_tuple.structure not in smap[hkey]["fskey"]:
                    smap[hkey]["fskey"].append(key_tuple.structure)
        # add new keys
        if hkey in smap:
            if smap[hkey]["isAbout"] is not None and (
                "UNKNOWN" not in smap[hkey]["isAbout"]
                and "CUSTOM" not in smap[hkey]["isAbout"]
            ):
                fs_cde[key]["isAbout"] = smap[hkey]["isAbout"]
            if key_tuple.structure not in smap[hkey]["fskey"]:
                smap[hkey]["fskey"].append(key_tuple.structure)
        else:
            smap[hkey] = dict(isAbout=None, fskey=[])

        if mmap[key_tuple.measure]["measureOf"] is not None:
            fs_cde[key].update(**mmap[key_tuple.measure])

    with open(map_file, "w") as fp:
        json.dump(fs_map, fp, sort_keys=True, indent=2)
        fp.write("\n")

    with open(cde_file, "w") as fp:
        json.dump(fs_cde, fp, indent=2)
        fp.write("\n")

    return fs_map, fs_cde


def create_cde_graph(restrict_to=None):
    """Create an RDFLIB graph with the FreeSurfer CDEs

    Any CDE that has a mapping will be mapped
    """
    with open(cde_file, "r") as fp:
        fs_cde = json.load(fp)
    from nidm.core import Constants

    fs = Constants.FREESURFER
    nidm = Constants.NIDM

    g = rl.Graph()
    g.bind("fs", fs)
    g.bind("nidm", nidm)

    # added by DBK to create subclass relationship
    g.add((fs["DataElement"], rl.RDFS['subClassOf'], nidm['DataElement']))


    for key, value in fs_cde.items():
        if key == "count":
            continue
        if restrict_to is not None:
            if value["id"] not in restrict_to:
                continue
        for subkey, item in value.items():
            if subkey == "id":
                fsid = "fs_" + item
                g.add((fs[fsid], rl.RDF.type, fs["DataElement"]))
                continue
            if item is None or "unknown" in str(item):
                continue
            if isinstance(item, str) and item.startswith("fs:"):
                item = fs[item.replace("fs:", "")]
            if subkey in ["isAbout", "datumType", "measureOf"]:
                g.add((fs[fsid], nidm[subkey], rl.URIRef(item)))
            elif subkey in ["hasUnit"]:
                g.add((fs[fsid], nidm[subkey], rl.Literal(item)))
            # added by DBK to use rdfs:label
            elif subkey in ["label"]:
                g.add((fs[fsid], rl.RDFS['label'], rl.Literal(item)))
            else:
                if isinstance(item, rl.URIRef):
                    g.add((fs[fsid], fs[subkey], item))
                else:
                    g.add((fs[fsid], fs[subkey], rl.Literal(item)))
        key_tuple = eval(key)
        for subkey, item in key_tuple._asdict().items():
            if item is None:
                continue
            if subkey == "hemi":
                g.add((fs[fsid], nidm["hasLaterality"], rl.Literal(item)))
            else:
                g.add((fs[fsid], fs[subkey], rl.Literal(item)))
    return g


def convert_stats_to_nidm(stats):
    """Convert a stats record into a NIDM entity

    Returns the entity and the prov document
    """
    from nidm.core import Constants
    from nidm.experiment.Core import getUUID
    import prov

    fs = prov.model.Namespace("fs", str(Constants.FREESURFER))
    niiri = prov.model.Namespace("niiri", str(Constants.NIIRI))
    nidm = prov.model.Namespace("nidm", "http://purl.org/nidash/nidm#")
    doc = prov.model.ProvDocument()
    e = doc.entity(identifier=niiri[getUUID()])
    e.add_asserted_type(nidm["FSStatsCollection"])
    e.add_attributes(
        {
            fs["fs_" + val[0]]: prov.model.Literal(
                val[1],
                datatype=prov.model.XSD["float"]
                if "." in val[1]
                else prov.model.XSD["integer"],
            )
            for val in stats
        }
    )
    return e, doc
