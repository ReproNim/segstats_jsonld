from collections import namedtuple
import json

from .fsutils import hemiless

FSL = namedtuple("FSL", ["structure", "hemi", "measure", "unit"])
cde_file = Path(os.path.dirname(__file__)) / "mapping_data" / "fsl-cde.json"
map_file = Path(os.path.dirname(__file__)) / "mapping_data" / "fslmap.json"


def read_fsl_stats(stat_file):
    with open(stat_file, "r") as fp:
        data = json.load(fp)

    with open(cde_file, "r") as fp:
        cde = json.load(fp)

    measures = []
    for key, value in data.items():
        voxkey = FSL(
            structure=key,
            hemi="Left" if "Left" in key else "Right" if "Right" in key else None,
            measure="NVoxels",
            unit="voxel",
        )
        volkey = FSL(structure=key, hemi=voxkey.hemi, measure="Volume", unit="mm^3")
        if voxkey in cde:
            measures.append((f'{fs_cde[str(voxkey)]["id"]}', value[0]))
        else:
            raise ValueError(f"Key {voxkey} not found in FSL data elements file")
        if volkey in cde:
            measures.append((f'{fs_cde[str(volkey)]["id"]}', value[1]))
        else:
            raise ValueError(f"Key {volkey} not found in FSL data elements file")
    return measures


def map_fsl_cdes():
    """Update FSL to ReproNim mapping information
    """

    with open(map_file, "r") as fp:
        fsl_map = json.load(fp)

    with open(cde_file, "r") as fp:
        fsl_cde = json.load(fp)

    mmap = fsl_map["Measures"]
    smap = fsl_map["Structures"]
    for key in fsl_cde:
        if key == "count":
            continue
        key_tuple = eval(key)
        # Deal with structures
        hkey = hemiless(key_tuple.structure)
        if hkey in smap:
            if smap[hkey]["isAbout"] is not None and (
                "UNKNOWN" not in smap[hkey]["isAbout"]
                and "CUSTOM" not in smap[hkey]["isAbout"]
            ):
                fsl_cde[key]["isAbout"] = smap[hkey]["isAbout"]
        if mmap[key_tuple.measure]["measureOf"] is not None:
            fsl_cde[key].update(**mmap[key_tuple.measure])

    with open(map_file, "w") as fp:
        json.dump(fsl_map, fp, sort_keys=True, indent=2)
        fp.write("\n")

    with open(cde_file, "w") as fp:
        json.dump(fsl_cde, fp, indent=2)
        fp.write("\n")

    return fsl_map, fsl_cde


def create_cde_graph(restrict_to=None):
    """Create an RDFLIB graph with the FSL CDEs

    Any CDE that has a mapping will be mapped
    """
    import rdflib as rl
    from nidm.core import Constants

    with open(cde_file, "r") as fp:
        fsl_cde = json.load(fp)

    fsl = Constants.FSL
    nidm = Constants.NIDM

    g = rl.Graph()
    g.bind("fsl", fsl)
    g.bind("nidm", nidm)

    for key, value in fsl_cde.items():
        if key == "count":
            continue
        if restrict_to is not None:
            if value["id"] not in restrict_to:
                continue
        for subkey, item in value.items():
            if subkey == "id":
                fslid = "fsl_" + item
                g.add((fsl[fslid], rl.RDF.type, fsl["DataElement"]))
                continue
            if item is None or "unknown" in str(item):
                continue
            if isinstance(item, str) and item.startswith("fsl:"):
                item = fsl[item.replace("fsl:", "")]
            if subkey in ["isAbout", "datumType", "measureOf"]:
                g.add((fsl[fslid], nidm[subkey], rl.URIRef(item)))
            else:
                if isinstance(item, rl.URIRef):
                    g.add((fsl[fslid], fsl[subkey], item))
                else:
                    g.add((fsl[fslid], fsl[subkey], rl.Literal(item)))
        key_tuple = eval(key)
        for subkey, item in key_tuple._asdict().items():
            if item is None:
                continue
            if subkey == "hemi":
                g.add((fsl[fslid], nidm["hasLaterality"], rl.Literal(item)))
            else:
                g.add((fsl[fslid], fsl[subkey], rl.Literal(item)))
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
