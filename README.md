# Making Freesurfer FAIR
Script to Export Freesurfer-based Parcellation/Segmentation Stats and Provenance as JSON-LD and NIDM
## Project Description
This project ultimately aims to facilitate both query and analysis of parcellation/segmentation based regional statistics across popular softwares such as [Freesurfer](https://surfer.nmr.mgh.harvard.edu/), [FSL](https://fsl.fmrib.ox.ac.uk/fsl/fslwiki), and [ANTS](http://stnava.github.io/ANTs/). Currently each software produces its own output format and brain region labels are specific to the atlas used in generating the regional statistics.  This makes life difficult when trying to search for "nucleus accumbens" volume, for example, across the different software products.  Further, knowing which version of the software tool used and what atlas and version of the atlas in a structured representation facilitating query is lacking.  To this end we propose augmenting the various segmentation tools with scripts that will: (1) map atlas-specific anatomical nomeclature to anatomical concepts hosted in terminology resources (e.g. InterLex); (2) capture better structured provenance about the input image(s) and the atlases used for the segmentation; (3) export the segmentation results and the provenance as either [JSON-LD](https://json-ld.org/), [NIDM](http://nidm.nidash.org/) which can then link the derived data to broader records of the original project metadata, or as an additional component of a BIDS derivative.

We aim to tackle this problem in steps.  For this hackathon project we'll be focusing on conversion from Freesurfer's [mri_segstats](https://surfer.nmr.mgh.harvard.edu/fswiki/mri_segstat) program output along with some additional parsing/conversion of Freesurfer log files. The conversion is driven by a function which queries InterLex and develops a JSON structure which defines the atlas terminology and the measures being output.

## Skills required to participate
Python and structural neuroimaging experience.  If one has experience with [rdflib](https://github.com/RDFLib/rdflib) or [PROV](https://github.com/trungdong/prov) that would also be helpful.

## Participants
- David Keator
- Adina Wagner
- Jeffrey Grethe
- Satra Ghosh
- David Kennedy
- JB Poline

## Integration
This project will need expertise in programming, structural neuroimaging, and anatomy.  To make this project sucessful we need individuals who have skills in any of these domains to help with: (1) understand Freesurfer's segmentation results format and log files; (2) programming up a script in Python; (3) understand anatomy well enough to select the proper anatomical concept that maps to a specific atlas designation of a region and ***can define new anatomy terms where needed, linking them to broader concepts*** to facilitate segmentation results queries across softwares.

## Preparation material
* [Freesurfer](https://surfer.nmr.mgh.harvard.edu/)
* [mri_segstats](https://surfer.nmr.mgh.harvard.edu/fswiki/mri_segstat)
* [rdflib](https://github.com/RDFLib/rdflib)
* [PyNIDM](https://github.com/incf-nidash/PyNIDM)
* [InterLex Term Search](https://scicrunch.org/scicrunch/interlex/dashboard)
* [Anatomical Term Mappings via InterLex](https://docs.google.com/spreadsheets/d/1VcpNj1deZ7dF8XM6yXt5VWCNVVQkCnV9Y48wvMFYw0g)
* Examples done pseudo-manually of Freesurfer, FSL, and ANTS segmentation data added to NIDM documents for [ABIDE](https://github.com/dbkeator/simple2_NIDM_examples/tree/master/datasets.datalad.org/abide/RawDataBIDS) and [ADHD200](https://github.com/dbkeator/simple2_NIDM_examples/tree/master/datasets.datalad.org/adhd200/RawDataBIDS) datasets


## Installation

```
$ conda create -n segstats_jsonld python=3
$ source activate segstats_jsonld
$ git clone https://github.com/ReproNim/segstats_jsonld.git
$ cd segstats_jsonld
$ pip install -e .
```

## Usage

The program is installed as `segstats2nidm`; `python -m segstats_jsonld.fs_to_nidm` runs the same
program. Give exactly one of `-o` (write new NIDM files) and `-n` (add the stats to an existing NIDM
file). With `-s`, `-o` is a directory that receives one file per stats file; with `-f` or `-csv` it is
the output file. The text below is the program's `--help`.

```
usage: fs_to_nidm.py [-h] (-s SUBJECT_DIR | -f SEGFILE | -csv CSVFILE) [-subjid SUBJID]
                     (-o OUTPUT | -n NIDM_FILE) [-j] [-add_de] [-forcenidm] [-json_map JSON_MAP]
                     [-acq_filename ACQ_FILENAME [ACQ_FILENAME ...]]

This program reads FreeSurfer stats files (e.g. aseg.stats), maps the FreeSurfer regions and
measures to common data elements, and writes the statistics as NIDM (Turtle or JSON-LD), either as
new files (-o) or added to an existing NIDM file (-n).

options:
  -h, --help            show this help message and exit
  -s SUBJECT_DIR, --subject_dir SUBJECT_DIR
                        Path to Freesurfer subject directory
  -f SEGFILE, --seg_file SEGFILE
                        Path or URL to a specific Freesurfer stats file. Check supported_files for
                        list of supported stats files.
  -csv CSVFILE, --csv_file CSVFILE
                        Path to CSV file which includes a header row with 1 column containing
                        subject IDs and the other columns are variables indicating the Freesurfer-
                        derived region measure (e.g. volume, surface area, etc. If you use this
                        mode of running this software you will be asked to match your region
                        variables to standard Freesurfer region / measure labels presented to you
                        (so this program will end up being interactive. In future WIP we will add
                        the ability to export a json sidecar file with those mappings for
                        automated runs of future CSV files with the same set of variables.
  -subjid SUBJID, --subjid SUBJID
                        Subject identifier to be used in the NIDM files. Required with
                        -f/--seg_file. With -s/--subject_dir it defaults to the name of the
                        subject directory; set it when that directory is not named after the
                        subject
  -o OUTPUT, --output OUTPUT
                        Where to write a new NIDM file. With -s, the directory that gets one
                        <stats>_nidm.ttl (or .jsonld) per stats file; with -f or -csv, the output
                        file. Exactly one of -o/--output and -n/--nidm is required
  -n NIDM_FILE, --nidm NIDM_FILE
                        Existing NIDM file to add the stats to; it is rewritten in place (with -j
                        the result goes to <NIDM_FILE>.json instead). Exactly one of -o/--output
                        and -n/--nidm is required
  -j, --jsonld          If flag set then NIDM file will be written as JSONLD instead of TURTLE.
                        With -n the JSON-LD goes to <NIDM_FILE>.json. Not allowed with -s and -n
                        together
  -add_de, --add_de     If flag set then the data element dictionary is added to the NIDM file,
                        else it is written to a separate file, fs_cde.ttl, in the output directory
                        (or the directory of the -n NIDM file)
  -forcenidm, --forcenidm
                        With -n: if the NIDM file has no subjects at all, add a new subject agent
                        with the subject ID and attach the stats to it. If the file has subjects
                        and none matches the subject ID (compared without a leading "sub-" and
                        leading zeros), the run fails with or without this option
  -json_map JSON_MAP, --json_map JSON_MAP
                        If storing freesurfer segmentation data stored in a CSV file and you've
                        previously stored the variable to Freesurfer CDE mappings, this can be
                        reused. Otherwise this argument is ignored
  -acq_filename ACQ_FILENAME [ACQ_FILENAME ...], --acq_filename ACQ_FILENAME [ACQ_FILENAME ...]
                        With -s or -f and -n: file names (or paths) of the anatomical images the
                        stats were computed from (several for repeated runs). The stats are linked
                        to the input NIDM acquisitions with those file names if each name matches
                        exactly one of the subject's anatomical acquisitions, and left unlinked
                        otherwise. Without it the stats are linked only when the subject has a
                        single anatomical acquisition
```

### Updating the FreeSurfer Common Data Elements

This section describes how one can update the common data elements from FreeSurfer
as they update the names they are using for their elements.

```Python
from fsutils import collate_fs_items, create_fs_mapper
collate_fs_items('/path/to/statsdir', error_on_new_key=False)
create_fs_mapper()
```

This will add new structures to freesurfermap.json file, which you can edit to
improve connections to ontologies (e.g., by editing the `isAbout`).

```Python
create_fs_mapper()
```
