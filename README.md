# Making Freesurfer FAIR
Script to Export Freesurfer-based Parcellation/Segmentation Stats and Provenance as JSON-LD and NIDM
## Project Description
This project ultimately aims to facilitate both query and analysis of parcellation/segmentation based regional statistics across popular softwares such as [Freesurfer](https://surfer.nmr.mgh.harvard.edu/), [FSL](https://fsl.fmrib.ox.ac.uk/fsl/fslwiki), and [ANTS](http://stnava.github.io/ANTs/). Currently each software produces its own output format and brain region labels are specific to the atlas used in generating the regional statistics.  This makes life difficult when trying to search for "nucleaus accumbens" volume, for example, across the different software products.  Further, knowing which version of the software tool used and what atlas and version of the atlas in a structured representation facilitating query is lacking.  To this end we propose augmenting the various segmentation tools with scripts that will: (1) map atlas-specific anatomical nomeclature to anatomical concepts hosted in terminology resources (e.g. InterLex); (2) capture better structured provenance about the input image(s) and the atlases used for the segmentation; (3) export the segmentation results and the provenance as either [JSON-LD](https://json-ld.org/), [NIDM](http://nidm.nidash.org/) which can then link the derived data to broader records of the original project metadata, or as an additional component of a BIDS derivative.

We aim to tackle this problem in steps.  For this hackathon project we'll be focusing on conversion from Freesurfer's [mri_segstats](https://surfer.nmr.mgh.harvard.edu/fswiki/mri_segstat) program output along with some additional parsing/conversion of Freesurfer log files. The conversion is driven by a function which queries InterLex and develops a JSON structure which defines the atlas terminology and the measures being output.

## Skills required to participate
Python and structural neuroimaging experience.  If one has experience with [rdflib](https://github.com/RDFLib/rdflib) or [PROV](https://github.com/trungdong/prov) that would also be helpful.

## Participants
- David Keator
- Adina Wagner
- Jeffrey Grethe
- Satra Gosh
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
