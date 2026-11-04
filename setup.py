#!/usr/bin/env python

from setuptools import setup
from setuptools import find_packages
from os.path import join as opj
from os.path import dirname


def get_version():
    """Load version only
    """
    with open(opj(dirname(__file__), "segstats_jsonld", "__init__.py")) as f:
        version_lines = list(filter(lambda x: x.startswith("__version__"), f))
    assert len(version_lines) == 1
    return version_lines[0].split("=")[1].strip(" '\"\t\n")


# extension version
version = get_version()
PACKAGES = find_packages()

README = opj(dirname(__file__), "README.md")
try:
    import pypandoc

    long_description = pypandoc.convert(README, "rst")
except (ImportError, OSError) as exc:
    print(
        "WARNING: pypandoc failed to import or threw an error while converting"
        " README.md to RST: %r  .md version will be used as is" % exc
    )
    long_description = open(README).read()

# Metadata
setup(
    name="segstats_jsonld",
    version=version,
    description="Freesurfer metadata extraction & conversion to jsonld",
    long_description=long_description,
    author="David Keator",
    author_email="dbkeator@uci.edu",
    url="https://github.com/ReproNim/segstats_jsonld",
    packages=PACKAGES,
    install_requires=[
        # The converter runs on PyNIDM 4.5 and 5 (segstats_jsonld/_pynidm.py
        # picks the API). Applications that install it pin the exact version.
        "pynidm>=4.5.0,<6",
        "pandas",
        # PyNIDM 4.5 imports prov.graph without declaring its "graph" extra.
        "prov[graph]",
        "xlrd",
        "rdflib>=6.0.0",
        # NOTE: rdflib-jsonld is intentionally NOT listed here. It is deprecated --
        # JSON-LD support moved into rdflib itself in 6.0. It still gets installed
        # transitively (pynidm -> funowl -> rdflib-shim -> rdflib-jsonld==0.6.1),
        # but rdflib-shim exists precisely to keep it inert under rdflib>=6, so
        # that is fine. We simply don't declare a direct dependency on it.
    ],  # Add requirements as necessary
    include_package_data=True,
    extras_require={
        "devel-docs": [
            # for converting README.md -> .rst for long description
            "pypandoc"
        ]
    },
    entry_points={
        "console_scripts": [
            "segstats2nidm=segstats_jsonld.fs_to_nidm:main"  # this is where the console entry points are defined
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],  # Change if necessary
)
