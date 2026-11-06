"""The parts of PyNIDM this converter uses, from either PyNIDM line.

PyNIDM 5 removed the prov-toolbox API (``nidm.core``, ``nidm.experiment``)
and moved everything under ``nidm.linkml``. This module imports from
``nidm.linkml`` when it is installed and from the 4.x API otherwise, so the
converter runs unchanged on both.

The constants used here are rdflib Namespaces and URIRefs in both lines,
except ``NIDM_SUBJECTID``: a prov QualifiedName in 4.x and a URIRef in 5.
``NIDM_SUBJECTID_URI`` and ``NIDM_SUBJECTID_LOCALPART`` give its URI string
and local name either way, so callers never touch ``.uri``/``.localpart``.
"""

import importlib.util

# Chosen by which package is installed, not by catching ImportError: an
# import error inside an installed PyNIDM 5 must surface as itself, not as a
# misleading "No module named 'nidm.core'" from the 4.x fallback.
if importlib.util.find_spec("nidm.linkml") is not None:
    from nidm.linkml.core import constants as _constants
    from nidm.linkml.core import namespaces as _namespaces
    from nidm.linkml.core.constants import DD
    from nidm.linkml.experiment.core import getUUID
    from nidm.linkml.experiment.utils import DD_to_nidm, tupleKeysToSimpleKeys

    class _Constants:
        """Attribute lookup over nidm.linkml.core's constants and namespaces,
        which together hold what nidm.core.Constants held."""

        def __getattr__(self, name):
            for module in (_constants, _namespaces):
                if hasattr(module, name):
                    return getattr(module, name)
            raise AttributeError(f"PyNIDM 5 has no constant {name!r}")

    Constants = _Constants()
    NIDM_SUBJECTID_URI = str(Constants.NIDM_SUBJECTID)
    PYNIDM_MAJOR = 5
else:
    from nidm.core import Constants
    from nidm.core.Constants import DD
    from nidm.experiment.Core import getUUID
    from nidm.experiment.Utils import DD_to_nidm, tupleKeysToSimpleKeys

    NIDM_SUBJECTID_URI = str(Constants.NIDM_SUBJECTID.uri)
    PYNIDM_MAJOR = 4

NIDM_SUBJECTID_LOCALPART = NIDM_SUBJECTID_URI.rsplit("/", 1)[-1].rsplit("#", 1)[-1]

__all__ = [
    "Constants",
    "DD",
    "DD_to_nidm",
    "NIDM_SUBJECTID_LOCALPART",
    "NIDM_SUBJECTID_URI",
    "PYNIDM_MAJOR",
    "getUUID",
    "tupleKeysToSimpleKeys",
]
