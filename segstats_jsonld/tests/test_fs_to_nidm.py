import pytest
import json
import numpy as np
from os.path import join, exists, abspath
from .. import mapping_data
from . import testdata
from .. import fs_to_nidm as s

datapath = mapping_data.__path__[0] + '/'
testdatap = abspath('segstats_jsonld/tests/testdata/')


def test_test_connection():
    """ smoke test to see whether this function tests internet connection"""

    # trying to ping a non-existing server should fail
    assert s.test_connection('albseirhnerjgel.com') == False
