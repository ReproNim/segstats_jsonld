import pytest
import json
import numpy as np
from os.path import join, exists
from .. import mapping_data
from . import testdata
from .. import fs_to_nidm as s


datapath = mapping_data.__path__._path[0] + '/'
testdatap = testdata.__path__._path[0] + '/'

def test_remap2json():
    """
    Test some basic functionality of remap2json
    """

    xlsx_file = join(datapath, 'ReproNimCDEs.xlsx')
    aparc_example = join(testdatap, 'rh.aparc.stats')
    asag_example = join(testdatap, 'aseg.stats')

    # smoke test whether non-scraping works
    for examplefile in [aparc_example, asag_example]:
        header, tableinfo, measures, biggie = s.remap2json(xlsx_file,
                                                             examplefile,
                                                             noscrape=True
                                                             )
        # check that we have the overall keys assigned
        assert "Anatomy" in biggie.keys()
        assert "Measures" in biggie.keys()
        # assert that definitions are not assigned if there is no base-mapper
        if exists(join(datapath, 'jsonmap.json')):
            # TODO
            continue
        else:
            # if there is no remapping file present yet:
            for d in ['Anatomy', 'Measures']:
                for key, value in biggie[d].items():
                    for k, v in biggie[d][key].items():
                        if k == 'definition':
                            assert v == ""

    # smoke test whether scraping works
    # TODO: This takes very long, will have to think of sth faster
    if s.test_connection():
        for examplefile in [aparc_example, asag_example]:
            header, tableinfo, measures, biggie = s.remap2json(xlsx_file,
                                                               examplefile,
                                                               noscrape=False,
                                                               force_update=True
                                                                )
            for d in ['Anatomy', 'Measures']:
                for key, value in biggie[d].items():
                    for k, v in biggie[d][key].items():
                        if k == 'definition':
                            assert v != np.nan

            # check the correct definitions are retrieved for some example terms
            if examplefile == aparc_example:
                assert biggie['Anatomy']['Left-Lateral-Ventricle']['definition'] == \
                    'left lateral ventricle cerebral spinal fluid volume'
                assert biggie['Measures']['SurfArea']['measureOf'] == 'http://uri.interlex.org/base/ilx_0738271#'

            elif examplefile == asag_example:
                assert biggie['Measures']['NVoxels']['measureOf'] == 'http://uri.interlex.org/base/ilx_0105536#'

    # check whether we save valid json
    with open(join(testdatap, 'tmpjson.json'), 'w') as f:
        json.dump(biggie, f, indent=4)
    # parsing this will fail if json is not valid
    with open(join(testdatap, 'tmpjson.json'), 'r') as j:
        json.load(j)


def test_test_connection():
    """ smoke test to see whether this function tests internet connection"""

    # trying to ping a non-existing server should fail
    assert s.test_connection('albseirhnerjgel.com') == False