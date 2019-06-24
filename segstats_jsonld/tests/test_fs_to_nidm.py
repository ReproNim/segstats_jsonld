import pytest
import numpy as np
from .. import fs_to_nidm as s


def test_remap2json():
    """

    """
    xlsx_file = '../mapping_data/ReproNimCDEs.xlsx'
    aparc_example = 'testdata/rh.aparc.stats'
    asag_example = 'testdata/aseg.stats'

    # check whether non-scraping works
    for examplefile in [aparc_example, asag_example]:
        header, tableinfo, measures, biggie = s.remap2json(xlsx_file,
                                                             examplefile,
                                                             noscrape=True
                                                             )
        # assert that definitions are not assigned
        for d in ['Anatomy', 'Measures']:
            for key, value in biggie[d].items():
                for k, v in biggie[d][key].items():
                    if k == 'definition':
                        assert v == ""
