import unittest

from test.sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.graph.test_NEW_FIELDS import *
from test.sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.graph.test_SET_FIELD_ORDER_REFORMAT import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
