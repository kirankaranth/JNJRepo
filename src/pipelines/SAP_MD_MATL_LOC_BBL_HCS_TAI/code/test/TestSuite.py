import unittest

from test.sap_md_matl_loc_bbl_hcs_tai.graph.test_NEW_FIELDS import *
from test.sap_md_matl_loc_bbl_hcs_tai.graph.test_SET_FIELD_ORDER_REFORMAT import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
