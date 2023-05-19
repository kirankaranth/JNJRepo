import unittest

from test.sap_01_md_matl_loc_hmd.graph.test_NEW_FIELDS import *
from test.sap_01_md_matl_loc_hmd.graph.test_SET_FIELD_ORDER_REFORMAT import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
