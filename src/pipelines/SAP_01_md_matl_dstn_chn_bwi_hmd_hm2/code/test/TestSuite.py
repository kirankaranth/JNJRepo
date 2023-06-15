import unittest

from test.sap_md_matl_dstn_chn_hmd.graph.test_MANDT_FILTER_tvmst import *
from test.sap_md_matl_dstn_chn_hmd.graph.test_MANDT_FILTER_t179 import *
from test.sap_md_matl_dstn_chn_hmd.graph.test_MANDT_FILTER_tvms import *
from test.sap_md_matl_dstn_chn_hmd.graph.test_MANDT_FILTER_t179t import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
