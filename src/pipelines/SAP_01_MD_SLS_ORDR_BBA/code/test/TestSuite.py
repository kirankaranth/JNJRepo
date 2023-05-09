import unittest

from test.sap_01_md_sls_ordr_bba_mrs_fsn.graph.test_NEW_FIELDS_TRANSFORMATION_1 import *
from test.sap_01_md_sls_ordr_bba_mrs_fsn.graph.test_NEW_FIELDS_TRANSFORMATION import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
