import unittest

from test.sap_md_matl_inv_atl.graph.test_SchemaTransform_4_MSLB import *
from test.sap_md_matl_inv_atl.graph.test_SchemaTransform_2_MCHB import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
