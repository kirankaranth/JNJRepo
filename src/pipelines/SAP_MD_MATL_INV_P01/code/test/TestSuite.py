import unittest

from test.sap_md_matl_inv_p01.graph.test_SchemaTransform_5_MSLB import *
from test.sap_md_matl_inv_p01.graph.test_SchemaTransform_2_MCHB import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
