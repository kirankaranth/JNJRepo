import unittest

from test.sap_01_md_matl_inv.graph.test_SchemaTransform_1_MARD import *
from test.sap_01_md_matl_inv.graph.test_SchemaTransform_2_MCHB import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
