import unittest

from test.sap_md_btch.graph.test_SchemaTransform_MCH1 import *
from test.sap_md_btch.graph.test_SchemaTransform_MCHA import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
