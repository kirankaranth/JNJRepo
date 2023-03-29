from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_01_md_matl_inv.graph.TRANSFORM import *
from jde_01_md_matl_inv.config.ConfigStore import *


class TRANSFORMTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/in0/schema.json',
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/out/schema.json',
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = TRANSFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("MATL_NUM"), dfOutComputed.select("MATL_NUM"), self.maxUnequalRowsToShow)

    def test_unit_test_1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/in0/schema.json',
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/in0/data/test_unit_test_1.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/out/schema.json',
            'test/resources/data/jde_01_md_matl_inv/graph/TRANSFORM/out/data/test_unit_test_1.json',
            'out'
        )
        dfOutComputed = TRANSFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("MATL_NUM"), dfOutComputed.select("MATL_NUM"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
