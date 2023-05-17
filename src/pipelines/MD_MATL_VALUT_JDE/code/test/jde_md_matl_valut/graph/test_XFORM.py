from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from jde_md_matl_valut.graph.XFORM import *
from jde_md_matl_valut.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/XFORM/out/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("TOT_VAL_AMT"), dfOutComputed.select("TOT_VAL_AMT"), self.maxUnequalRowsToShow)

    def test_lookup(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/XFORM/in0/data/test_lookup.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/XFORM/out/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/XFORM/out/data/test_lookup.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("BASE_UOM_CD", "VALUT_CLS_CD", "MATL_NUM"),
            dfOutComputed.select("BASE_UOM_CD", "VALUT_CLS_CD", "MATL_NUM"),
            self.maxUnequalRowsToShow
        )

    def test_decimal(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/XFORM/in0/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/XFORM/in0/data/test_decimal.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/XFORM/out/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/XFORM/out/data/test_decimal.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("PRC_AMT"), dfOutComputed.select("PRC_AMT"), self.maxUnequalRowsToShow)

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
        dfgraph_F4101_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/jde_md_matl_valut/graph/F4101_LU/schema.json',
            'test/resources/data/jde_md_matl_valut/graph/F4101_LU/data.json',
            "in0"
        )
        from jde_md_matl_valut.graph.F4101_LU import F4101_LU
        F4101_LU(self.spark, dfgraph_F4101_LU)
