from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_valut.graph.XFORM import *
from sap_md_matl_valut.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_matl_valut/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_matl_valut/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MATL_NUM", "PRC_CNTL_IND", "TMST"),
            dfOutComputed.select("MATL_NUM", "PRC_CNTL_IND", "TMST"),
            self.maxUnequalRowsToShow
        )

    def test_decimal(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_matl_valut/graph/XFORM/in0/data/test_decimal.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_matl_valut/graph/XFORM/out/data/test_decimal.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("TOT_STK_QTY"), dfOutComputed.select("TOT_STK_QTY"), self.maxUnequalRowsToShow)

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
        dfgraph_MEINS_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut/graph/MEINS_LU/schema.json',
            'test/resources/data/sap_md_matl_valut/graph/MEINS_LU/data.json',
            "in0"
        )
        from sap_md_matl_valut.graph.MEINS_LU import MEINS_LU
        MEINS_LU(self.spark, dfgraph_MEINS_LU)
