from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_mbp.graph.XFORM import *
from sap_md_matl_mbp.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mbp/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_matl_mbp/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mbp/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_matl_mbp/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("BASE_UOM_CD"), dfOutComputed.select("BASE_UOM_CD"), self.maxUnequalRowsToShow)

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
        dfgraph_MAKTX_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mbp/graph/MAKTX_LU/schema.json',
            'test/resources/data/sap_md_matl_mbp/graph/MAKTX_LU/data.json',
            "in0"
        )
        from sap_md_matl_mbp.graph.MAKTX_LU import MAKTX_LU
        MAKTX_LU(self.spark, dfgraph_MAKTX_LU)
        dfgraph_MTBEZ_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mbp/graph/MTBEZ_LU/schema.json',
            'test/resources/data/sap_md_matl_mbp/graph/MTBEZ_LU/data.json',
            "in0"
        )
        from sap_md_matl_mbp.graph.MTBEZ_LU import MTBEZ_LU
        MTBEZ_LU(self.spark, dfgraph_MTBEZ_LU)
        dfgraph_WGBEZx_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_mbp/graph/WGBEZx_LU/schema.json',
            'test/resources/data/sap_md_matl_mbp/graph/WGBEZx_LU/data.json',
            "in0"
        )
        from sap_md_matl_mbp.graph.WGBEZx_LU import WGBEZx_LU
        WGBEZx_LU(self.spark, dfgraph_WGBEZx_LU)
