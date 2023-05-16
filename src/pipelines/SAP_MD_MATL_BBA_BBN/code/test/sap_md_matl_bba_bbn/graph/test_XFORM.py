from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_bba_bbn.graph.XFORM import *
from sap_md_matl_bba_bbn.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/XFORM/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CHG_BY"), dfOutComputed.select("CHG_BY"), self.maxUnequalRowsToShow)

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
        dfgraph_T134T_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/T134T_LU/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/T134T_LU/data.json',
            "in0"
        )
        from sap_md_matl_bba_bbn.graph.T134T_LU import T134T_LU
        T134T_LU(self.spark, dfgraph_T134T_LU)
        dfgraph_MAT_SPEC_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/MAT_SPEC_LU/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/MAT_SPEC_LU/data.json',
            "in0"
        )
        from sap_md_matl_bba_bbn.graph.MAT_SPEC_LU import MAT_SPEC_LU
        MAT_SPEC_LU(self.spark, dfgraph_MAT_SPEC_LU)
        dfgraph_MAKTX_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/MAKTX_LU/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/MAKTX_LU/data.json',
            "in0"
        )
        from sap_md_matl_bba_bbn.graph.MAKTX_LU import MAKTX_LU
        MAKTX_LU(self.spark, dfgraph_MAKTX_LU)
        dfgraph_WGBEZx_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/WGBEZx_LU/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/WGBEZx_LU/data.json',
            "in0"
        )
        from sap_md_matl_bba_bbn.graph.WGBEZx_LU import WGBEZx_LU
        WGBEZx_LU(self.spark, dfgraph_WGBEZx_LU)
        dfgraph_SPEC_VER_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_bba_bbn/graph/SPEC_VER_LU/schema.json',
            'test/resources/data/sap_md_matl_bba_bbn/graph/SPEC_VER_LU/data.json',
            "in0"
        )
        from sap_md_matl_bba_bbn.graph.SPEC_VER_LU import SPEC_VER_LU
        SPEC_VER_LU(self.spark, dfgraph_SPEC_VER_LU)
