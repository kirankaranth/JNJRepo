from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_hm2.graph.XFORM import *
from sap_md_matl_hm2.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/XFORM/out/data/test_unit_test_.json',
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
        dfgraph_SUTUR_USP_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/SUTUR_USP_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/SUTUR_USP_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.SUTUR_USP_LU import SUTUR_USP_LU
        SUTUR_USP_LU(self.spark, dfgraph_SUTUR_USP_LU)
        dfgraph_MAKTG_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/MAKTG_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/MAKTG_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.MAKTG_LU import MAKTG_LU
        MAKTG_LU(self.spark, dfgraph_MAKTG_LU)
        dfgraph_SUTUR_LEN_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/SUTUR_LEN_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/SUTUR_LEN_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.SUTUR_LEN_LU import SUTUR_LEN_LU
        SUTUR_LEN_LU(self.spark, dfgraph_SUTUR_LEN_LU)
        dfgraph_VTEXT_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/VTEXT_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/VTEXT_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.VTEXT_LU import VTEXT_LU
        VTEXT_LU(self.spark, dfgraph_VTEXT_LU)
        dfgraph_MTBEZ_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/MTBEZ_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/MTBEZ_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.MTBEZ_LU import MTBEZ_LU
        MTBEZ_LU(self.spark, dfgraph_MTBEZ_LU)
        dfgraph_STERILE_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/STERILE_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/STERILE_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.STERILE_LU import STERILE_LU
        STERILE_LU(self.spark, dfgraph_STERILE_LU)
        dfgraph_BRAVO_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/BRAVO_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/BRAVO_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.BRAVO_LU import BRAVO_LU
        BRAVO_LU(self.spark, dfgraph_BRAVO_LU)
        dfgraph_MAKTX_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/MAKTX_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/MAKTX_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.MAKTX_LU import MAKTX_LU
        MAKTX_LU(self.spark, dfgraph_MAKTX_LU)
        dfgraph_NDL_SLS_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/NDL_SLS_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/NDL_SLS_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.NDL_SLS_LU import NDL_SLS_LU
        NDL_SLS_LU(self.spark, dfgraph_NDL_SLS_LU)
        dfgraph_ATWTB_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/ATWTB_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/ATWTB_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.ATWTB_LU import ATWTB_LU
        ATWTB_LU(self.spark, dfgraph_ATWTB_LU)
        dfgraph_NDL_ALLOY_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/NDL_ALLOY_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/NDL_ALLOY_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.NDL_ALLOY_LU import NDL_ALLOY_LU
        NDL_ALLOY_LU(self.spark, dfgraph_NDL_ALLOY_LU)
        dfgraph_WGBEZx_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/WGBEZx_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/WGBEZx_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.WGBEZx_LU import WGBEZx_LU
        WGBEZx_LU(self.spark, dfgraph_WGBEZx_LU)
        dfgraph_OBJEK_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/OBJEK_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/OBJEK_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.OBJEK_LU import OBJEK_LU
        OBJEK_LU(self.spark, dfgraph_OBJEK_LU)
        dfgraph_NDL_COL_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/NDL_COL_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/NDL_COL_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.NDL_COL_LU import NDL_COL_LU
        NDL_COL_LU(self.spark, dfgraph_NDL_COL_LU)
        dfgraph_MSEHL_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/MSEHL_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/MSEHL_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.MSEHL_LU import MSEHL_LU
        MSEHL_LU(self.spark, dfgraph_MSEHL_LU)
        dfgraph_MAT_TYPE_LU = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_hm2/graph/MAT_TYPE_LU/schema.json',
            'test/resources/data/sap_md_matl_hm2/graph/MAT_TYPE_LU/data.json',
            "in0"
        )
        from sap_md_matl_hm2.graph.MAT_TYPE_LU import MAT_TYPE_LU
        MAT_TYPE_LU(self.spark, dfgraph_MAT_TYPE_LU)
