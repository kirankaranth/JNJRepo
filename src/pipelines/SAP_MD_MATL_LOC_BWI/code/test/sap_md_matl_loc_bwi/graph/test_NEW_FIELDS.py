from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_loc_bwi.graph.NEW_FIELDS import *
from sap_md_matl_loc_bwi.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bwi/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_matl_loc_bwi/graph/NEW_FIELDS/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bwi/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_matl_loc_bwi/graph/NEW_FIELDS/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("PRCTR_CD", "LD_GRP_CD", "COST_LOT_SIZE_VAL"),
            dfOutComputed.select("PRCTR_CD", "LD_GRP_CD", "COST_LOT_SIZE_VAL"),
            self.maxUnequalRowsToShow
        )

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
        dfgraph_LU_SAP_T141T = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bwi/graph/LU_SAP_T141T/schema.json',
            'test/resources/data/sap_md_matl_loc_bwi/graph/LU_SAP_T141T/data.json',
            "in0"
        )
        from sap_md_matl_loc_bwi.graph.LU_SAP_T141T import LU_SAP_T141T
        LU_SAP_T141T(self.spark, dfgraph_LU_SAP_T141T)
        dfgraph_LU_SAP_T024 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_loc_bwi/graph/LU_SAP_T024/schema.json',
            'test/resources/data/sap_md_matl_loc_bwi/graph/LU_SAP_T024/data.json',
            "in0"
        )
        from sap_md_matl_loc_bwi.graph.LU_SAP_T024 import LU_SAP_T024
        LU_SAP_T024(self.spark, dfgraph_LU_SAP_T024)
