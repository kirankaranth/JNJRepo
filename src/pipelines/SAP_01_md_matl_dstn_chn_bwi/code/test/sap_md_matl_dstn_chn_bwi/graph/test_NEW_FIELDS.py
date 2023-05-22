from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_dstn_chn_bwi.graph.NEW_FIELDS import *
from sap_md_matl_dstn_chn_bwi.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_bwi/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_bwi/graph/NEW_FIELDS/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_bwi/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_bwi/graph/NEW_FIELDS/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("_deleted_", "_pk_", "SRC_SYS_CD", "MATL_NUM", "SL_ORG_NUM", "DSTR_CHNL_CD", "_pk_md5_"),
            dfOutComputed.select(
              "_deleted_",
              "_pk_",
              "SRC_SYS_CD",
              "MATL_NUM",
              "SL_ORG_NUM",
              "DSTR_CHNL_CD",
              "_pk_md5_"
            ),
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
        dfgraph_LU_SAP_t179t = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_bwi/graph/LU_SAP_t179t/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_bwi/graph/LU_SAP_t179t/data.json',
            "in0"
        )
        from sap_md_matl_dstn_chn_bwi.graph.LU_SAP_t179t import LU_SAP_t179t
        LU_SAP_t179t(self.spark, dfgraph_LU_SAP_t179t)
