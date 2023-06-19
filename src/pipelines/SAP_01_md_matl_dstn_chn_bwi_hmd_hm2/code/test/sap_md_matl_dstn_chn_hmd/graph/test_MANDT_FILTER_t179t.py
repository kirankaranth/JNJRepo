from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_dstn_chn_hmd.graph.MANDT_FILTER_t179t import *
from sap_md_matl_dstn_chn_hmd.config.ConfigStore import *


class MANDT_FILTER_t179tTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/MANDT_FILTER_t179t/in0/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/MANDT_FILTER_t179t/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/MANDT_FILTER_t179t/out/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/MANDT_FILTER_t179t/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = MANDT_FILTER_t179t(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("PRODH", "VTEXT"),
            dfOutComputed.select("PRODH", "VTEXT"),
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
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_t179t/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_t179t/data.json',
            "in0"
        )
        from sap_md_matl_dstn_chn_hmd.graph.LU_SAP_t179t import LU_SAP_t179t
        LU_SAP_t179t(self.spark, dfgraph_LU_SAP_t179t)
        dfgraph_LU_SAP_tvmst = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_tvmst/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_tvmst/data.json',
            "in0"
        )
        from sap_md_matl_dstn_chn_hmd.graph.LU_SAP_tvmst import LU_SAP_tvmst
        LU_SAP_tvmst(self.spark, dfgraph_LU_SAP_tvmst)
        dfgraph_LU_SAP_tvms = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_tvms/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_tvms/data.json',
            "in0"
        )
        from sap_md_matl_dstn_chn_hmd.graph.LU_SAP_tvms import LU_SAP_tvms
        LU_SAP_tvms(self.spark, dfgraph_LU_SAP_tvms)
        dfgraph_LU_SAP_t179 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_t179/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_t179/data.json',
            "in0"
        )
        from sap_md_matl_dstn_chn_hmd.graph.LU_SAP_t179 import LU_SAP_t179
        LU_SAP_t179(self.spark, dfgraph_LU_SAP_t179)
        dfgraph_LU_SAP_tptmt = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_tptmt/schema.json',
            'test/resources/data/sap_md_matl_dstn_chn_hmd/graph/LU_SAP_tptmt/data.json',
            "in0"
        )
        from sap_md_matl_dstn_chn_hmd.graph.LU_SAP_tptmt import LU_SAP_tptmt
        LU_SAP_tptmt(self.spark, dfgraph_LU_SAP_tptmt)
