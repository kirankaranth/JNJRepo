from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.graph.XFORM import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_first_15_strings(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/data/test_first_15_strings.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/data/test_first_15_strings.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "MFG_ORDR_TYP_CD",
              "MFG_ORDR_NUM",
              "DEL_IND",
              "OBJECT_NUMBER",
              "PLNT_CD",
              "ORDR_CAT",
              "REF_ORDR_NUM",
              "ENT_BY",
              "LAST_CHG_BY",
              "DESC",
              "LONG_TEXT_EXISTS",
              "CO_CD",
              "BUSN_AREA",
              "CNTL_AREA",
              "COST_CLCT_KEY",
              "ORDR_CRNCY"
            ),
            dfOutComputed.select(
              "MFG_ORDR_TYP_CD",
              "MFG_ORDR_NUM",
              "DEL_IND",
              "OBJECT_NUMBER",
              "PLNT_CD",
              "ORDR_CAT",
              "REF_ORDR_NUM",
              "ENT_BY",
              "LAST_CHG_BY",
              "DESC",
              "LONG_TEXT_EXISTS",
              "CO_CD",
              "BUSN_AREA",
              "CNTL_AREA",
              "COST_CLCT_KEY",
              "ORDR_CRNCY"
            ),
            self.maxUnequalRowsToShow
        )

    def test_datetime_test(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/data/test_datetime_test.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/data/test_datetime_test.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CHG_DTTM"), dfOutComputed.select("CHG_DTTM"), self.maxUnequalRowsToShow)

    def test_decimal_test(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/data/test_decimal_test.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/data/test_decimal_test.json',
            'out'
        )
        dfOutComputed = XFORM(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CNFRMD_SCRP_QTY"),
            dfOutComputed.select("CNFRMD_SCRP_QTY"),
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
