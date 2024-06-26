from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.graph\
    .NEW_FIELDS_RENAME_FORMAT import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.config.ConfigStore import *


class NEW_FIELDS_RENAME_FORMATTest(BaseTestCase):

    def test_trim_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_trim_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_trim_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("PRC_CNTL_IND", "VALUT_CLS_CD"),
            dfOutComputed.select("PRC_CNTL_IND", "VALUT_CLS_CD"),
            self.maxUnequalRowsToShow
        )

    def test_decimal_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_decimal_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_decimal_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("MVG_AVG_PRC_AMT"),
            dfOutComputed.select("MVG_AVG_PRC_AMT"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_timestamp_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_timestamp_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(dfOut.select("_l0_upt_"), dfOutComputed.select("_l0_upt_"), self.maxUnequalRowsToShow)

    def test_decimal_test_2_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_decimal_test_2_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_decimal_test_2_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("TOT_STK_QTY", "TOT_VAL_AMT", "MVG_AVG_PRC_AMT", "PRC_AMT", "PRC_UNIT_NBR"),
            dfOutComputed.select("TOT_STK_QTY", "TOT_VAL_AMT", "MVG_AVG_PRC_AMT", "PRC_AMT", "PRC_UNIT_NBR"),
            self.maxUnequalRowsToShow
        )

    def test_lu_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/in0/data/test_lu_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/NEW_FIELDS_RENAME_FORMAT/out/data/test_lu_test_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS_RENAME_FORMAT(self.spark, dfIn0)
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
        dfgraph_LU_SAP_MARA = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/LU_SAP_MARA/schema.json',
            'test/resources/data/sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd/graph/LU_SAP_MARA/data.json',
            "in0"
        )
        from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.graph.LU_SAP_MARA import (
            LU_SAP_MARA
        )
        LU_SAP_MARA(self.spark, dfgraph_LU_SAP_MARA)
