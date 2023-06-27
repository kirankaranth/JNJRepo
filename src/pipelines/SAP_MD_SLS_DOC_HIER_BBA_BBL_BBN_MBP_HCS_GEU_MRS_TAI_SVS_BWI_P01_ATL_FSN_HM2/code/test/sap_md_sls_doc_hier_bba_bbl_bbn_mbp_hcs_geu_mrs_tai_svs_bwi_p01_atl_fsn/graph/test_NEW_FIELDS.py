from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn.graph.NEW_FIELDS import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn.config.ConfigStore import *


class NEW_FIELDSTest(BaseTestCase):

    def test_pk(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/data/test_pk.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/data/test_pk.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "SRC_SYS_CD",
              "CO_CD",
              "PREV_DOC_NUM",
              "PREV_DOC_LINE_NBR",
              "SUBSQ_DOC_NUM",
              "SUBSQ_DOC_LINE_NBR",
              "SUBSQ_DOC_CAT_CD",
              "PREV_DOC_TYPE_CD",
              "PREV_DOC_CAT_CD"
            ),
            dfOutComputed.select(
              "SRC_SYS_CD",
              "CO_CD",
              "PREV_DOC_NUM",
              "PREV_DOC_LINE_NBR",
              "SUBSQ_DOC_NUM",
              "SUBSQ_DOC_LINE_NBR",
              "SUBSQ_DOC_CAT_CD",
              "PREV_DOC_TYPE_CD",
              "PREV_DOC_CAT_CD"
            ),
            self.maxUnequalRowsToShow
        )

    def test_trim(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/data/test_trim.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/data/test_trim.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("CRNCY_CD", "MATL_NUM", "REQ_TYPE_CD", "PLNG_TYPE_CD", "LVL_CD"),
            dfOutComputed.select("CRNCY_CD", "MATL_NUM", "REQ_TYPE_CD", "PLNG_TYPE_CD", "LVL_CD"),
            self.maxUnequalRowsToShow
        )

    def test_decimal(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/data/test_decimal.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/data/test_decimal.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("REF_QTY", "GRS_WT_MEAS"),
            dfOutComputed.select("REF_QTY", "GRS_WT_MEAS"),
            self.maxUnequalRowsToShow
        )

    def test_timestamp_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/data/test_timestamp_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/data/test_timestamp_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(dfOut.select("CRT_DTTM"), dfOutComputed.select("CRT_DTTM"), self.maxUnequalRowsToShow)

    def test_all_trims_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/in0/data/test_all_trims_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/schema.json',
            'test/resources/data/sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn/graph/NEW_FIELDS/out/data/test_all_trims_.json',
            'out'
        )
        dfOutComputed = NEW_FIELDS(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select(
              "BASE_UOM_CD",
              "REF_AMT",
              "CRNCY_CD",
              "MATL_NUM",
              "REQ_TYPE_CD",
              "LVL_CD",
              "WHSE_CD",
              "BILL_CAT_CD",
              "WT_UOM_CD",
              "VOL_UOM_CD",
              "SLS_UOM_CD",
              "SPL_STK_TYPE_CD",
              "SPL_STK_NUM",
              "GM_STS_CD",
              "QTY_CONV_CD",
              "MATL_MVMT_YR",
              "QTY_CALC_POS_NGTV",
              "ID_MM_WM_TFR_ORDR_CNFRM",
              "MVMT_TYPE",
              "BILL_INVC_PLAN_NUM",
              "ITM_FOR_BILL_INVC_PLAN_PMT_CRD",
              "IN_INV_MGMT_ACT",
              "LOGL_SYS"
            ),
            dfOutComputed.select(
              "BASE_UOM_CD",
              "REF_AMT",
              "CRNCY_CD",
              "MATL_NUM",
              "REQ_TYPE_CD",
              "LVL_CD",
              "WHSE_CD",
              "BILL_CAT_CD",
              "WT_UOM_CD",
              "VOL_UOM_CD",
              "SLS_UOM_CD",
              "SPL_STK_TYPE_CD",
              "SPL_STK_NUM",
              "GM_STS_CD",
              "QTY_CONV_CD",
              "MATL_MVMT_YR",
              "QTY_CALC_POS_NGTV",
              "ID_MM_WM_TFR_ORDR_CNFRM",
              "MVMT_TYPE",
              "BILL_INVC_PLAN_NUM",
              "ITM_FOR_BILL_INVC_PLAN_PMT_CRD",
              "IN_INV_MGMT_ACT",
              "LOGL_SYS"
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
