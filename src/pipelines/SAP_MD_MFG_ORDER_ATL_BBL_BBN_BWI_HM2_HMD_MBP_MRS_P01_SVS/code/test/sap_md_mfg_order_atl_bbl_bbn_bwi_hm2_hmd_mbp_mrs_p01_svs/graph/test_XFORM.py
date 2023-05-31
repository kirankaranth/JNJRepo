from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.graph.XFORM import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.config.ConfigStore import *


class XFORMTest(BaseTestCase):

    def test_all_strings(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/data/test_all_strings.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/data/test_all_strings.json',
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
              "ORDR_CRNCY",
              "ORDR_STS",
              "STS_REACHED_SO_FAR",
              "PH_ORDR_RELS",
              "PH_ORDR_CMPLT",
              "APPL",
              "COST_SHT",
              "OVHD_KEY",
              "PRCSG_GRP",
              "SEQ_NUM",
              "APPLT",
              "PRFT_CTR",
              "WRK_BRKDWN_STRC_ELMNT",
              "VAR_KEY",
              "RSLTS_ANAL_KEY",
              "REQ_CO_CD",
              "ITM_NUM_IN_SLS_ORDR",
              "PRDTN_PRCS",
              "MFG_ORDR_STTS_CD",
              "MFG_ORDR_STS_TXT",
              "ORDR_RTNG_NUM",
              "MRP_CNTRLLR_CD",
              "PRD_SPVSR_CD",
              "RTNG_GRP_CNTR_NUM",
              "RTNG_GRP_CD",
              "RTNG_TYP_CD",
              "RSRVTN_NUM",
              "BOM_ALT_NUM",
              "BOM_NUM",
              "BOM_CAT_CD",
              "MATL_NUM",
              "BASE_UOM",
              "APPL_OF_THE_TASK_LIST",
              "TASK_LIST_USG",
              "TASK_LIST_UOM",
              "CHG_NUM1",
              "RESP_PLNR_GRP_OR_DEPT",
              "MATL_NUM1",
              "BILL_OF_MATL_STS",
              "BASE_UNIT_OF_MEAS",
              "CHG_NUM2",
              "BOM_USG",
              "SCHDLNG_MRGN_KEY_FOR_FLOATS",
              "SCHDLNG_TYPE",
              "FLOAT_BEF_PRDTN",
              "FLOAT_AFTER_PRDTN",
              "RLSE_PER",
              "CHG_TO_SCHD_DT_IN",
              "ID_OF_THE_CAPY_RQR_REC",
              "PROJ_DEF",
              "INTRNL_CNTR1",
              "INTRNL_CNTR2",
              "CNTR_FOR_ADDL_CRITA",
              "INSP_LOT_NUM",
              "COST_VRNT_FOR_PLAN_COSTS",
              "COST_VRNT_FOR_ACTL_COSTS",
              "CMPLT_CNFRM_NUM_FOR_THE_OPR",
              "INTRNL_CNTR3",
              "CNFG",
              "OBJ_ID_OF_THE_RSRS1",
              "OBJ_ID_OF_THE_RSRS2",
              "NUM_OF_RESV",
              "ORDR_ITM_NUM",
              "LEFT_NODE_IN_CLCTV_ORDR",
              "RIGHT_NODE_OF_CLCTV_ORDR",
              "CNFRM_DEG_OF_PRCSG",
              "RTG_NUM_OF_OPS_IN_THE_ORDR",
              "GENL_CNTR_FOR_ORDR"
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
              "ORDR_CRNCY",
              "ORDR_STS",
              "STS_REACHED_SO_FAR",
              "PH_ORDR_RELS",
              "PH_ORDR_CMPLT",
              "APPL",
              "COST_SHT",
              "OVHD_KEY",
              "PRCSG_GRP",
              "SEQ_NUM",
              "APPLT",
              "PRFT_CTR",
              "WRK_BRKDWN_STRC_ELMNT",
              "VAR_KEY",
              "RSLTS_ANAL_KEY",
              "REQ_CO_CD",
              "ITM_NUM_IN_SLS_ORDR",
              "PRDTN_PRCS",
              "MFG_ORDR_STTS_CD",
              "MFG_ORDR_STS_TXT",
              "ORDR_RTNG_NUM",
              "MRP_CNTRLLR_CD",
              "PRD_SPVSR_CD",
              "RTNG_GRP_CNTR_NUM",
              "RTNG_GRP_CD",
              "RTNG_TYP_CD",
              "RSRVTN_NUM",
              "BOM_ALT_NUM",
              "BOM_NUM",
              "BOM_CAT_CD",
              "MATL_NUM",
              "BASE_UOM",
              "APPL_OF_THE_TASK_LIST",
              "TASK_LIST_USG",
              "TASK_LIST_UOM",
              "CHG_NUM1",
              "RESP_PLNR_GRP_OR_DEPT",
              "MATL_NUM1",
              "BILL_OF_MATL_STS",
              "BASE_UNIT_OF_MEAS",
              "CHG_NUM2",
              "BOM_USG",
              "SCHDLNG_MRGN_KEY_FOR_FLOATS",
              "SCHDLNG_TYPE",
              "FLOAT_BEF_PRDTN",
              "FLOAT_AFTER_PRDTN",
              "RLSE_PER",
              "CHG_TO_SCHD_DT_IN",
              "ID_OF_THE_CAPY_RQR_REC",
              "PROJ_DEF",
              "INTRNL_CNTR1",
              "INTRNL_CNTR2",
              "CNTR_FOR_ADDL_CRITA",
              "INSP_LOT_NUM",
              "COST_VRNT_FOR_PLAN_COSTS",
              "COST_VRNT_FOR_ACTL_COSTS",
              "CMPLT_CNFRM_NUM_FOR_THE_OPR",
              "INTRNL_CNTR3",
              "CNFG",
              "OBJ_ID_OF_THE_RSRS1",
              "OBJ_ID_OF_THE_RSRS2",
              "NUM_OF_RESV",
              "ORDR_ITM_NUM",
              "LEFT_NODE_IN_CLCTV_ORDR",
              "RIGHT_NODE_OF_CLCTV_ORDR",
              "CNFRM_DEG_OF_PRCSG",
              "RTG_NUM_OF_OPS_IN_THE_ORDR",
              "GENL_CNTR_FOR_ORDR"
            ),
            self.maxUnequalRowsToShow
        )

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

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/schema.json',
            'test/resources/data/sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs/graph/XFORM/out/data/test_unit_test_.json',
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
