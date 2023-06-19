from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_bba_fsn_tai.config.ConfigStore import *
from sap_md_mfg_order_bba_fsn_tai.udfs.UDFs import *

def SELECT_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MFG_ORDR_TYP_CD"), 
        col("MFG_ORDR_NUM"), 
        col("CHG_DTTM"), 
        col("CRTD_DTTM"), 
        col("DEL_IND"), 
        col("OBJECT_NUMBER"), 
        col("PLNT_CD"), 
        col("ORDR_CAT"), 
        col("REF_ORDR_NUM"), 
        col("ENT_BY"), 
        col("LAST_CHG_BY"), 
        col("DESC"), 
        col("LONG_TEXT_EXISTS"), 
        col("CO_CD"), 
        col("BUSN_AREA"), 
        col("CNTL_AREA"), 
        col("COST_CLCT_KEY"), 
        col("ORDR_CRNCY"), 
        col("ORDR_STS"), 
        col("LAST_STS_CHG_DTTM"), 
        col("STS_REACHED_SO_FAR"), 
        col("PH_ORDR_RELS"), 
        col("PH_ORDR_CMPLT"), 
        col("PLAN_CMPLT_DTTM"), 
        col("PLAN_CLS_DTTM"), 
        col("RLSE_DTTM"), 
        col("TECH_CMPLT_DTTM"), 
        col("CLSE_DTTM"), 
        col("OBJ_ID"), 
        col("USG_OF_THE_COND_TBL"), 
        col("APPL"), 
        col("COST_SHT"), 
        col("OVHD_KEY"), 
        col("PRCSG_GRP"), 
        col("SEQ_NUM"), 
        col("APPLT"), 
        col("EST_TOT_COSTS_OF_ORDR"), 
        col("APPL_DTTM"), 
        col("WRK_STRT_DTTM"), 
        col("END_OF_WRK_DTTM"), 
        col("PRFT_CTR"), 
        col("WRK_BRKDWN_STRC_ELMNT"), 
        col("VAR_KEY"), 
        col("RSLTS_ANAL_KEY"), 
        col("REQ_CO_CD"), 
        col("ITM_NUM_IN_SLS_ORDR"), 
        col("PRDTN_PRCS"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_deleted_"), 
        col("MFG_ORDR_STTS_CD"), 
        col("ORDR_RTNG_NUM"), 
        col("MRP_CNTRLLR_CD"), 
        col("PRD_SPVSR_CD"), 
        col("ACT_RLSE_DTTM"), 
        col("PLAN_RLSE_DTTM"), 
        col("SCH_REL_DTTM"), 
        col("PRDTN_END_DTTM"), 
        col("END_DTTM"), 
        col("SCHD_END_DTTM"), 
        col("ACT_STRT_DTTM"), 
        col("STRT_DTTM"), 
        col("SCHD_STRT_DTTM"), 
        col("CNFRMD_SCRP_QTY"), 
        col("CNFRMD_YLD_QTY"), 
        col("RTNG_GRP_CNTR_NUM"), 
        col("RTNG_GRP_CD"), 
        col("RTNG_TYP_CD"), 
        col("RSRVTN_NUM"), 
        col("BOM_ALT_NUM"), 
        col("BOM_NUM"), 
        col("BOM_CAT_CD"), 
        col("MATL_NUM"), 
        col("TOT_ORDR_QTY"), 
        col("TOT_SCRAP_QTY_IN_ORDR"), 
        col("BASE_UOM"), 
        col("APPL_OF_THE_TASK_LIST"), 
        col("TASK_LIST_USG"), 
        col("MAX_LOT_SIZE"), 
        col("TASK_LIST_UOM"), 
        col("MIN_LOT_SIZE"), 
        col("CHG_NUM1"), 
        col("RESP_PLNR_GRP_OR_DEPT"), 
        col("LOT_SIZE_DIVSR"), 
        col("MATL_NUM1"), 
        col("BILL_OF_MATL_STS"), 
        col("BASE_QTY"), 
        col("BASE_UNIT_OF_MEAS"), 
        col("CHG_NUM2"), 
        col("BOM_USG"), 
        col("FROM_LOT_SIZE"), 
        col("TO_LOT_SIZE"), 
        col("SCHDLNG_MRGN_KEY_FOR_FLOATS"), 
        col("SCHDLNG_TYPE"), 
        col("FLOAT_BEF_PRDTN"), 
        col("FLOAT_AFTER_PRDTN"), 
        col("RLSE_PER"), 
        col("CHG_TO_SCHD_DT_IN"), 
        col("ID_OF_THE_CAPY_RQR_REC"), 
        col("PROJ_DEF"), 
        col("INTRNL_CNTR1"), 
        col("INTRNL_CNTR2"), 
        col("CNTR_FOR_ADDL_CRITA"), 
        col("INSP_LOT_NUM"), 
        col("COST_VRNT_FOR_PLAN_COSTS"), 
        col("COST_VRNT_FOR_ACTL_COSTS"), 
        col("CMPLT_CNFRM_NUM_FOR_THE_OPR"), 
        col("INTRNL_CNTR3"), 
        col("CNFG"), 
        col("OBJ_ID_OF_THE_RSRS1"), 
        col("OBJ_ID_OF_THE_RSRS2"), 
        col("LVL"), 
        col("PATH1"), 
        col("PATH2"), 
        col("NUM_OF_RESV"), 
        col("ORDR_ITM_NUM"), 
        col("LEFT_NODE_IN_CLCTV_ORDR"), 
        col("RIGHT_NODE_OF_CLCTV_ORDR"), 
        col("CNFRM_DEG_OF_PRCSG"), 
        col("RTG_NUM_OF_OPS_IN_THE_ORDR"), 
        col("GENL_CNTR_FOR_ORDR"), 
        col("RTG_EXPLS_DTTM"), 
        col("VLD_FROM_DTTM"), 
        col("VLD_FROM_DTTM1"), 
        col("BOM_EXPLS_TFR_DTTM"), 
        col("FIN_DTTM"), 
        col("FCST_STRT_DTTM"), 
        col("SCHD_FCST_FIN_DTTM"), 
        col("SCHD_FCST_STRT_DTTM")
    )
