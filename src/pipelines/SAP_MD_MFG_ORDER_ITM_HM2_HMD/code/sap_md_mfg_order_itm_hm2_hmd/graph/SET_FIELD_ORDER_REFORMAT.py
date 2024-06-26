from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_itm_hm2_hmd.config.ConfigStore import *
from sap_md_mfg_order_itm_hm2_hmd.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MFG_ORDR_TYP_CD"), 
        col("MFG_ORDR_NUM"), 
        col("LN_ITM_NBR"), 
        col("PRDTN_UOM_CD"), 
        col("BTCH_NUM"), 
        col("DLV_CMPLT_IND"), 
        col("MATL_NUM"), 
        col("BASE_UOM_CD"), 
        col("MFG_PLNND_ORDR_NUM"), 
        col("SCRP_QTY"), 
        col("ITM_QTY"), 
        col("PLNT_CD"), 
        col("FCTR_DNMNTR_MEAS"), 
        col("FCTR_NMRTR_MEAS"), 
        col("PRDNT_VRSN_NUM"), 
        col("GOOD_RCPT_LD_DAYS_QTY"), 
        col("RCVD_QTY"), 
        col("DEL_IND"), 
        col("L1_LAST_PRODUCTION_DTTM"), 
        col("STRG_LOC"), 
        col("SPL_PRCMT_TYPE"), 
        col("NUM_OF_QTA_ARNG"), 
        col("QTA_ARNG_ITM"), 
        col("WRK_BRKDWN_STRC_ELMNT"), 
        col("PLAN_ORDR_PLAN_STRT_DTTM"), 
        col("PLAN_ORDR_OPN_DTTM"), 
        col("SLS_ORDR"), 
        col("SLS_ORDR_ITM"), 
        col("DELV_SCHED_FOR_SLS_ORDR"), 
        col("PRCMT_TYPE"), 
        col("EXPTD_SURPLUS"), 
        col("PLAN_SCRAP_QTY"), 
        col("PLAN_TOT_ORDR_QTY"), 
        col("ACCT_ASGNMT_CAT"), 
        col("PRTL_CONV_IN"), 
        col("PLAN_ORDR_DELV_DTTM"), 
        col("COST_EST_NUM"), 
        col("OVERDELV_TLRNC"), 
        col("UNLMTED_OVERDELV_ALLW"), 
        col("UNDRDELV_TLRNC"), 
        col("STK_TYPE"), 
        col("GOODS_RCPT_IN"), 
        col("VALUT_TYPE"), 
        col("VALUT_CAT"), 
        col("RUN_SCHED_HDR_NUM"), 
        col("BOM_EXPLS_NUM"), 
        col("PARM_VARIANT"), 
        col("PLNT"), 
        col("ORDR_CAT"), 
        col("ORDR_TYPE"), 
        col("BSC_FIN_DTTM"), 
        col("SCHD_FIN_DTTM"), 
        col("ORDR_RELS_IN"), 
        col("ORDR_ITM_NOT_RLVNT_MRP_IN"), 
        col("MRP_DSTN_KEY"), 
        col("SPL_STK_IN"), 
        col("CNSMPTN_PSTNG"), 
        col("VAL_GOODS_RECV_IN_LCL_CRNCY"), 
        col("GOODS_RCPT_NON_VALUTED"), 
        col("UNLOADNG_PT"), 
        col("GOODS_RCPNT"), 
        col("BUSN_AREA"), 
        col("GOODS_RCPT_IN_CN_BE_CHG_IN"), 
        col("CNFG"), 
        col("KANBAN_IN"), 
        col("SETLM_RESV_NUM"), 
        col("ITM_NUM_OF_THE_SETLM_RESV"), 
        col("NUM_OF_RESV"), 
        col("ITM_NUM_OF_RESV"), 
        col("COST_CLCT_KEY"), 
        col("COST_CLCT_FOR_REPTV_MFG"), 
        col("COST_CLCT_FOR_KANBAN"), 
        col("COST_CLCT_VAL_SLS_ORDR_STK"), 
        col("COST_CLCT_FOR_EXTRNL_PPC"), 
        col("COST_CLCT_VLD_FROM_DTTM"), 
        col("COST_CLCT_VLD_TO_DTTM"), 
        col("OBJ_NUM"), 
        col("MATL_ORDR_ITM_IS_NOT_RLVNT"), 
        col("CMT_QTY_FOR_ORDR_ACC"), 
        col("TOT_CMMT_DTTM"), 
        col("TYPE_OF_AVLBLTY_CHK_IN"), 
        col("VALUT_OF_SPL_STK"), 
        col("SER_NUM_PRFL"), 
        col("NUM_OF_SER_NUMS"), 
        col("CHG_IN"), 
        col("PRCS_LEAD_TO_CHG_OF_AN_OBJ"), 
        col("FX_PRC_CO_PROD"), 
        col("CNFG_INTRNAL_OBJ_NUM"), 
        col("MRP_AREA"), 
        col("PARM_VRNT"), 
        col("STK_SGMNT"), 
        col("CUST_NUM1"), 
        col("SEASN_YR"), 
        col("SEASN"), 
        col("FSHN_CLCT"), 
        col("FSHN_THEME"), 
        col("ALC_STK_QTY"), 
        col("NUM_OF_ORIG_ORDR"), 
        col("CNFRM_QTY_FOR_ITM"), 
        col("ITM_SEQ"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
