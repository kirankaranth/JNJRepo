from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_ship.config.ConfigStore import *
from sap_01_md_ship.udfs.UDFs import *

def SET_FIELD_ORDER_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SHIP_NUM"), 
        col("DELV_TYP_CD"), 
        col("DOC_REF_NUM"), 
        col("CO_CD"), 
        col("DELV_LINE_NBR"), 
        col("ACTL_SHIP_DTTM"), 
        col("PLAN_SHIP_DTTM"), 
        col("SLS_ORDR_CAR_CD"), 
        col("SHIP_CNTNR_NUM"), 
        col("SHIP_STS_CD"), 
        col("SHIP_TYPE_CD"), 
        col("TRSPN_PLNG_PT"), 
        col("NM_PRSN_RESP_CREAT_OBJ"), 
        col("CRT_DTTM"), 
        col("NM_PRSN_CHG_OBJ"), 
        col("CHG_DTTM"), 
        col("LEG_DTRMN"), 
        col("SHIP_CMPLT_TYPE"), 
        col("PRCSG_CNTL"), 
        col("SRVC_LVL"), 
        col("SHIPPING_TYPE"), 
        col("SHIPPING_TYPE_PRLM_LEG"), 
        col("SHIPPING_TYPE_SUBSQ_LEG"), 
        col("LEG_IN"), 
        col("SHIPPING_COND"), 
        col("SHIP_RTE"), 
        col("EXTRNL_ID_1"), 
        col("EXTRNL_ID_2"), 
        col("DESC_SHIP"), 
        col("STS_TRSPN_PLNG"), 
        col("END_PLNG_DTTM"), 
        col("STS_OF_CHKIN"), 
        col("PLAN_CHKIN_DTTM"), 
        col("CHKIN_DTTM"), 
        col("STS_STRT_LD"), 
        col("PLAN_LD_STRT_DTTM"), 
        col("CUR_STRT_LD_DTTM"), 
        col("STS_FOR_END_OF_LD"), 
        col("PLAN_END_LD_DTTM"), 
        col("ACTL_END_LD_DTTM"), 
        col("STS_SHIP_CMPLT"), 
        col("STS_FOR_STRT_OF_SHIP"), 
        col("PLAN_TRNSP_STRT_DTTM"), 
        col("ACTUAL_TRNSP_STRT_DTTM"), 
        col("STS_END_SHIP"), 
        col("PLAN_TRNSP_END_DTTM"), 
        col("ACTL_SHIP_END_DTTM"), 
        col("NUM_FRWD_AGN"), 
        col("ORDR_NUM"), 
        col("SHIP_CONT_HNDG_UNIT"), 
        col("UNIT_WT_TRSPN_PLNG"), 
        col("VOL_UNIT_TRSPN_PLNG"), 
        col("DSTC"), 
        col("DSTC_UNIT_MEAS"), 
        col("TRAVL_TIME_ONLY_BTWN_TWO_LOC"), 
        col("TOT_TRAVL_TIME_BTWN_TWO_LOC_INCL_BRK"), 
        col("UNIT_MEAS_TRAVL_TM"), 
        col("UPDT_GRP_STATS_UPDT"), 
        col("STS_SHIP_COST_CALC"), 
        col("OVRL_STS_CALC_SHIP_COST_SHIP"), 
        col("STS_SHIP_COST_SETLM"), 
        col("TOT_STS_SHIP_COST_SETLM_SHIP"), 
        col("LEG_DTRMN_CMPLT"), 
        col("HNDG_UNIT_DATA_REF_SHIP_COST_DOC"), 
        col("PRC_PCDR_SHIP_HDR"), 
        col("SPL_PRCSG_IN"), 
        col("SHIP_COST_RLVNT"), 
        col("PLAN_TOT_TIME_TRSPN"), 
        col("PLAN_DUR_TRSPN"), 
        col("ACTL_TOT_TIME_SHIP"), 
        col("ACTL_TIME_NEED_TRSPN"), 
        col("RTE_COPY_FROM_DELV"), 
        col("GLOBL_UNIQ_ID"), 
        col("TIME_SGMNT_EXISTS"), 
        col("TIME_SGMNT_EV_GRP_SHIP_HDR"), 
        col("SUP_1"), 
        col("SUP_2"), 
        col("SUP_3"), 
        col("SUP_4"), 
        col("ADDID_TEXT_1"), 
        col("ADDID_TEXT_2"), 
        col("ADDID_TEXT_3"), 
        col("ADDID_TEXT_4"), 
        col("DNGRS_GOODS_MGMT_PRFL_SD_DOC"), 
        col("BLK_IN_DNGRS_GOODS"), 
        col("SLCT_DNGRS_GOODS_MSTR_DATA_DTTM"), 
        col("IN_DOC_CONT_DNGRS_GOODS"), 
        col("PLAN_WAIT_TIME_SHIP"), 
        col("CUR_WAIT_TIME_SHIP"), 
        col("RTE_SCHED"), 
        col("TENDER_STS"), 
        col("ACC_COND_REJ_RSN"), 
        col("TENDER_TEXT"), 
        col("MAX_PRC_SHIP"), 
        col("CRNCY_MAX_PRC"), 
        col("ACTL_SHIP_COST_SHIP"), 
        col("CRNCY_ACTL_SHIP_COST"), 
        col("FRWD_AGN_ACPT_SHIP"), 
        col("NM_CARR_ACPT_SHIP"), 
        col("FRWD_AGN_TCKG_ID"), 
        col("QTA_EXP_DTTM"), 
        col("EARLY_PCKUP_DTTM"), 
        col("LTST_PCKUP_DTTM"), 
        col("EARLY_DELV_DTTM"), 
        col("LTST_DELV_DTTM"), 
        col("LGTH_LD_PLTF"), 
        col("UNIT_MEAS_LD_LGTH"), 
        col("IN_HUS_RLVNT_DELV_ITM_GNR"), 
        col("ALLW_TOT_WT_SHIP"), 
        col("DSTN_STS"), 
        col("SHIP_DSTN_ORIG_SYS"), 
        col("ID_NUM_CONT_MOVE"), 
        col("SEQ_TRNSP_CNCT_TRFC"), 
        col("EXTRNL_FRGHT_ORDR_ID"), 
        col("KEY_NM_BUSN_SYS"), 
        col("DRVR_1"), 
        col("DRVR_2"), 
        col("VEH"), 
        col("TRLR"), 
        col("LD_SEQ_NUM_TOUR"), 
        col("STS_VEH_SPACE_OPTZ"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
