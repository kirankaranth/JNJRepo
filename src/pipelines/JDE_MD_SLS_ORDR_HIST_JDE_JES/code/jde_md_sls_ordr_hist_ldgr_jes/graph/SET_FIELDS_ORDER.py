from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_ldgr_jes.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_jes.udfs.UDFs import *

def SET_FIELDS_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("ORDR_TYPE"), 
        col("UPDT_DTTM"), 
        col("TIME_OF_DAY"), 
        col("LINE_NUM"), 
        col("ORDR_CO"), 
        col("DOC_NUM"), 
        col("ORDR_SFX"), 
        col("BUSN_UNIT"), 
        col("CO"), 
        col("DOC_CO_ORIG_ORDR"), 
        col("ORIG_ORDR_NUM"), 
        col("ORIG_ORDR_TYPE"), 
        col("ORIG_LINE_NUM"), 
        col("COMPANY_KEY"), 
        col("RLTD_PO_NUM"), 
        col("RLTD_PO_ORDR_TYPE"), 
        col("RLTD_PO_LINE_NUM"), 
        col("AGMT_NUM_DSTN"), 
        col("AGMT_SPLMN_DSTN"), 
        col("ADDR_NUM"), 
        col("ADDR_NUM_SHIP_TO"), 
        col("ADDR_NUM_PARNT"), 
        col("RQST_DTTM"), 
        col("ORDR_DTTM"), 
        col("SCHD_PICK_DTTM"), 
        col("ACTL_SHIP_DTTM"), 
        col("INVC_DTTM"), 
        col("CAN_DTTM"), 
        col("FOR_GL_DTTM"), 
        col("PROM_DELV_DTTM"), 
        col("PRC_EFF_DTTM"), 
        col("PROM_SHIP_DTTM"), 
        col("REF"), 
        col("REF_2"), 
        col("ITM_NUM_SHRT"), 
        col("ITM_NUM_2"), 
        col("ITM_NUM_3"), 
        col("LOC"), 
        col("LOT_SER_NUM"), 
        col("FROM_GRADE"), 
        col("THRU_GRADE"), 
        col("FROM_POTENCY"), 
        col("THRU_POTENCY"), 
        col("DAYS_BEF_EXPN"), 
        col("DESC"), 
        col("DESC_LINE_2"), 
        col("LINE_TYPE"), 
        col("STS_CD_NEXT"), 
        col("STS_CD_LAST"), 
        col("BUSN_UNIT_HDR"), 
        col("ITM_NUM_RLTD"), 
        col("KIT_MSTR_LINE_NUM"), 
        col("CMPNT_LINE_NUM"), 
        col("RLTD_KIT_CMPNT"), 
        col("NUM_OF_CMPNT_PER_PARNT"), 
        col("SLS_CATLG_SECTN"), 
        col("SUB_SECTN"), 
        col("SLS_CAT_CD_3"), 
        col("SLS_CAT_CD_4"), 
        col("SLS_CAT_CD_5"), 
        col("CMMDTY_CLS"), 
        col("CMMDTY_SUB_CLS"), 
        col("SUP_REBT_CD"), 
        col("MSTR_PLNG_FMLY"), 
        col("PRCHSNG_CAT_CD_5"), 
        col("UNIT_OF_MEAS_AS_INP"), 
        col("UNIT_ORDR_QTY"), 
        col("QTY_SHIP"), 
        col("UNIT_QTY_BKORD_HELD"), 
        col("UNIT_QTY_CAN_SCRAP"), 
        col("UNIT_FUT_QTY_CMT"), 
        col("UNIT_OPEN"), 
        col("UNIT_SHIP_TO_DT"), 
        col("UNIT_RELIEVED"), 
        col("CMT"), 
        col("OTH_QTY"), 
        col("AMT_PRC_PER_UNIT"), 
        col("AMT_EXTD_PRC"), 
        col("AMT_OPEN"), 
        col("PRC_OVRD_CD"), 
        col("TEMP_PRC"), 
        col("UNIT_OF_MEAS_ENT_FOR_UNIT_PRC"), 
        col("AMT_LIST_PRC"), 
        col("AMT_UNIT_COST"), 
        col("AMT_EXTD_COST"), 
        col("COST_OVRD_CD"), 
        col("EXTD_COST_TFR"), 
        col("PRT_MSG"), 
        col("PMT_TERM_CD"), 
        col("PMT_INSTM"), 
        col("BAS_ON_DT"), 
        col("DISC_TRD"), 
        col("TRD_DISC"), 
        col("PRC_AND_ADJ_SCHED"), 
        col("ITM_PRC_GRP"), 
        col("PRC_CAT_LVL"), 
        col("DISC_CASH"), 
        col("DOC_CO"), 
        col("DOC"), 
        col("DOC_TYPE"), 
        col("DOC_ORIG"), 
        col("DOC_TYPE_ORIG"), 
        col("DOC_CO_ORIG"), 
        col("PICK_SLIP_NUM"), 
        col("DELV_NUM"), 
        col("SLS_TAX"), 
        col("TAX_RATE"), 
        col("TAX_EXPL_CD_1"), 
        col("ASSCTD_TEXT"), 
        col("PRIR_PRCSG"), 
        col("PRTD_CD"), 
        col("BKORD_ALLW"), 
        col("SUBST_ALLW"), 
        col("PRTL_LINE_SHIP_ALLW"), 
        col("LINE_OF_BUSN"), 
        col("END_USE"), 
        col("DUTY_STS"), 
        col("NATR_OF_TRX"), 
        col("PRMRY_LAST_SUP_NUM"), 
        col("CARR_NUM"), 
        col("MODE_OF_TRNSP"), 
        col("RTE_CD"), 
        col("STOP_CD"), 
        col("ZN_NUM"), 
        col("CNTNR_ID"), 
        col("FRGHT_HNDG_CD"), 
        col("SHIPPING_CMMDTY_CLS"), 
        col("SHIPPING_COND_CD"), 
        col("SER_NUM_LOT"), 
        col("UNIT_OF_MEAS_PRMRY"), 
        col("UNIT_PRMRY_QTY_ORD"), 
        col("UNIT_OF_MEAS_SEC"), 
        col("UNIT_SEC_QTY_ORD"), 
        col("UNIT_OF_MEAS_PRC"), 
        col("UNIT_WT"), 
        col("WT_UNIT_OF_MEAS"), 
        col("UNIT_VOL"), 
        col("VOL_UNIT_OF_MEAS"), 
        col("REPRC_BSK_PRC_CAT"), 
        col("ORDR_REPRC_CAT"), 
        col("ORDR_REPRC_IN"), 
        col("COST_METH_INV"), 
        col("GL_OFFST"), 
        col("CEN"), 
        col("FISC_YR"), 
        col("INTER_BRNCH_SLS"), 
        col("ON_HAND_UPDT"), 
        col("CNFG_PRT_FL"), 
        col("SLS_ORDR_STS_04"), 
        col("SUBST_ITM_IN"), 
        col("PREF_CMMT_IN"), 
        col("SHIP_DT_OVRD"), 
        col("PRC_ADJ_LINE_IN"), 
        col("PRC_ADJ_HIST_IN"), 
        col("PREF_PRDTN_ALLC"), 
        col("TFR_DIR_SHIP_INTCO_FL"), 
        col("DFR_ENTR_FL"), 
        col("EURO_CONV_STS_FL"), 
        col("SLS_ORDR_STS_14"), 
        col("SLS_ORDR_STS_15"), 
        col("APPL_COMMSN"), 
        col("COMMSN_CAT"), 
        col("RSN_CD"), 
        col("GRS_WT"), 
        col("GRS_WT_UNIT_OF_MEAS"), 
        col("SUBLDGR_GL"), 
        col("SUBLDGR_TYPE"), 
        col("CD_LOC_TAX_STS"), 
        col("PRC_CD_1"), 
        col("PRC_CD_2"), 
        col("PRC_CD_3"), 
        col("STS_IN_WHSE"), 
        col("WRK_ORDR_FRZ_CD"), 
        col("SEND_METH"), 
        col("CRNCY_CD_FROM"), 
        col("CRNCY_CONV_RT_SPOT_RT"), 
        col("AMT_LIST_PRC_PER_UNIT"), 
        col("AMT_FRGN_PRC_PER_UNIT"), 
        col("AMT_FRGN_EXTD_PRC"), 
        col("AMT_FRGN_UNIT_COST"), 
        col("AMT_FRGN_EXTD_COST"), 
        col("USER_RSRV_CD"), 
        col("USER_RSRV_DTTM"), 
        col("USER_RSRV_AMT"), 
        col("USER_RSRV_NUM"), 
        col("USER_RSRV_REF"), 
        col("TRX_ORIGNTR"), 
        col("USER_ID"), 
        col("PRG_ID"), 
        col("WRK_STN_ID"), 
        col("MFG_VAR_ACTG_FL"), 
        col("SLS_ORDR_STS_17"), 
        col("SLS_ORDR_STS_18"), 
        col("SLS_ORDR_STS_19"), 
        col("SLS_ORDR_STS_20"), 
        col("INTGR_REF_01"), 
        col("INTGR_REF_02"), 
        col("INTGR_REF_03"), 
        col("INTGR_REF_04"), 
        col("INTGR_REF_05"), 
        col("SRC_OF_ORDR"), 
        col("REF_3"), 
        col("DMAND_UNIQ_KEY_ID"), 
        col("PULL_SIGNL"), 
        col("RLSE_NUM"), 
        col("SCHD_SHIP_TIME"), 
        col("TIME_RLSE"), 
        col("RLSE_DTTM"), 
        col("RQST_DELV_TIME"), 
        col("ACTL_SHIP_TIME"), 
        col("TIME_ORIG_PROM_DELV"), 
        col("TIME_SCHD_PICK"), 
        col("TIME_FUT_TIME_2"), 
        col("CROSS_DK_FL"), 
        col("CROSS_DK_PRIR_FOR_SLS_ORD"), 
        col("DUAL_UNIT_OF_MEAS_ITM"), 
        col("BUY_SGMNT_CD"), 
        col("CUR_BUY_SGMNT_CD"), 
        col("CHG_ORDR_NUM"), 
        col("ADDR_NUM_DELV_TO"), 
        col("PEND_APPR_FL"), 
        col("RVSN_RSN"), 
        col("MATRX_CNTL_LINE_NUM"), 
        col("SHIP_NUM"), 
        col("PROM_DELV_TIME"), 
        col("PROJ_NUM"), 
        col("SEQ_NUM"), 
        col("ITM_RVSN_LVL"), 
        col("HOLD_ORD_CD"), 
        col("BUSN_UNIT_HDR_2"), 
        col("BUSN_UNIT_DMAND"), 
        col("CRNCY_CD_BASE"), 
        col("DOC_LINE_NUM_ORIG"), 
        col("ORIG_PROM_DELV_DTTM"), 
        col("CROSS_DK_ORDR_CO"), 
        col("CROSS_DK_ORDR_NO"), 
        col("CROSS_DK_ORDR_TYPE"), 
        col("CROSS_DK_LINE_NUM"), 
        col("CROSS_DK_ORDR_SFX"), 
        col("PORT_OF_ENT_OR_EXIT"), 
        col("PMT_TERM_OVRD_CD"), 
        col("BUY_NUM"), 
        col("PROM_ID"), 
        col("ASSET_ITM_NUM"), 
        col("PARNT_NUM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_deleted_"), 
        col("_pk_md5_"), 
        col("_pk_")
    )
