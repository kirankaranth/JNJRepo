from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_mbp_fsn.config.ConfigStore import *
from sap_md_co_cd_mbp_fsn.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("CO_CD"), 
        col("CUST_NUM"), 
        col("BUY_GRP_CD"), 
        col("PLNG_CUST_IND"), 
        col("TERM_PMT_KEY"), 
        col("PERS_NUM"), 
        col("REC_CRT_DTTM"), 
        col("NM_PRSNCRT_OBJ"), 
        col("PSTNG_BLK_CO_CD"), 
        col("DEL_FL_MSTR_REC"), 
        col("KEY_SRT_ACCORD_ASGNMT_NUM"), 
        col("ACTG_CLERK_ABBR"), 
        col("RCNL_ACCT_GENL_LDGR"), 
        col("AUTH_GRP"), 
        col("HD_OFF_ACCT_NUM"), 
        col("ACCT_NUM_ALT_PYR"), 
        col("IN_PMT_NOTC_CUST_WTH_CLR_ITM"), 
        col("IN_PMT_NOTC_SLS_DEPT"), 
        col("IN_PMT_NOTC_LEGAL_DEPT"), 
        col("IN_PMT_NOTC_ACTG_DEPT"), 
        col("IN_PMT_NOTC_CUST_WOUT_CLR_ITM"), 
        col("LIST_RSPCT_PMT_METHDS"), 
        col("IN_CLRNG_BTWN_CUST_VEND"), 
        col("BLK_KEY_PMT"), 
        col("TERM_PMT_KEY_BILL_EXCH_CHRG"), 
        col("INT_IN"), 
        col("KEY_LAST_INT_CALC_DTTM"), 
        col("INT_CALC_FREQ_MO"), 
        col("ACCT_NUM_CUST"), 
        col("USER_AT_CUST"), 
        col("MEMO"), 
        col("EXPT_CR_INSR_INST_NUM"), 
        col("AMT_INS"), 
        col("INSR_LEAD_MO"), 
        col("DEDCT_PCT_RT"), 
        col("INSR_NUM"), 
        col("INSR_VLD_DTTM"), 
        col("COLL_INVC_VRNT"), 
        col("IN_LCL_PRCSG"), 
        col("IN_PRD_ACCT_STMT"), 
        col("BILL_EXCH_LMT"), 
        col("NEXT_PAYEE"), 
        col("LAST_INT_CALC_RUN_DTTM"), 
        col("IN_REC_PMT_HIST"), 
        col("TLRNC_GRP_BUSN_PTNR_ACCT"), 
        col("PRBL_TIME_UNTL_CHK_PD"), 
        col("SHRT_KEY_HS_BANK"), 
        col("IN_PAY_ALL_ITM_SEP"), 
        col("OBSOL_SUB_IN_DTRMN_RDCTN_RT"), 
        col("PREV_MSTR_REC_NUM"), 
        col("KEY_PMT_GRP"), 
        col("SHRT_KEY_KNOWN_NEGT_LV"), 
        col("KEY_DUN_NOTC_GRP"), 
        col("KEY_LKBX_CUST_PAY"), 
        col("PMT_METH_SPLMN"), 
        col("SLCT_RULE_PMT_ADVC"), 
        col("IN_SEND_PMT_ADVC_EDI"), 
        col("RLSE_APPR_GRP"), 
        col("RSN_CD_CONV_VERS"), 
        col("ACTG_CLERK_FAX_NUM_CUST_VEND"), 
        col("INET_ADDR_PTNR_CO_CLERK"), 
        col("IN_ALT_PYR_USE_ACCT_NUM"), 
        col("PMT_TERM_KEY_FOR_CR_MEMO"), 
        col("ACTV_CD_GRS_INCM_TAX"), 
        col("DSTN_TYPE_EMP_TAX"), 
        col("VAL_ADJ_KEY"), 
        col("STS_CHG_AUTH"), 
        col("CHG_CNFRM_DTTM"), 
        col("LAST_CHG_CNFRM_DTTM"), 
        col("DEL_BOCK_MSTR_REC"), 
        col("ACTG_CLERK_PHN_NUM_BUSN_PTNR"), 
        col("ACCT_RCVBL_PLDG_IN"), 
        col("CUST_EXEC"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
