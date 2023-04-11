from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_sls_doc_ptnr_func_hm2.config.ConfigStore import *
from sap_md_sls_doc_ptnr_func_hm2.udfs.UDFs import *

def SET_FIELDS_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SLS_DOC_ITEM_NBR"), 
        col("SLS_DOC_ID"), 
        col("PTNR_FUNC_CD"), 
        col("COMPANY_CD"), 
        col("SLS_ORDR_TYPE_CD"), 
        col("FURTHER_PTNR_IND"), 
        col("SUP_NUM"), 
        col("ADDR_USG_CD"), 
        col("CUST_NUM"), 
        col("CTRY_CD"), 
        col("PERS_NUM"), 
        col("NUM_CNTCT_PRSN"), 
        col("UNLD_PT"), 
        col("ADDR_IN"), 
        col("IN_ACCT_ONE_TM_ACCT"), 
        col("CUST_HIER_TYPE"), 
        col("RLVNT_PRC_DTRMN_ID"), 
        col("IN_CUST_REBT_RLVNT"), 
        col("LVL_NUM_WTHN_HIER"), 
        col("CUST_DESC_PTNR"), 
        col("TRSPN_ZN_GOODS_DELV"), 
        col("ASGNMT_HIER"), 
        col("VAT_REGS_NUM"), 
        col("PRSN_NUM"), 
        col("MANTN_APPT_CAL"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_deleted_"), 
        col("DATA_FIL_VAL_DATA_AGE_DTTM"), 
        col("BUSN_PTNR_NUM"), 
        col("ADDR_DTRMN_DOC"), 
        col("ADDR_TYPE"), 
        col("BP_REF_ADDR_NUM"), 
        col("DUMMY_FUNC_LGTH_1"), 
        col("SDM_VERS_FLD_FOR_VBPA")
    )
