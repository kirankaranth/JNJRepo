from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *

def SET_FIELD_ORDER_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("BILL_DOC"), 
        col("SLORG_CD"), 
        col("SLORG_DESC"), 
        col("DSTR_CHNL_CD"), 
        col("DSTR_CHNL_DESC"), 
        col("SLS_DIV_CD"), 
        col("SLS_DIV_DESC"), 
        col("BILL_TYPE_CD"), 
        col("BILL_TYPE_DESC"), 
        col("BILL_CAT"), 
        col("DOC_CAT"), 
        col("PYR"), 
        col("SOLD_TO"), 
        col("CRT_DTTM"), 
        col("BILL_DTTM"), 
        col("BILL_INVC_DTTM"), 
        col("CRNCY_CD"), 
        col("CRT_BY"), 
        col("PRC_PCDR_CD"), 
        col("DOC_COND_OWN_COND"), 
        col("SHIPPING_COND_CD"), 
        col("FISC_YR"), 
        col("PSTNG_PER"), 
        col("CUST_GRP_CD"), 
        col("INTNL_COM_CD"), 
        col("DEL_DPRT_PT_CD"), 
        col("PSTNG_STS"), 
        col("EXCH_RT_FIN_PSTNG"), 
        col("ADDL_VAL_DAYS"), 
        col("FX_VAL_DTTM"), 
        col("PMT_TERM_CD"), 
        col("ACCT_ASGNMT_GRP"), 
        col("CTRY_CD"), 
        col("REGION_CD"), 
        col("CO_CD"), 
        col("TAX_CLSN_1"), 
        col("NET_VAL_AMT"), 
        col("COMB_CRITA"), 
        col("STATS_CRNCY"), 
        col("CHG_DTTM"), 
        col("INVC_LIST_TYPE"), 
        col("CNTL_AREA_CD"), 
        col("CR_ACCT"), 
        col("CRNCY_CR_CNTL_AREA"), 
        col("CR_DX_RT"), 
        col("HIER_TYPE_PRC"), 
        col("CUST_PO_NUM"), 
        col("TRAD_PTNR_CO_CD"), 
        col("TAX_DPRT_CTRY"), 
        col("ORIG_VAT_NUM"), 
        col("CTRY_VAT_NUM"), 
        col("REF_DOC_NUM"), 
        col("ASGNMT_NUM"), 
        col("TAX_AMT"), 
        col("LOGL_SYS"), 
        col("TRNL_DTTM"), 
        col("PMT_REF"), 
        col("NUM_OF_PG"), 
        col("CUST_PRC_GRP"), 
        col("SLS_DSTRC"), 
        col("PRC_LIST_TYPE"), 
        col("TAX_CLSN_2"), 
        col("TAX_CLSN_3"), 
        col("TAX_CLSN_4"), 
        col("TAX_CLSN_5"), 
        col("TAX_CLSN_6"), 
        col("TAX_CLSN_7"), 
        col("TAX_CLSN_8"), 
        col("TAX_CLSN_9"), 
        col("INDSTR_CD_1"), 
        col("INDSTR_CD_2"), 
        col("BILL_DOC_IS_CAN"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )