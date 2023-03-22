from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def sql_MD_CUST_CR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CNTL_AREA_CD,\ncast('' as string) as CUST_NUM,\ncast('' as string) as CR_RISK_CAT_CD,\ncast('' as decimal(18,4)) as CUST_CR_LMT,\ncast('' as string) as CUST_ACCT_NUM_CR_LMT_REF,\ncast('' as decimal(18,4)) as TOT_SLS_VAL_CR_LMT_CHK,\ncast('' as decimal(18,4)) as TOT_RCVBL,\ncast('' as decimal(18,4)) as RLVNT_SPL_LIAB_CR_LMT_CHK,\ncast('' as timestamp) as CR_LMT_EXCD_DTTM,\ncast('' as string) as IN_CR_LMT_RECRT,\ncast('' as string) as NM_PRSN_CRT_OBJ,\ncast('' as timestamp) as REC_CRT_DTTM,\ncast('' as timestamp) as LAST_INTRNL_REV_DTTM,\ncast('' as string) as IN_BLOK_CR_MGMT,\ncast('' as string) as CR_REP_GRP_CR_MGMT,\ncast('' as timestamp) as NEXT_REV_DTTM,\ncast('' as string) as CR_INFO_NUM,\ncast('' as string) as DO_NOT_USE_RPLC_DATA_ELMNT_DBPAY_CM,\ncast('' as string) as DO_NOT_USE_RPLC_DBRTG_CM,\ncast('' as timestamp) as LAST_EXTRNL_REV_DTTM,\ncast('' as timestamp) as LAST_CHG_DTTM,\ncast('' as timestamp) as LAST_TEXT_CHG_DTTM,\ncast('' as string) as CUST_CR_GRP,\ncast('' as string) as LAST_CHG_BY,\ncast('' as timestamp) as REF_DTTM,\ncast('' as string) as CUST_GRP,\ncast('' as timestamp) as LAST_PMT_DTTM,\ncast('' as decimal(18,4)) as AMT_LAST_PMT,\ncast('' as string) as CRNCY_LAST_PMT,\ncast('' as string) as PMT_INDX,\ncast('' as string) as RATG,\ncast('' as decimal(18,4)) as RCMND_CR_LMT,\ncast('' as string) as CRNCY_RCMND_CR_LMT,\ncast('' as timestamp) as MONTR_DTTM,\ncast('' as decimal(18,4)) as TOT_SECR_RCVBL,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
