from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def sql_MD_CUST_SLS_PTNR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as CUST_NUM,\ncast('' as String) as DSTR_CHNL_CD,\ncast('' as String) as DIV_CD,\ncast('' as String) as PTNR_CNTR_NBR,\ncast('' as String) as PTNR_FUNC_CD,\ncast('' as String) as SLORG_NUM,\ncast('' as String) as SUP_NUM,\ncast('' as String) as DFLT_PTNR_IND,\ncast('' as String) as PTNR_NUM,\ncast('' as String) as NM_BUSN_PTNR_FUNC,\ncast('' as String) as PTNR_FUNC_TEXT,\ncast('' as String) as NM_SLORG,\ncast('' as String) as SLORG_TEXT,\ncast('' as String) as NM_DSTN,\ncast('' as String) as DSTN_CHNL_TEXT,\ncast('' as String) as NM_SLS,\ncast('' as String) as DIV_TEXT,\ncast('' as String) as PERS_NUM,\ncast('' as String) as NUM_CNTCT_PRSN,\ncast('' as String) as CUST_DESC_PTNR,\ncast('' as String) as BUSN_PTNR_ADDR_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
