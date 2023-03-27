from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SALES_ORDER.config.ConfigStore import *
from tbl_strct_MD_SALES_ORDER.udfs.UDFs import *

def sql_MD_SLS_DOC_PTNR_FUNC(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SLS_DOC_ITEM_NBR,\ncast('' as string) as SLS_DOC_ID,\ncast('' as string) as PTNR_FUNC_CD,\ncast('' as string) as COMPANY_CD,\ncast('' as string) as SLS_ORDR_TYPE_CD,\ncast('' as string) as FURTHER_PTNR_IND,\ncast('' as string) as SUP_NUM,\ncast('' as string) as ADDR_USG_CD,\ncast('' as string) as CUST_NUM,\ncast('' as string) as CTRY_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
