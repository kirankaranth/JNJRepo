from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_EQUIPMENT.config.ConfigStore import *
from tbl_strct_MD_EQUIPMENT.udfs.UDFs import *

def sql_MD_SER_NUM_STOCK_SGMNT(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as EQMNT_NUM,\ncast('' as String) as STOCK_TYPE_GOODS_MVMT,\ncast('' as String) as PLNT,\ncast('' as String) as STRG_LOC,\ncast('' as String) as BTCH_NUM,\ncast('' as String) as SPL_STOCK_IN,\ncast('' as String) as SPL_STOCK_CUST_ACCT_NUM,\ncast('' as String) as ACCT_NUM_VEND,\ncast('' as String) as SLS_ORDR_NUM,\ncast('' as String) as ITM_NUM_SLS_ORDR,\ncast('' as String) as WBS_ELMNT,\ncast('' as String) as OWN_STOCK,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
