from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BATCH_MASTER.config.ConfigStore import *
from tbl_strct_MD_BATCH_MASTER.udfs.UDFs import *

def sql_MD_BTCH(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SRC_TBL_NM,\ncast('' as string) as MATL_NUM,\ncast('' as string) as BTCH_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as DEL_IND,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as timestamp) as AVAIL_DTTM,\ncast('' as timestamp) as BTCH_EXP_DTTM,\ncast('' as string) as BTCH_STS_CD,\ncast('' as timestamp) as BTCH_LAST_STS_DTTM,\ncast('' as string) as SUP_NUM,\ncast('' as string) as SUP_BTCH_NUM,\ncast('' as timestamp) as BTCH_LAST_GR_DTTM,\ncast('' as timestamp) as BTCH_MFG_DTTM,\ncast('' as string) as BTCH_TYPE,\ncast('' as string) as SUI_IND,\ncast('' as string) as LOT_GRADE,\ncast('' as string) as PARENT_CODE,\ncast('' as string) as SHRT_MATL_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
