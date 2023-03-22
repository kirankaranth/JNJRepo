from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *

def sql_MD_PRCHSNG_ORG(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as PRCHSNG_ORG,\ncast('' as String) as DESC,\ncast('' as String) as CO_CD,\ncast('' as String) as TEXT_SNDR,\ncast('' as String) as LTR_HD,\ncast('' as String) as TEXT_FTER,\ncast('' as String) as TEXT_CLSE,\ncast('' as String) as SCHMA_GP,\ncast('' as String) as MKT_SCHMA,\ncast('' as String) as EFF_PRC,\ncast('' as String) as CO_CD_SUBSQ_SETLM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
