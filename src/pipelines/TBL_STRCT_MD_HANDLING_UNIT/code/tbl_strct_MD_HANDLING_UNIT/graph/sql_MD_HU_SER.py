from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_HANDLING_UNIT.config.ConfigStore import *
from tbl_strct_MD_HANDLING_UNIT.udfs.UDFs import *

def sql_MD_HU_SER(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as int) as HU_SER_NUM,\ncast('' as String) as HU_NUM,\ncast('' as String) as EXTRNL_HU_NUM,\ncast('' as String) as HU_LINE_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
