from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_FACTORY_CALENDAR.config.ConfigStore import *
from tbl_strct_MD_FACTORY_CALENDAR.udfs.UDFs import *

def sql_MD_FCTRY_CAL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CAL_NUM,\ncast('' as int) as CAL_YR,\ncast('' as int) as CAL_DATE,\ncast('' as int) as WORK_DAY,\ncast('' as int) as FACTORY_DAY,\ncast('' as int) as FACTORY_DATE,\ncast('' as int) as FACTORY_DAY_NEXT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
