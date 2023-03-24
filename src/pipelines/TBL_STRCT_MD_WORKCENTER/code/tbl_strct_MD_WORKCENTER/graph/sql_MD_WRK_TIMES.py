from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_WORKCENTER.config.ConfigStore import *
from tbl_strct_MD_WORKCENTER.udfs.UDFs import *

def sql_MD_WRK_TIMES(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as GRP_SHFT_DEF,\ncast('' as string) as SHFT_DEF,\ncast('' as timestamp) as END_DTTM,\ncast('' as timestamp) as STRT_DTTM,\ncast('' as int) as OPER_TIME_IN_SEC,\ncast('' as string) as WRK_BRK_SCHED,\ncast('' as string) as PICK_WAVE_PRFL,\ncast('' as string) as PERS_GRP_DAILY_WRK_SCHED,\ncast('' as string) as DAILY_WRK_SCHED,\ncast('' as string) as NM_OF_VRNT,\ncast('' as timestamp) as HR_END_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
