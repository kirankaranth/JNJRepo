from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_TRACKING_DATA.config.ConfigStore import *
from tbl_strct_MD_TRACKING_DATA.udfs.UDFs import *

def sql_MD_TCKG_DATA_ITM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as SD_DOC_CAT,\ncast('' as String) as SLS_AND_DSTN_DOC_NUM,\ncast('' as String) as EXP_DELV_CO,\ncast('' as String) as ITM_NUM_OF_SD_DOC,\ncast('' as timestamp) as TCKG_IN_UTC_FMT_DTTM,\ncast('' as String) as TCKG_LOC,\ncast('' as String) as TCKG_STS,\ncast('' as String) as TCKG_STS_DTL,\ncast('' as String) as DESC_OF_TCKG_EV,\ncast('' as String) as TIME_ZN,\ncast('' as timestamp) as FIL_VAL_DATA_AGE_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
