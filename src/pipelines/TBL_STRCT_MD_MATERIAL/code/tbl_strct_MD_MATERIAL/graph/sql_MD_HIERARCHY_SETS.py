from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *

def sql_MD_HIERARCHY_SETS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SET_CLS,\ncast('' as string) as ORG_UNIT_SUBCL,\ncast('' as string) as SET_NM,\ncast('' as string) as LINE_ID,\ncast('' as string) as SIGN_FLD,\ncast('' as string) as OPT_FLD,\ncast('' as string) as VLD_FROM,\ncast('' as string) as VLD_TO,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
