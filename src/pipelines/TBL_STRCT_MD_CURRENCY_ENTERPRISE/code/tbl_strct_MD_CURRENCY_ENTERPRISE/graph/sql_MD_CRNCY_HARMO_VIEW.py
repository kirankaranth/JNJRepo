from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CURRENCY_ENTERPRISE.config.ConfigStore import *
from tbl_strct_MD_CURRENCY_ENTERPRISE.udfs.UDFs import *

def sql_MD_CRNCY_HARMO_VIEW(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CRNCY_CD,\ncast('' as string) as ENTRP_CRNCY_SHRT_NM,\ncast('' as string) as ENTRP_CRNCY_LONG_NM,\ncast('' as string) as ENTRP_ISO_CRNCY_CD,\ncast('' as string) as CRNCY_SHRT_NM,\ncast('' as string) as CRNCY_LONG_NM,\ncast('' as string) as ISO_CRNCY_CD,\ncast('' as timestamp) as VLD_FROM_DTTM,\ncast('' as timestamp) as VLD_TO_DTTM,\ncast('' as string) as CRNCY_ALT_CD,\ncast('' as string) as ENTRP_CRNCY_ALT_CD,\ncast('' as string) as DEC_PLACE_CNT,\ncast('' as string) as ENTRP_DEC_PLACE_CNT,\ncast('' as string) as ENTRP_SRC_SYS_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
