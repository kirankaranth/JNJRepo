from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PLANT.config.ConfigStore import *
from tbl_strct_MD_PLANT.udfs.UDFs import *

def sql_MD_PLNT(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PLNT_CD,\ncast('' as string) as PRTY_TYPE_CD,\ncast('' as string) as PLNT_NM,\ncast('' as string) as MDM_LOC_NUM,\ncast('' as string) as PLNG_RLVNT_IND,\ncast('' as string) as CTRY_CD,\ncast('' as string) as PLNT_CAT_CD,\ncast('' as string) as ENT_PLNT_CAT_CD,\ncast('' as string) as RGN_CD,\ncast('' as string) as OPER_GRP_CD,\ncast('' as string) as PLNT_FRAN_CD,\ncast('' as string) as PLNT_SITE,\ncast('' as string) as CURVE_ID,\ncast('' as string) as PLNT_ADDR,\ncast('' as string) as LAT_NBR,\ncast('' as string) as LONG_NBR,\ncast('' as String) as VALUT_AREA,\ncast('' as String) as CO_CD,\ncast('' as string) as CAL_NUM,\ncast('' as string) as CTRY_SHRT_NM,\ncast('' as string) as ADDR_LINE_1_TXT,\ncast('' as string) as PSTL_CD_NUM,\ncast('' as string) as CITY_NM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
