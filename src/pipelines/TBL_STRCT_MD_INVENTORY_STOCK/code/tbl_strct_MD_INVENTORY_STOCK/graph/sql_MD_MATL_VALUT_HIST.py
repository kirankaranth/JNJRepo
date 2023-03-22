from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_INVENTORY_STOCK.config.ConfigStore import *
from tbl_strct_MD_INVENTORY_STOCK.udfs.UDFs import *

def sql_MD_MATL_VALUT_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as MATL_NUM,\ncast('' as string) as VALUT_AREA_CD,\ncast('' as string) as VALUT_TYPE_CD,\ncast('' as int) as PSTNG_YR,\ncast('' as int) as PSTNG_PER,\ncast('' as string) as PRC_CNTL_IND,\ncast('' as decimal(18,4)) as TOT_STK_QTY,\ncast('' as decimal(18,4)) as TOT_VAL_AMT,\ncast('' as decimal(18,4)) as MVG_AVG_PRC_AMT,\ncast('' as decimal(18,4)) as PRC_AMT,\ncast('' as decimal(18,4)) as PRC_UNIT_NBR,\ncast('' as string) as VALUT_CLS_CD,\ncast('' as string) as BASE_UOM_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
