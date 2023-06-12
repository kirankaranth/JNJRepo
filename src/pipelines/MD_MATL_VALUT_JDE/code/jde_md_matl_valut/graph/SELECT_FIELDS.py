from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *

def SELECT_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("VALUT_AREA_CD"), 
        col("PRC_CNTL_IND"), 
        col("VALUT_TYPE_CD"), 
        col("TOT_STK_QTY"), 
        col("VALUT_CLS_CD"), 
        col("BASE_UOM_CD"), 
        col("PRC_AMT"), 
        col("PRC_UNIT_NBR"), 
        col("TOT_VAL_AMT"), 
        col("_l0_upt_"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_deleted_")
    )
