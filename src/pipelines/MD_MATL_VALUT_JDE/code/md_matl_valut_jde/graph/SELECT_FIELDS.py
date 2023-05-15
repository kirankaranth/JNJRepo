from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

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
        col("TOT_VAL_AMT")
    )
