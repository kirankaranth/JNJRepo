from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy(
              "SRC_SYS_CD", 
              "SRC_TBL_NM", 
              "MATL_NUM", 
              "PLNT_CD", 
              "SLOC_CD", 
              "BTCH_NUM", 
              "BTCH_STS_CD", 
              "SPCL_STCK_IND", 
              "PRTY_NUM"
            )\
            .orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
