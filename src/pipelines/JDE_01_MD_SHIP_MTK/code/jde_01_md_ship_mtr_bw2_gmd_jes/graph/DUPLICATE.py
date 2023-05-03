from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_mtr_bw2_gmd_jes.config.ConfigStore import *
from jde_01_md_ship_mtr_bw2_gmd_jes.udfs.UDFs import *

def DUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("SHIP_NUM"), 
        col("DELV_TYP_CD"), 
        col("DOC_REF_NUM"), 
        col("CO_CD"), 
        col("DELV_LINE_NBR")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
