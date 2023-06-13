from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *

def REFORMAT_F0401(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("A6AN8"), 
        col("A6UPMJ"), 
        col("A6CRRP"), 
        col("A6FRTH"), 
        col("A6AVCH"), 
        col("_upt_"), 
        col("_deleted_")
    )
