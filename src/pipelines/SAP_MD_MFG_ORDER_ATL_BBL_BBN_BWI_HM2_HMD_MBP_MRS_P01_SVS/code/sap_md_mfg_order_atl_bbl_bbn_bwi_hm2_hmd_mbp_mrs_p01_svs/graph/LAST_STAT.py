from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.udfs.UDFs import *

def LAST_STAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("row_number", row_number().over(Window.partitionBy("OBJNR", "STAT").orderBy(col("STAT").asc())))\
        .withColumn("count", count("*").over(Window.partitionBy("OBJNR", "STAT")))\
        .filter(col("row_number") == col("count"))\
        .drop("row_number")\
        .drop("count")
