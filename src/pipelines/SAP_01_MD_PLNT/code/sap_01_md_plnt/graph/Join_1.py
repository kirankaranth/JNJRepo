from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.BWKEY") == col("in1.BWKEY")), "left_outer")\
        .join(in2.alias("in2"), (col("in1.BUKRS") == col("in2.BUKRS")), "inner")\
        .select(col("in0.WERKS").alias("WERKS"), col("in0.NAME1").alias("NAME1"), col("in0.LAND1").alias("LAND1"), col("in0.NODETYPE").alias("NODETYPE"), col("in0.REGIO").alias("REGIO"), col("in0.BWKEY").alias("BWKEY"), col("in1.BUKRS").alias("BUKRS"), col("in0.FABKL").alias("FABKL"), col("in0.STRAS").alias("STRAS"), col("in0.PSTLZ").alias("PSTLZ"), col("in0.ORT01").alias("ORT01"), col("in0._upt_").alias("_upt_"), col("in2.WAERS").alias("WAERS"))
