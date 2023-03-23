from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from ppln_sk_test2.config.ConfigStore import *
from ppln_sk_test2.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MATNR").alias("MATL_NUM"), 
        lit(Config.sourceSystem).alias("SRC_SYS_CD"), 
        trim(col("WEORA")).alias("PLNT_CD")
    )