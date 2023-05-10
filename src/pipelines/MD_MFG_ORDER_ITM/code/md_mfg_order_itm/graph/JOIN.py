from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_mfg_order_itm.config.ConfigStore import *
from md_mfg_order_itm.udfs.UDFs import *

def JOIN(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.AUFNR") == col("in1.AUFNR")), "inner")\
        .select(col("in0.MANDT").alias("MANDT"), col("in0.AUART").alias("AUART"), col("in1.AUFNR").alias("AUFNR"), col("in1.POSNR").alias("POSNR"), col("in1.AMEIN").alias("AMEIN"), col("in1.CHARG").alias("CHARG"), col("in1.ELIKZ").alias("ELIKZ"), col("in1.MATNR").alias("MATNR"), col("in1.MEINS").alias("MEINS"), col("in1.PLNUM").alias("PLNUM"), col("in1.PSAMG").alias("PSAMG"), col("in1.PSMNG").alias("PSMNG"), col("in1.PWERK").alias("PWERK"), col("in1.UMREN").alias("UMREN"), col("in1.UMREZ").alias("UMREZ"), col("in1.VERID").alias("VERID"), col("in1.WEBAZ").alias("WEBAZ"), col("in1.WEMNG").alias("WEMNG"), col("in1.XLOEK").alias("XLOEK"))
