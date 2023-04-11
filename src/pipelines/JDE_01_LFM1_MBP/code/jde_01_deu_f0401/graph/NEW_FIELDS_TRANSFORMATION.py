from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SUP_NUM", col("LIFNR"))\
        .withColumn("PRCHSNG_ORG_NUM", col("EKORG"))\
        .withColumn(
          "CRT_ON_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("PRCH_BLK_IND", trim(col("SPERM")))\
        .withColumn("DEL_IND", trim(col("LOEVM")))\
        .withColumn("CRNCY_CD", trim(col("WAERS")))\
        .withColumn("PMT_TERM_CD", trim(col("ZTERM")))
