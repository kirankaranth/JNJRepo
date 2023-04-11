from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "CRT_ON_DTTM",
        when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
    )
