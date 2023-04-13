from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def JDE_F0101_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
