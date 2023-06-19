from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_btch.config.ConfigStore import *
from jde_md_btch.udfs.UDFs import *

def F4108_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((((col("_deleted_") == lit("F")) & col("IOLITM").isNotNull()) & (trim(col("IOLITM")) != lit(""))))
