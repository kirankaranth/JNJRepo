from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_mfg_order_itm.config.ConfigStore import *
from md_mfg_order_itm.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.agg(count(lit(1)).alias("PK_COUNT"))
