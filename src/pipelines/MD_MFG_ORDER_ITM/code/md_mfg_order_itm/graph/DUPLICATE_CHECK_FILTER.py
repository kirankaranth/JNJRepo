from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_mfg_order_itm.config.ConfigStore import *
from md_mfg_order_itm.udfs.UDFs import *

def DUPLICATE_CHECK_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
