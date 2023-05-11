from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order_itm.config.ConfigStore import *
from jde_md_mfg_order_itm.udfs.UDFs import *

def MANDT_FILTER_AFPO(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
