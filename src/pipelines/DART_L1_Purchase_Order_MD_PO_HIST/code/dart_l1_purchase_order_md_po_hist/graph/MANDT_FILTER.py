from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *
from dart_l1_purchase_order_md_po_hist.udfs.UDFs import *

def MANDT_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("MANDT") == lit(Config.MANDT)))
