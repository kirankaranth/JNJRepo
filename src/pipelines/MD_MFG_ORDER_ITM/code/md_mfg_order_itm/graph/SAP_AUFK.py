from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_mfg_order_itm.config.ConfigStore import *
from md_mfg_order_itm.udfs.UDFs import *

def SAP_AUFK(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.aufk")
