from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order.config.ConfigStore import *
from jde_md_mfg_order.udfs.UDFs import *

def F4801(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.F4801}")
