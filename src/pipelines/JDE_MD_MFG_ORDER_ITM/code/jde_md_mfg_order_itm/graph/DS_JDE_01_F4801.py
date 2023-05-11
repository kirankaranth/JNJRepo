from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order_itm.config.ConfigStore import *
from jde_md_mfg_order_itm.udfs.UDFs import *

def DS_JDE_01_F4801(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
