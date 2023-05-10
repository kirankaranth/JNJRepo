from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_itm.config.ConfigStore import *
from sap_md_mfg_order_itm.udfs.UDFs import *

def SELECT_AUFK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("MANDT"), col("AUFNR"), col("AUART"), col("_deleted_"))
