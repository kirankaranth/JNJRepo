from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_geu.config.ConfigStore import *
from sap_md_mfg_order_geu.udfs.UDFs import *

def DEL2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT)))
          & (col("INACT").isNull() | (col("INACT") == lit(" ")))
        )
    )
