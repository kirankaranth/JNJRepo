from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_mbp.config.ConfigStore import *
from sap_md_matl_mbp.udfs.UDFs import *

def DEL_MANDT_9(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT))) & (col("SPRAS") == lit("E")))
    )
