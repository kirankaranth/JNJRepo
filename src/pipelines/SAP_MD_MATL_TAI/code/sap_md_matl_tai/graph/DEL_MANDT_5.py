from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_tai.config.ConfigStore import *
from sap_md_matl_tai.udfs.UDFs import *

def DEL_MANDT_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT))) & (col("SPRAS") == lit("E")))
    )
