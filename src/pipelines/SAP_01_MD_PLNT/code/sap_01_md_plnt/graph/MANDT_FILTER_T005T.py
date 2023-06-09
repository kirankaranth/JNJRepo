from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *

def MANDT_FILTER_T005T(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("MANDT") == lit(Config.MANDT)) & (col("SPRAS") == lit("E"))) & (col("_deleted_") == lit("F")))
    )
