from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bwi_tai.config.ConfigStore import *
from sap_md_delv_line_bwi_tai.udfs.UDFs import *

def MANDT_FILTER_VBUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
