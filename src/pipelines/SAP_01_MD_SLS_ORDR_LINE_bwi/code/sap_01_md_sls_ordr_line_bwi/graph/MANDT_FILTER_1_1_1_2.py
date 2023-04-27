from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_bwi.config.ConfigStore import *
from sap_01_md_sls_ordr_line_bwi.udfs.UDFs import *

def MANDT_FILTER_1_1_1_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
