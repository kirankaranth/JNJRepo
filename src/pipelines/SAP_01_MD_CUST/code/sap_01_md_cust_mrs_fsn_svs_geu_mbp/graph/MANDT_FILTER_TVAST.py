from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.config.ConfigStore import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.udfs.UDFs import *

def MANDT_FILTER_TVAST(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("MANDT") == lit(Config.MANDT)) & (col("SPRAS") == lit("E"))) & (col("_deleted_") == lit("F")))
    )
