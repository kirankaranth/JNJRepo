from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_mbp_fsn.config.ConfigStore import *
from sap_md_co_cd_mbp_fsn.udfs.UDFs import *

def MANDT_FILTER_KNB1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
