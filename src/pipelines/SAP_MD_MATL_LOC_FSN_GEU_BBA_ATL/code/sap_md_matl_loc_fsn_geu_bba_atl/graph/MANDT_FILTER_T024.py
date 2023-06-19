from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_fsn_geu_bba_atl.config.ConfigStore import *
from sap_md_matl_loc_fsn_geu_bba_atl.udfs.UDFs import *

def MANDT_FILTER_T024(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
