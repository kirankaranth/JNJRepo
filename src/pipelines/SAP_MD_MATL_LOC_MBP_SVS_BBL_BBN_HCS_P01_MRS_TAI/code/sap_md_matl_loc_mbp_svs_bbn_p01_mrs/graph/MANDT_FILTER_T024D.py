from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.udfs.UDFs import *

def MANDT_FILTER_T024D(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
