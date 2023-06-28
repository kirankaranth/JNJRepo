from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_01_md_matl_inv_hmd.config.ConfigStore import *
from sap_01_md_matl_inv_hmd.udfs.UDFs import *

def MANDT_4(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("_deleted_") == lit("F")) & (col("MANDT") == lit(Config.MANDT))))
