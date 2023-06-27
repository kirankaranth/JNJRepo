from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_ordr_hist_ldgr_gmd.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_gmd.udfs.UDFs import *

def DELETED(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
