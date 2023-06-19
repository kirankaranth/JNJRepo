from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def F554101B_SELEC(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("XBITM"), col("XB_T003"), col("XB_T162"), col("_deleted_"), col("XBMCU"))
