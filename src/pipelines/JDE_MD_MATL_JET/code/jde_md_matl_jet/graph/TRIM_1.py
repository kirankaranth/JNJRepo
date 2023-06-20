from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def TRIM_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        trim(col("DRSY")).alias("DRSY"), 
        trim(col("DRRT")).alias("DRRT"), 
        trim(col("DRKY")).alias("DRKY"), 
        trim(col("DRDL01")).alias("DRDL01"), 
        col("_deleted_")
    )
