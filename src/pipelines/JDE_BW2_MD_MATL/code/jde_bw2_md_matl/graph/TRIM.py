from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *

def TRIM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        trim(col("DRKY")).alias("DRKY"), 
        trim(col("DRDL01")).alias("DRDL01"), 
        trim(col("DRRT")).alias("DRRT"), 
        col("_deleted_")
    )
