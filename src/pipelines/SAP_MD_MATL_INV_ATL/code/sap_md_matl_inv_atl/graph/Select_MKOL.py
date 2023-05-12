from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_atl.config.ConfigStore import *
from sap_md_matl_inv_atl.udfs.UDFs import *

def Select_MKOL(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("MATNR"), 
        col("WERKS"), 
        col("CHARG"), 
        col("SOBKZ"), 
        col("LIFNR"), 
        col("SLABS"), 
        col("SINSM"), 
        col("SEINM"), 
        col("ERSDA"), 
        col("_upt_"), 
        col("LGORT")
    )
