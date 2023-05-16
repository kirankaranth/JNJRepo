from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def Reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("LIITM"), 
        col("LIMCU"), 
        col("LILOCN"), 
        col("LILOTN"), 
        col("LILOTS"), 
        col("LIPBIN"), 
        col("LIPQOH"), 
        col("LIQTTR"), 
        col("LIQTIN"), 
        col("_upt_")
    )
