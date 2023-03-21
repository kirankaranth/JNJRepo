from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.LIITM") == col("in1.IMITM")), "left_outer")\
        .select(col("IMLITM"), col("LIMCU"), col("LILOCN"), col("LILOTN"), col("LILOTS"), col("LIPBIN"), col("LIPQOH"), col("LIQTTR"), col("LIQTIN"), col("IMUOM1"), col("LIITM"))
