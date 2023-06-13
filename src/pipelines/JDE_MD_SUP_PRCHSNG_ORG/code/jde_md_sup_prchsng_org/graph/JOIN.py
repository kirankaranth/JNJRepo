from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *

def JOIN(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.A6AN8") == col("in1.ABAN8")), "left_outer")\
        .select(col("in0.A6AN8").alias("A6AN8"), col("in0.A6UPMJ").alias("A6UPMJ"), col("in0.A6CRRP").alias("A6CRRP"), col("in0.A6FRTH").alias("A6FRTH"), col("in0.A6AVCH").alias("A6AVCH"), col("in1.ABMCU").alias("ABMCU"), col("in0._upt_").alias("_upt_"), col("in0._deleted_").alias("_deleted_"))
