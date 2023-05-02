from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def Join_2(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("IN0.WERKS") == col("IN1.WERKS")) & (col("IN0.DISPO") == col("IN1.DISPO"))),
          "left_outer"
        )\
        .select(*[col("DSNAM")], col("in0.*"))
