from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_hmd.config.ConfigStore import *
from sap_md_matl_loc_hmd.udfs.UDFs import *

def Join_3(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("IN0.WERKS") == col("IN1.WERKS")) & (col("IN0.FEVOR") == col("IN1.FEVOR"))),
          "left_outer"
        )\
        .select(*[col("TXT")], col("in0.*"))
