from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *

def Join_1(
        spark: SparkSession,
        in0: DataFrame,
        in1: DataFrame,
        in2: DataFrame, 
        in3: DataFrame, 
        in4: DataFrame
) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.VBELN") == col("in1.VBELN")), "left_outer")\
        .join(in2.alias("in2"), (col("in0.VGBEL") == col("in2.VBELN")), "left_outer")\
        .join(
          in3.alias("in3"),
          ((col("in0.VGBEL") == col("in3.VBELN")) & (col("in0.POSNR") == col("in3.POSNR"))),
          "left_outer"
        )\
        .join(in4.alias("in4"), (col("in0.MVGR4") == col("in4.MVGR4")), "left_outer")
