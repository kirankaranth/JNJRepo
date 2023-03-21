from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

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
        .join(in1.alias("in1"), (col("in0.MMSTA") == col("in1.MMSTA")), "left_outer")\
        .join(in2.alias("in2"), (col("in0.DISPO") == col("in2.DISPO")), "left_outer")\
        .join(
          in3.alias("in3"),
          ((col("in0.WERKS") == col("in3.WERKS")) & (col("in0.FEVOR") == col("in3.FEVOR"))),
          "left_outer"
        )\
        .join(in4.alias("in4"), (col("in0.EKGRP") == col("in4.EKGRP")), "left_outer")\
        .where((col("in1.SPRAS") == lit("E")))\
        .select(col("in0.WERKS").alias("NSDM_V_MARC_WERKS"), col("in0.MATNR").alias("NSDM_V_MARC_MATNR"))
