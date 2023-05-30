from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hm2_hmd_mbp_mrs_p01_svs.udfs.UDFs import *

def JOIN(spark: SparkSession, in0: DataFrame, in1: DataFrame, in2: DataFrame) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.AUFNR") == col("in1.AFKO_AUFNR")), "left_outer")\
        .join(in2.alias("in2"), (col("in0.OBJNR") == col("in2.JEST_OBJNR")), "inner")
