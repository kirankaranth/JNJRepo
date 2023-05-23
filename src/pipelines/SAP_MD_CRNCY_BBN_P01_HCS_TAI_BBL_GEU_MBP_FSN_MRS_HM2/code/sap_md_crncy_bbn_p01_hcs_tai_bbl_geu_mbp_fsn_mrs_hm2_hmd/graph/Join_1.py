from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.config.ConfigStore import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.WAERS") == col("in1.CURRKEY")), "left_outer")\
        .select(col("in0.WAERS").alias("WAERS"), col("in0.ISOCD").alias("ISOCD"), col("in0.ALTWR").alias("ALTWR"), col("in0.GDATU").alias("GDATU"), col("in0.XPRIMARY").alias("XPRIMARY"), col("in1.CURRDEC").alias("CURRDEC"), col("in0._upt_").alias("_upt_"))
