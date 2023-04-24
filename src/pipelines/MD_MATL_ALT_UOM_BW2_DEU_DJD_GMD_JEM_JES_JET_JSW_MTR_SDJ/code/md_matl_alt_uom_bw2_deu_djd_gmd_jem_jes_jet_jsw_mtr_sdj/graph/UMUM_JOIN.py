from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def UMUM_JOIN(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), ((col("in0.UMITM") == col("in1.umitm")) & (col("in0.imuom1") == col("in1.UMUM"))), "inner")\
        .select(coalesce(col("in0.UMITM"), col("in0.IMLITM")).alias("ITEM"), col("in1.UMRUM").alias("ALT_UOM"), col("in0.IMUOM1").alias("BASE_UOM"), col("in1.UMCONV").alias("CONVERSION"), lit(1).alias("JOIN_ORIGIN"), col("in1._upt_").alias("f41002_upt_"))
