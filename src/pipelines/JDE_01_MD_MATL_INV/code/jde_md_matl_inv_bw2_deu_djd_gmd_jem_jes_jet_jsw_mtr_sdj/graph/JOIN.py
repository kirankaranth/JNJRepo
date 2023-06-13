from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def JOIN(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.LIITM") == col("in1.IMITM")), "left_outer")\
        .select(col("IMLITM"), col("LIMCU"), col("LILOCN"), col("LILOTN"), col("LILOTS"), col("LIPBIN"), col("LIPQOH"), col("LIQTTR"), col("LIQTIN"), col("IMUOM1"), col("LIITM"), col("in0._upt_").alias("_upt_"))
