from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def Reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("LIITM"), 
        col("LIMCU"), 
        col("LILOCN"), 
        col("LILOTN"), 
        col("LILOTS"), 
        col("LIPBIN"), 
        col("LIPQOH"), 
        col("LIQTTR"), 
        col("LIQTIN"), 
        col("_upt_")
    )
