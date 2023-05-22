from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.config.ConfigStore import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.udfs.UDFs import *

def Join_JDE(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.MCAN8") == col("in1.ALAN8")), "left_outer")\
        .select(col("in0.MCMCU").alias("MCMCU"), col("in0.MCDC").alias("MCDC"), col("in1.ALCTR").alias("ALCTR"), col("in0.MCSTYL").alias("MCSTYL"), col("in0.MCRP02").alias("MCRP02"), col("in0.MCCO").alias("MCCO"), col("in1.ALADDZ").alias("ALADDZ"), col("in1.ALCTY1").alias("ALCTY1"), col("in0._upt_").alias("_upt_"))
