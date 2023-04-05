from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in0.UMITM") == col("in1.IMITM")), "inner")
