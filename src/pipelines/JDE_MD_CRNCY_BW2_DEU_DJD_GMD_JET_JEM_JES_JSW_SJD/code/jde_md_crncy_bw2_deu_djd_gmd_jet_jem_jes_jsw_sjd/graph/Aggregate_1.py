from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.agg()
