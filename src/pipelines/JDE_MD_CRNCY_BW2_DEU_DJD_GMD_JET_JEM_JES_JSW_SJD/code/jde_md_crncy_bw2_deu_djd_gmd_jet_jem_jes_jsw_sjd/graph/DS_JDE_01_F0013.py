from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.udfs.UDFs import *

def DS_JDE_01_F0013(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"bw2.f0013")
