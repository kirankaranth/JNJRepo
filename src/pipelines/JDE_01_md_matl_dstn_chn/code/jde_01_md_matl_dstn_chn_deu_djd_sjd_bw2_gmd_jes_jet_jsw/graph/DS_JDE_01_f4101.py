from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw.config.ConfigStore import *
from jde_01_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw.udfs.UDFs import *

def DS_JDE_01_f4101(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
