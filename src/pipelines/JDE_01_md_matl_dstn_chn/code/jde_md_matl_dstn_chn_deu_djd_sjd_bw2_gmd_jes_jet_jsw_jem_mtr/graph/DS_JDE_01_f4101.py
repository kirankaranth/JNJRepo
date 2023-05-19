from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.config.ConfigStore import *
from jde_md_matl_dstn_chn_deu_djd_sjd_bw2_gmd_jes_jet_jsw_jem_mtr.udfs.UDFs import *

def DS_JDE_01_f4101(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
