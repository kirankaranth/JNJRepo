from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.config.ConfigStore import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.udfs.UDFs import *

def DS_JDE_F0101_adt(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable}")
