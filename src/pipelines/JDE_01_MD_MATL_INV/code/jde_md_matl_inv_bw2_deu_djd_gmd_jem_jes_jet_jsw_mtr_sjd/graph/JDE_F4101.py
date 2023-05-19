from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sjd.config.ConfigStore import *
from jde_md_matl_inv_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sjd.udfs.UDFs import *

def JDE_F4101(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.DBTABLE}")
