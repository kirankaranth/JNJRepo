from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.config.ConfigStore import *
from jde_md_sls_ordr_line_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr.udfs.UDFs import *

def JDE_F4211(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable1}")
