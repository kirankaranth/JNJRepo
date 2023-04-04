from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.config.ConfigStore import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.udfs.UDFs import *

def JDE_F3002_ADT(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.DBNAME}.{Config.TBNAME} WHERE _deleted_ = 'F'")
