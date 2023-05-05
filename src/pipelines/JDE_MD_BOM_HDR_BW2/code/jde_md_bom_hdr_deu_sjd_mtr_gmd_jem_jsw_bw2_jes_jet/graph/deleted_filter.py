from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.config.ConfigStore import *
from jde_md_bom_hdr_deu_sjd_mtr_gmd_jem_jsw_bw2_jes_jet.udfs.UDFs import *

def deleted_filter(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
