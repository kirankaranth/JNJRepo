from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd.config.ConfigStore import *
from jde_md_matl_loc_jet_jsw_mtr_bw2_gmd_deu_jem_jes_sjd_djd.udfs.UDFs import *

def DUP_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
