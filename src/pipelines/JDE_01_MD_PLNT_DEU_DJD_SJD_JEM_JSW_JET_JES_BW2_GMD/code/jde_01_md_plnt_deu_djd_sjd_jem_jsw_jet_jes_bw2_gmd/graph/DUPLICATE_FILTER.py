from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.config.ConfigStore import *
from jde_01_md_plnt_deu_djd_sjd_jem_jsw_jet_jes_bw2_gmd.udfs.UDFs import *

def DUPLICATE_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
