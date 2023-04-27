from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.config.ConfigStore import *
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.udfs.UDFs import *

def JDE_F43121_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
