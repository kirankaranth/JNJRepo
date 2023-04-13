from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jes_01_md_ship_jet_jsw_deu_jem_sjd_djd.config.ConfigStore import *
from jes_01_md_ship_jet_jsw_deu_jem_sjd_djd.udfs.UDFs import *

def JDE_F43121(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.sourceTable1}")
