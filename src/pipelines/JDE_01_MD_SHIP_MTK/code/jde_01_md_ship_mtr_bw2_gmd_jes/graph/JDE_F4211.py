from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_mtr_bw2_gmd_jes.config.ConfigStore import *
from jde_01_md_ship_mtr_bw2_gmd_jes.udfs.UDFs import *

def JDE_F4211(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.sourceTable}")