from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_mtr_bw2_gmd_jes.config.ConfigStore import *
from jde_01_md_ship_mtr_bw2_gmd_jes.udfs.UDFs import *

def UNION(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.unionAll(in1)
