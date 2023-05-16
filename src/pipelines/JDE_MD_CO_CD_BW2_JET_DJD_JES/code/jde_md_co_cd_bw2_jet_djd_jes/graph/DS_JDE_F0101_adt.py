from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_co_cd_bw2_jet_djd_jes.config.ConfigStore import *
from jde_md_co_cd_bw2_jet_djd_jes.udfs.UDFs import *

def DS_JDE_F0101_adt(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"bw2.f0101_adt")
