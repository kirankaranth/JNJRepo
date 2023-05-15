from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_jde_bw2.config.ConfigStore import *
from jde_md_sls_ordr_hist_jde_bw2.udfs.UDFs import *

def F42199(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"bw2.f42199")
