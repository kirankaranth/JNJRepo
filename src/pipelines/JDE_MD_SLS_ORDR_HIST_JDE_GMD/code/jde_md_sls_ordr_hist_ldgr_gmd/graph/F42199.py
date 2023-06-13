from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_ldgr_gmd.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_gmd.udfs.UDFs import *

def F42199(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.f42199")
