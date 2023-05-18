from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_jde_jes.config.ConfigStore import *
from jde_md_sls_ordr_hist_jde_jes.udfs.UDFs import *

def DELETED(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
