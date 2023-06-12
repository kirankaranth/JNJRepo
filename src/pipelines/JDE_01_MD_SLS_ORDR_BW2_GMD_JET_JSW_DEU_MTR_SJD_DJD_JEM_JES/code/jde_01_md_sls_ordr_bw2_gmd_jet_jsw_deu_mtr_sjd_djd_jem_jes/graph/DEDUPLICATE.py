from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_sls_ordr_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy("SHKCOO", "SRC_SYS_CD", "SHDOCO", "SHDCTO")\
            .orderBy(lit(1))\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .withColumn(
          "count",
          count("*")\
            .over(Window\
            .partitionBy("SHKCOO", "SRC_SYS_CD", "SHDOCO", "SHDCTO")\
            .orderBy(lit(1))\
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        )\
        .filter(col("row_number") == col("count"))\
        .drop("row_number")\
        .drop("count")
