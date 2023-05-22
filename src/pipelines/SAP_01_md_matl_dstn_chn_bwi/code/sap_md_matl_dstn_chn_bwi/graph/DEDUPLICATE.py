from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_dstn_chn_bwi.config.ConfigStore import *
from sap_md_matl_dstn_chn_bwi.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number().over(Window.partitionBy("SRC_SYS_CD", "MATL_NUM", "SL_ORG_NUM", "DSTR_CHNL_CD").orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
