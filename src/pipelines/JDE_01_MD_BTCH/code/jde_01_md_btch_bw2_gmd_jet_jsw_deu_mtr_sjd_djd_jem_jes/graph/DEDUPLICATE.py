from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy("SRC_SYS_CD", "SRC_TBL_NM", "MATL_NUM", "BTCH_NUM", "PLNT_CD", "SHRT_MATL_NUM")\
            .orderBy(lit(1))\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
