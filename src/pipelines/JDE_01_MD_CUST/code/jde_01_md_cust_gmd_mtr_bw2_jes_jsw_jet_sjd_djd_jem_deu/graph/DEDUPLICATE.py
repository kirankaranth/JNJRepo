from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.config.ConfigStore import *
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window.partitionBy("CUST_NUM").orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
