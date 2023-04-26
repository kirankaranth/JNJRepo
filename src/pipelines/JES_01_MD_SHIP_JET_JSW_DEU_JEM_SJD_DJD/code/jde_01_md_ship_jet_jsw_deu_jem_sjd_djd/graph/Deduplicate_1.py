from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.config.ConfigStore import *
from jde_01_md_ship_jet_jsw_deu_jem_sjd_djd.udfs.UDFs import *

def Deduplicate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window\
            .partitionBy("SRC_SYS_CD", "SHIP_NUM", "DELV_TYP_CD", "DOC_REF_NUM", "CO_CD", "DELV_LINE_NBR")\
            .orderBy(lit(1))\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
