from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd.config.ConfigStore import *
from sap_md_matl_hmd.udfs.UDFs import *

def INOB_DEDUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window.partitionBy("OBJEK").orderBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.currentRow))
        )\
        .withColumn(
          "count",
          count("*")\
            .over(Window\
            .partitionBy("OBJEK")\
            .orderBy(lit(1))\
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
        )\
        .filter(col("row_number") == col("count"))\
        .drop("row_number")\
        .drop("count")
