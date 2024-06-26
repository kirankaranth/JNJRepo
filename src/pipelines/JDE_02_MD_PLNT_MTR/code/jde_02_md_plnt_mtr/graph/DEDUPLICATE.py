from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_02_md_plnt_mtr.config.ConfigStore import *
from jde_02_md_plnt_mtr.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number().over(Window.partitionBy("ALAN8", "ALEFTB").orderBy(col("ALAN8").asc(), col("ALEFTB").desc()))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
