from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_gmd_deu_jem_jes_djd.config.ConfigStore import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.udfs.UDFs import *

def DUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("row_number", row_number().over(Window.partitionBy("SRC_SYS_CD", "MATL_NUM", "PLNT_CD").orderBy(lit(1))))\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
