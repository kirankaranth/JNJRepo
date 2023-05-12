from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def COLEDG_COCSIN(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("COLEDG") == lit("07")) & (trim(col("COCSIN")) == lit("I"))) & (col("_deleted_") == lit("F")))
    )
