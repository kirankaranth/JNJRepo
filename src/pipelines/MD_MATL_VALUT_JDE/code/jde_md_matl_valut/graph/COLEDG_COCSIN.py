from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *

def COLEDG_COCSIN(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (((col("COLEDG") == lit("07")) & (trim(col("COCSIN")) == lit("I"))) & (col("_deleted_") == lit("F")))
    )
