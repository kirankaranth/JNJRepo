from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *

def Filter_cabn(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(col("ATNAM").isin(lit("SPEC_REV_LEVEL"), lit("MATERIAL_SPEC")))
