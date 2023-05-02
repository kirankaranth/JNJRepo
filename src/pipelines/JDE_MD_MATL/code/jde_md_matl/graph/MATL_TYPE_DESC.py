from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl.config.ConfigStore import *
from jde_md_matl.udfs.UDFs import *

def MATL_TYPE_DESC(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
