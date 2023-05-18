from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_deu.config.ConfigStore import *
from jde_md_matl_loc_deu.udfs.UDFs import *

def DUP_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
