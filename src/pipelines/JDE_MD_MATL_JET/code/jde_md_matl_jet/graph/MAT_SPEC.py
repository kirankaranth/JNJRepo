from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def MAT_SPEC(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(expr(Config.MAT_SPEC_Filter))
