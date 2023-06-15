from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def T_O_MAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(expr(Config.TYPE_O_MAT_Filter))
