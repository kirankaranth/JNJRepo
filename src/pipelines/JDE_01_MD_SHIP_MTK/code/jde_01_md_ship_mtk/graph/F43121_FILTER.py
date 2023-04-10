from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_mtk.config.ConfigStore import *
from jde_01_md_ship_mtk.udfs.UDFs import *

def F43121_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
