from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *

def F4105(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.F4105}")
