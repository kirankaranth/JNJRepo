from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_btch.config.ConfigStore import *
from jde_01_md_btch.udfs.UDFs import *

def JDE_F4108(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.f4108")
