from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_bom.config.ConfigStore import *
from jde_01_md_matl_bom.udfs.UDFs import *

def JDE_f3002_f3002_adt(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.sourceTable}")
