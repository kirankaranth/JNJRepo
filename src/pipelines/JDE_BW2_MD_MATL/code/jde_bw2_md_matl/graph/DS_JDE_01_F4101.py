from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *

def DS_JDE_01_F4101(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.DBTABLE}")
