from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *

def JDE_F0101(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.{Config.sourceTable1}")
