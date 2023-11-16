from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *

def DS_SAP_01_MAKT_JNJ(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"bbn.makt")
