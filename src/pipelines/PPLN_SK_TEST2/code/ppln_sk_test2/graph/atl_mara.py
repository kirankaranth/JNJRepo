from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from ppln_sk_test2.config.ConfigStore import *
from ppln_sk_test2.udfs.UDFs import *

def atl_mara(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.mara")
