from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from ppln_sk_test.config.ConfigStore import *
from ppln_sk_test.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("SRC_SYS_CD2", lit(Config.sourceSystem))
