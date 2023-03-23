from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from ppln_sk_test.config.ConfigStore import *
from ppln_sk_test.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("_pk_", to_json(expr("named_struct('field1_name', 'val1', 'field2_name', 'val2')")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('field1_name', 'val1', 'field2_name', 'val2')"))))\
        .withColumn("_l1_upt_", current_timestamp())
