from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from ppln_sk_test.config.ConfigStore import *
from ppln_sk_test.udfs.UDFs import *

def Script_1(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(f"""
    SELECT * 
    
    FROM {Config.sourceSystem}.mara
    limit 10
""")

    return out0
