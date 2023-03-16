from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from generic_delete.config.ConfigStore import *
from generic_delete.udfs.UDFs import *

def Generic_Delete(spark: SparkSession):
    spark.sql(f"TRUNCATE TABLE {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE};")

    return 
