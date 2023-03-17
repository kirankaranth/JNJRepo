from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from generic_swap.config.ConfigStore import *
from generic_swap.udfs.UDFs import *

def GENERIC_SWAP(spark: SparkSession):
    spark.sql(
        f"ALTER TABLE {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE} RENAME TO {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE}_TEMP;"
    )
    spark.sql(
        f"ALTER TABLE {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE}_SWAP RENAME TO {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE};"
    )
    spark.sql(
        f"ALTER TABLE {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE}_TEMP RENAME TO {Config.CATALOG}.{Config.DATABASE}.{Config.TABLE}_SWAP;"
    )

    return 
