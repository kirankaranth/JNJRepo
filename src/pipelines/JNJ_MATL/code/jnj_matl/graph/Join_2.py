from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jnj_matl.config.ConfigStore import *
from jnj_matl.udfs.UDFs import *

def Join_2(spark: SparkSession, ausp: DataFrame, inob: DataFrame, cabn: DataFrame) -> DataFrame:
    return ausp\
        .alias("ausp")\
        .join(inob.alias("inob"), (col("ausp.objek") == col("inob.CUOBJ")), "inner")\
        .join(cabn.alias("cabn"), (col("ausp.ATINN") == col("cabn.ATINN")), "inner")\
        .select(col("inob.OBJEK").alias("OBJEK"), col("ausp.KLART").alias("KLART"), col("cabn.ATNAM").alias("ATNAM"), col("ausp.ATWRT").alias("ATWRT"))
