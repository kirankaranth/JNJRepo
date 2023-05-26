from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_geu.config.ConfigStore import *
from sap_md_mfg_order_geu.udfs.UDFs import *

def INTERIM_LU(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("OBJNR", col("OBJNR"))\
        .withColumn("STAT", col("STAT"))\
        .withColumn("TXT04", lookup("LU_TXT04", col("STAT")).getField("TXT04"))
