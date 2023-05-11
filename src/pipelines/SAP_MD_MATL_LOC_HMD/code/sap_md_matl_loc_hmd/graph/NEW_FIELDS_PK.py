from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_hmd.config.ConfigStore import *
from sap_md_matl_loc_hmd.udfs.UDFs import *

def NEW_FIELDS_PK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))
