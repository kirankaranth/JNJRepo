from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_jem.config.ConfigStore import *
from jde_md_matl_loc_jem.udfs.UDFs import *

def NEW_FIELDS_PK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("iblitm"))\
        .withColumn("PLNT_CD", col("ibmcu"))
