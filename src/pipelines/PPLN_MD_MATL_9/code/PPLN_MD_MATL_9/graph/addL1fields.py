from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MD_MATL_9.config.ConfigStore import *
from PPLN_MD_MATL_9.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn(
          "_pk_",
          to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'SHRT_MATL_NUM', SHRT_MATL_NUM)"))
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'SHRT_MATL_NUM', SHRT_MATL_NUM)")
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())
