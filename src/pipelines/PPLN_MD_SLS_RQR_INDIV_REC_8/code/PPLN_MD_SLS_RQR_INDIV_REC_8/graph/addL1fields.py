from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MD_SLS_RQR_INDIV_REC_8.config.ConfigStore import *
from PPLN_MD_SLS_RQR_INDIV_REC_8.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SLS_DOC', SLS_DOC, 'SLS_DOC_ITM', SLS_DOC_ITM, 'SCHED_LINE_NUM', SCHED_LINE_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SLS_DOC', SLS_DOC, 'SLS_DOC_ITM', SLS_DOC_ITM, 'SCHED_LINE_NUM', SCHED_LINE_NUM)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())
