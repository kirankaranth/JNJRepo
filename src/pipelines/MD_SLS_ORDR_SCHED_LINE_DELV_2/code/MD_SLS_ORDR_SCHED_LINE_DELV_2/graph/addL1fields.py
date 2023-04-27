from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_SLS_ORDR_SCHED_LINE_DELV_2.config.ConfigStore import *
from MD_SLS_ORDR_SCHED_LINE_DELV_2.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SCHED_LINE_ITM_NUM', SCHED_LINE_ITM_NUM, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SCHED_LINE_NUM', SCHED_LINE_NUM, 'CO_CD', CO_CD, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SCHED_LINE_ITM_NUM', SCHED_LINE_ITM_NUM, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SCHED_LINE_NUM', SCHED_LINE_NUM, 'CO_CD', CO_CD, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
