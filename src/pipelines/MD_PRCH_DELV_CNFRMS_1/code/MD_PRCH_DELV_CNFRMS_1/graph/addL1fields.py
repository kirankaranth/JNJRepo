from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_PRCH_DELV_CNFRMS_1.config.ConfigStore import *
from MD_PRCH_DELV_CNFRMS_1.udfs.UDFs import *

def addL1fields(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'CNFRM_SEQ_NBR', CNFRM_SEQ_NBR, 'CNFRM_CAT_CD', CNFRM_CAT_CD, 'CO_CD', CO_CD, 'ORDER_SUF', ORDER_SUF)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'CNFRM_SEQ_NBR', CNFRM_SEQ_NBR, 'CNFRM_CAT_CD', CNFRM_CAT_CD, 'CO_CD', CO_CD, 'ORDER_SUF', ORDER_SUF)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
