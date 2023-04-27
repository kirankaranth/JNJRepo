from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_sched_line_delv_01.config.ConfigStore import *
from sap_md_po_sched_line_delv_01.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PO_NUM", trim(col("ebeln")))\
        .withColumn("PO_LINE_NBR", trim(col("ebelp")))\
        .withColumn("DELV_SCHED_CNT_NBR", trim(col("etenr")))\
        .withColumn(
          "DELV_DTTM",
          when((col("eindt") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("eindt"), "yyyyMMdd"))
        )\
        .withColumn("SCHD_QTY", trim(col("menge")).cast(DecimalType(18, 4)))\
        .withColumn("RECV_QTY", trim(col("wemng")).cast(DecimalType(18, 4)))\
        .withColumn("STK_TFR_RECV_QTY", trim(col("glmng")).cast(DecimalType(18, 4)))\
        .withColumn("MRP_ADJ_QTY", trim(col("dabmg")).cast(DecimalType(18, 4)))\
        .withColumn(
          "STAT_DELV_DTTM",
          when((col("slfdt") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("slfdt"), "yyyyMMdd"))
        )\
        .withColumn("CMT_QTY", trim(col("mng02")).cast(DecimalType(18, 4)))\
        .withColumn(
          "ORDER_TYPE",
          lit(
            "#"
          )
        )\
        .withColumn(
          "ORDER_CO",
          lit(
            "#"
          )
        )\
        .withColumn(
          "ORDER_SUF",
          lit(
            "#"
          )
        )\
        .withColumn("PREV_QTY", trim(col("ameng")).cast(DecimalType(18, 4)))\
        .withColumn(
          "CMT_DTTM",
          when((col("dat01") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("dat01"), "yyyyMMdd"))
        )\
        .withColumn("CAT_OF_DELV_DT", trim(col("lpein")))\
        .withColumn("BTCH_NUM", trim(col("charg")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'DELV_SCHED_CNT_NBR', DELV_SCHED_CNT_NBR, 'ORDER_TYPE', ORDER_TYPE, 'ORDER_CO', ORDER_CO, 'ORDER_SUF', ORDER_SUF)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'DELV_SCHED_CNT_NBR', DELV_SCHED_CNT_NBR, 'ORDER_TYPE', ORDER_TYPE, 'ORDER_CO', ORDER_CO, 'ORDER_SUF', ORDER_SUF)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())
