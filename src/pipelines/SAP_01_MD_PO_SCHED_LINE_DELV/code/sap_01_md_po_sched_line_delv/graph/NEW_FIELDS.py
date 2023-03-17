from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_po_sched_line_delv.config.ConfigStore import *
from sap_01_md_po_sched_line_delv.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.SRC_SYS_CD))\
        .withColumn("PO_NUM", trim(col("ebeln")))\
        .withColumn("PO_LINE_NBR", trim(col("ebelp")))\
        .withColumn("DELV_SCHED_CNT_NBR", trim(col("etenr")))\
        .withColumn(
          "DELV_DTTM",
          when((col("eindt") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("eindt"), "yyyyMMdd"))
        )\
        .withColumn("SCHD_QTY", trim(col("menge")))\
        .withColumn("RECV_QTY", trim(col("wemng")))\
        .withColumn("STK_TFR_RECV_QTY", trim(col("glmng")))\
        .withColumn("MRP_ADJ_QTY", trim(col("dabmg")))\
        .withColumn(
          "STAT_DELV_DTTM",
          when((col("slfdt") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("slfdt"), "yyyyMMdd"))
        )\
        .withColumn("CMT_QTY", trim(col("mng02")))\
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
        .withColumn("PREV_QTY", trim(col("ameng")))\
        .withColumn(
          "CMT_DTTM",
          when((col("dat01") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("dat01"), "yyyyMMdd"))
        )\
        .withColumn("CAT_OF_DELV_DT", trim(col("lpein")))\
        .withColumn("BTCH_NUM", trim(col("charg")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", to_timestamp(current_timestamp(), "yyyyMMdd"))\
        .withColumn("DAI_UPDT_DTTM", to_timestamp(current_timestamp(), "yyyyMMdd"))
