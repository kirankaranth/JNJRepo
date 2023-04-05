from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_po_sched_line_delv.config.ConfigStore import *
from sap_01_md_po_sched_line_delv.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("PO_NUM"), 
        col("PO_LINE_NBR"), 
        col("DELV_SCHED_CNT_NBR"), 
        col("DELV_DTTM"), 
        col("SCHD_QTY"), 
        col("RECV_QTY"), 
        col("STK_TFR_RECV_QTY"), 
        col("MRP_ADJ_QTY"), 
        col("STAT_DELV_DTTM"), 
        col("CMT_QTY"), 
        col("ORDER_TYPE"), 
        col("ORDER_CO"), 
        col("ORDER_SUF"), 
        col("PREV_QTY"), 
        col("CMT_DTTM"), 
        col("CAT_OF_DELV_DT"), 
        col("BTCH_NUM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_").alias("_l1_upt_\u00A0"), 
        col("_deleted_").alias("_deleted_\u00A0")
    )
