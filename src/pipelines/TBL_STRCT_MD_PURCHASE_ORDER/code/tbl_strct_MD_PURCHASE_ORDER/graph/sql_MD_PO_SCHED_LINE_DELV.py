from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PURCHASE_ORDER.config.ConfigStore import *
from tbl_strct_MD_PURCHASE_ORDER.udfs.UDFs import *

def sql_MD_PO_SCHED_LINE_DELV(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PO_NUM,\ncast('' as string) as PO_LINE_NBR,\ncast('' as string) as DELV_SCHED_CNT_NBR,\ncast('' as timestamp) as DELV_DTTM,\ncast('' as decimal(18,4)) as SCHD_QTY,\ncast('' as decimal(18,4)) as RECV_QTY,\ncast('' as decimal(18,4)) as STK_TFR_RECV_QTY,\ncast('' as decimal(18,4)) as MRP_ADJ_QTY,\ncast('' as timestamp) as STAT_DELV_DTTM,\ncast('' as decimal(18,4)) as CMT_QTY,\ncast('' as string) as ORDER_TYPE,\ncast('' as string) as ORDER_CO,\ncast('' as string) as ORDER_SUF,\ncast('' as decimal(18,4)) as PREV_QTY,\ncast('' as timestamp) as CMT_DTTM,\ncast('' as string) as CAT_OF_DELV_DT,\ncast('' as string) as BTCH_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
