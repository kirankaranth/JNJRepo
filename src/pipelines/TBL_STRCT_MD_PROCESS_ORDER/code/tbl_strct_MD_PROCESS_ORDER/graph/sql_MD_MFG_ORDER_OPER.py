from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PROCESS_ORDER.config.ConfigStore import *
from tbl_strct_MD_PROCESS_ORDER.udfs.UDFs import *

def sql_MD_MFG_ORDER_OPER(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as GENL_CNTR_ORDR,\ncast('' as String) as RTG_NUM_OPS_ORDR,\ncast('' as int) as OPR_SCRAP,\ncast('' as String) as BASE_QTY,\ncast('' as String) as UOM_ACTV_OPR,\ncast('' as int) as OPR_QTY,\ncast('' as int) as MIN_SEND_AHD_QTY,\ncast('' as String) as DENOM_CNV_RTG_AND_OP_UNIT_MEAS,\ncast('' as decimal(18,4)) as NUMRTR_CNV_TASK_LIST_AND_OPER_UNIT_MEAS,\ncast('' as String) as ACTV1_UOM_CD,\ncast('' as String) as ACTV2_UOM_CD,\ncast('' as String) as ACTV3_UOM_CD,\ncast('' as String) as ACTV4_UOM_CD,\ncast('' as String) as ACTV5_UOM_CD,\ncast('' as String) as ACTV6_UOM_CD,\ncast('' as decimal(18,4)) as ACTV1_QTY,\ncast('' as decimal(18,4)) as ACTV2_QTY,\ncast('' as decimal(18,4)) as ACTV3_QTY,\ncast('' as decimal(18,4)) as ACTV4_QTY,\ncast('' as decimal(18,4)) as ACTV5_QTY,\ncast('' as decimal(18,4)) as ACTV6_QTY,\ncast('' as String) as UNIT_MAX_WAIT_TIME,\ncast('' as String) as UNIT_REQ_WAIT_TIME,\ncast('' as String) as UNIT_MIN_PRCSG_TIME,\ncast('' as String) as UNIT_MIN_OVL_TIME,\ncast('' as String) as UNIT_MIN_MOVE_TIME,\ncast('' as String) as UNIT_STD_MOVE_TIME,\ncast('' as String) as UNIT_MIN_QUE_TIME,\ncast('' as String) as UNIT_STD_QUE_TIME,\ncast('' as String) as MAX_WAIT_TIME,\ncast('' as String) as MIN_WAIT_TIME,\ncast('' as String) as MIN_PRCSG_TIME,\ncast('' as String) as MIN_OVL_TIME,\ncast('' as String) as MIN_MOVE_TIME,\ncast('' as String) as STD_MOVE_TIME,\ncast('' as String) as MIN_QUE_TIME,\ncast('' as String) as STD_QUE_TIME,\ncast('' as timestamp) as LTST_SCHD_STRT_DTTM,\ncast('' as timestamp) as LTST_END_STRT_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
