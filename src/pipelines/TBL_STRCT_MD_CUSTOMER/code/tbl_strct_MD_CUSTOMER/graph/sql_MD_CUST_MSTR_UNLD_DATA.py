from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def sql_MD_CUST_MSTR_UNLD_DATA(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CUST_NUM,\ncast('' as string) as UNLOADING_PT,\ncast('' as string) as SEQ_NUM_FOR_UNLOADING_PT,\ncast('' as string) as CUST_FCTRY_CAL,\ncast('' as string) as GOODS_RECV_HRS_ID,\ncast('' as string) as NOT_IN_USE_VAL_1,\ncast('' as string) as NOT_IN_USE_VAL_2,\ncast('' as string) as NOT_IN_USE_VAL_3,\ncast('' as string) as NOT_IN_USE_VAL_4,\ncast('' as string) as RCPT_TIMES_MON_MORNING_FROM,\ncast('' as string) as RECV_HRS_MON_MORNING_UNTL,\ncast('' as string) as RECV_HRS_MON_PM_FROM,\ncast('' as string) as RECV_HRS_MON_PM_UNTL,\ncast('' as string) as RECV_HRS_TUE_MORNING_FROM,\ncast('' as string) as RCPT_TIMES_TUE_MORNING_UNTL,\ncast('' as string) as RECV_HRS_TUE_PM_FROM,\ncast('' as string) as RECV_HRS_TUE_PM_UNTL,\ncast('' as string) as RECV_HRS_WED_MORNING_FROM,\ncast('' as string) as RECV_HRS_WED_MORNING_UNTL,\ncast('' as string) as RECV_HRS_WED_PM_FROM,\ncast('' as string) as RECV_HRS_WED_PM_UNTL,\ncast('' as string) as RECV_HRS_THU_MORNING_FROM,\ncast('' as string) as RECV_HRS_THU_MORNING_UNTL,\ncast('' as string) as RECV_HRS_THU_PM_FROM,\ncast('' as string) as RECV_HRS_THU_PM_UNTL,\ncast('' as string) as RECV_HRS_FRI_MORNING_FROM,\ncast('' as string) as RECV_HRS_FRI_MORNING_UNTL,\ncast('' as string) as RECV_HRS_FRI_PM_FROM,\ncast('' as string) as RECV_HRS_FRI_PM_UNTL,\ncast('' as string) as RECV_HRS_SAT_MORNING_FROM,\ncast('' as string) as RECV_HRS_SAT_MORNING_UNTL,\ncast('' as string) as RECV_HRS_SAT_PM_FROM,\ncast('' as string) as RECV_HRS_SAT_PM_UNTL,\ncast('' as string) as RECV_HRS_SUN_MORNING_FROM,\ncast('' as string) as RECV_HRS_SUN_MORNING_UNTL,\ncast('' as string) as RECV_HRS_SUN_PM_FROM,\ncast('' as string) as RECV_HRS_SUN_PM_UNTL,\ncast('' as string) as DFLT_UNLOADING_PT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
