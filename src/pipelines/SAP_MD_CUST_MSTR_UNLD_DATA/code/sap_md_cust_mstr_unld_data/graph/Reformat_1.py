from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_cust_mstr_unld_data.config.ConfigStore import *
from sap_md_cust_mstr_unld_data.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("CUST_NUM"), 
        col("UNLOADING_PT"), 
        col("SEQ_NUM_FOR_UNLOADING_PT"), 
        col("CUST_FCTRY_CAL"), 
        col("GOODS_RECV_HRS_ID"), 
        col("NOT_IN_USE_VAL_1"), 
        col("NOT_IN_USE_VAL_2"), 
        col("NOT_IN_USE_VAL_3"), 
        col("NOT_IN_USE_VAL_4"), 
        col("RCPT_TIMES_MON_MORNING_FROM"), 
        col("RECV_HRS_MON_MORNING_UNTL"), 
        col("RECV_HRS_MON_PM_FROM"), 
        col("RECV_HRS_MON_PM_UNTL")
    )
