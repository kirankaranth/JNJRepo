from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.config.ConfigStore import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.udfs.UDFs import *

def MD_NEWTABLE_SWAP(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
        col("RECV_HRS_MON_PM_UNTL"), 
        col("RECV_HRS_TUE_MORNING_FROM"), 
        col("RCPT_TIMES_TUE_MORNING_UNTL"), 
        col("RECV_HRS_TUE_PM_FROM"), 
        col("RECV_HRS_TUE_PM_UNTL"), 
        col("RECV_HRS_WED_MORNING_FROM"), 
        col("RECV_HRS_WED_MORNING_UNTL"), 
        col("RECV_HRS_WED_PM_FROM"), 
        col("RECV_HRS_WED_PM_UNTL"), 
        col("RECV_HRS_THU_MORNING_FROM"), 
        col("RECV_HRS_THU_MORNING_UNTL"), 
        col("RECV_HRS_THU_PM_FROM"), 
        col("RECV_HRS_THU_PM_UNTL"), 
        col("RECV_HRS_FRI_MORNING_FROM"), 
        col("RECV_HRS_FRI_MORNING_UNTL"), 
        col("RECV_HRS_FRI_PM_FROM"), 
        col("RECV_HRS_FRI_PM_UNTL"), 
        col("RECV_HRS_SAT_MORNING_FROM"), 
        col("RECV_HRS_SAT_MORNING_UNTL"), 
        col("RECV_HRS_SAT_PM_FROM"), 
        col("RECV_HRS_SAT_PM_UNTL"), 
        col("RECV_HRS_SUN_MORNING_FROM"), 
        col("RECV_HRS_SUN_MORNING_UNTL"), 
        col("RECV_HRS_SUN_PM_FROM"), 
        col("RECV_HRS_SUN_PM_UNTL"), 
        col("DFLT_UNLOADING_PT"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
