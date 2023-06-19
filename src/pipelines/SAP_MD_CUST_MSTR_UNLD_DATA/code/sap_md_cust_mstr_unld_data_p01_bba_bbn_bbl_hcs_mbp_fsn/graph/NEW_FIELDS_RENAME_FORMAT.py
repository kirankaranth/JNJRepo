from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.config.ConfigStore import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CUST_NUM", col("KUNNR"))\
        .withColumn("UNLOADING_PT", col("ABLAD"))\
        .withColumn("SEQ_NUM_FOR_UNLOADING_PT", trim(col("LFDNR")))\
        .withColumn("CUST_FCTRY_CAL", trim(col("KNFAK")))\
        .withColumn("GOODS_RECV_HRS_ID", trim(col("WANID")))\
        .withColumn("NOT_IN_USE_VAL_1", trim(col("TPQUA")))\
        .withColumn("NOT_IN_USE_VAL_2", trim(col("TPGRP")))\
        .withColumn("NOT_IN_USE_VAL_3", trim(col("STZKL")))\
        .withColumn("NOT_IN_USE_VAL_4", trim(col("STZZU")))\
        .withColumn("RCPT_TIMES_MON_MORNING_FROM", trim(col("MOAB1")))\
        .withColumn("RECV_HRS_MON_MORNING_UNTL", trim(col("MOBI1")))\
        .withColumn("RECV_HRS_MON_PM_FROM", trim(col("MOAB2")))\
        .withColumn("RECV_HRS_MON_PM_UNTL", trim(col("MOBI2")))\
        .withColumn("RECV_HRS_TUE_MORNING_FROM", trim(col("DIAB1")))\
        .withColumn("RCPT_TIMES_TUE_MORNING_UNTL", trim(col("DIBI1")))\
        .withColumn("RECV_HRS_TUE_PM_FROM", trim(col("DIAB2")))\
        .withColumn("RECV_HRS_TUE_PM_UNTL", trim(col("DIBI2")))\
        .withColumn("RECV_HRS_WED_MORNING_FROM", trim(col("MIAB1")))\
        .withColumn("RECV_HRS_WED_MORNING_UNTL", trim(col("MIBI1")))\
        .withColumn("RECV_HRS_WED_PM_FROM", trim(col("MIAB2")))\
        .withColumn("RECV_HRS_WED_PM_UNTL", trim(col("MIBI2")))\
        .withColumn("RECV_HRS_THU_MORNING_FROM", trim(col("DOAB1")))\
        .withColumn("RECV_HRS_THU_MORNING_UNTL", trim(col("DOBI1")))\
        .withColumn("RECV_HRS_THU_PM_FROM", trim(col("DOAB2")))\
        .withColumn("RECV_HRS_THU_PM_UNTL", trim(col("DOBI2")))\
        .withColumn("RECV_HRS_FRI_MORNING_FROM", trim(col("FRAB1")))\
        .withColumn("RECV_HRS_FRI_MORNING_UNTL", trim(col("FRBI1")))\
        .withColumn("RECV_HRS_FRI_PM_FROM", trim(col("FRAB2")))\
        .withColumn("RECV_HRS_FRI_PM_UNTL", trim(col("FRBI2")))\
        .withColumn("RECV_HRS_SAT_MORNING_FROM", trim(col("SAAB1")))\
        .withColumn("RECV_HRS_SAT_MORNING_UNTL", trim(col("SABI1")))\
        .withColumn("RECV_HRS_SAT_PM_FROM", trim(col("SAAB2")))\
        .withColumn("RECV_HRS_SAT_PM_UNTL", trim(col("SABI2")))\
        .withColumn("RECV_HRS_SUN_MORNING_FROM", trim(col("SOAB1")))\
        .withColumn("RECV_HRS_SUN_MORNING_UNTL", trim(col("SOBI1")))\
        .withColumn("RECV_HRS_SUN_PM_FROM", trim(col("SOAB2")))\
        .withColumn("RECV_HRS_SUN_PM_UNTL", trim(col("SOBI2")))\
        .withColumn("DFLT_UNLOADING_PT", trim(col("DEFAB")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM, 'UNLOADING_PT', UNLOADING_PT)"))
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM, 'UNLOADING_PT', UNLOADING_PT)"))
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
