from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_cust_mstr_unld_data.config.ConfigStore import *
from sap_md_cust_mstr_unld_data.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.DAI_ETL_ID))\
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
        .withColumn("RECV_HRS_MON_PM_UNTL", trim(col("MOBI2")))
