from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_own_expln_for_term_of_pmt.config.ConfigStore import *
from sap_md_own_expln_for_term_of_pmt.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("L1_deleted_", col("_deleted_"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("l1_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD)"))))\
        .withColumn("L1name_SPRAS", col("SPRAS"))\
        .withColumn("L1name_ZTERM", col("ZTERM"))\
        .withColumn("L1name_ZTAGG", col("ZTAGG"))\
        .withColumn("L1name_TEXT1", col("TEXT1"))
