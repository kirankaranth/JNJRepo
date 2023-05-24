from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_own_expln_for_term_of_pmt.config.ConfigStore import *
from sap_md_own_expln_for_term_of_pmt.udfs.UDFs import *

def SELECT_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("LANG_KEY"), 
        col("TERM_OF_PMT_KEY"), 
        col("DAY_LMT"), 
        col("OWN_EXPLN_OF_TERM_OF_PMT"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("l1_pk_").alias("_pk_"), 
        col("_pk_md5_"), 
        col("L1_deleted_").alias("_deleted_")
    )
