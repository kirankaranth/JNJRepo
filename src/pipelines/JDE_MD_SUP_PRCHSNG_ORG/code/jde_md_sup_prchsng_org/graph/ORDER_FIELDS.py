from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *

def ORDER_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SUP_NUM"), 
        col("PRCHSNG_ORG_NUM"), 
        col("CRT_ON_DTTM"), 
        col("CRNCY_CD"), 
        col("INCOTERM1_CD"), 
        col("INCOTERM2_CD"), 
        col("EVAL_RCPT_SETLM_CD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_deleted_")
    )
