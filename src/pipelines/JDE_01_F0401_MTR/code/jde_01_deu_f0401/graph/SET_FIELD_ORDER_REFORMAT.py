from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SUP_NUM"), 
        col("PRCHSNG_ORG_NUM"), 
        col("CRT_ON_DTTM"), 
        col("PRCH_BLK_IND"), 
        col("DEL_IND"), 
        col("CRNCY_CD"), 
        col("INCOTERM1_CD"), 
        col("INCOTERM2_CD"), 
        col("EVAL_RCPT_SETLM_CD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
