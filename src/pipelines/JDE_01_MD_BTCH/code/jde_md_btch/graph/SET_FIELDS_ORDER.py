from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_btch.config.ConfigStore import *
from jde_md_btch.udfs.UDFs import *

def SET_FIELDS_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SRC_TBL_NM"), 
        col("MATL_NUM"), 
        col("BTCH_NUM"), 
        col("PLNT_CD"), 
        col("CRT_DTTM"), 
        col("CHG_DTTM"), 
        col("AVAIL_DTTM"), 
        col("BTCH_EXP_DTTM"), 
        col("BTCH_STS_CD"), 
        col("BTCH_LAST_STS_DTTM"), 
        col("SUP_NUM"), 
        col("SUP_BTCH_NUM"), 
        col("BTCH_LAST_GR_DTTM"), 
        col("BTCH_MFG_DTTM"), 
        col("BTCH_TYPE"), 
        col("LOT_GRADE"), 
        col("SHRT_MATL_NUM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_md5_"), 
        col("_pk_"), 
        col("_deleted_")
    )
