from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_mtr_bw2_gmd_jes.config.ConfigStore import *
from jde_01_md_ship_mtr_bw2_gmd_jes.udfs.UDFs import *

def SET_FIELD_ORDER_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SHIP_NUM"), 
        col("DELV_TYP_CD"), 
        col("DOC_REF_NUM"), 
        col("CO_CD"), 
        col("DELV_LINE_NBR"), 
        col("ACTL_SHIP_DTTM"), 
        col("PLAN_SHIP_DTTM"), 
        col("SLS_ORDR_CAR_CD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
