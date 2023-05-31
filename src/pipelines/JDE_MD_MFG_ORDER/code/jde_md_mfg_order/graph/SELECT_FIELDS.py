from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order.config.ConfigStore import *
from jde_md_mfg_order.udfs.UDFs import *

def SELECT_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MFG_ORDR_TYP_CD"), 
        col("MFG_ORDR_NUM"), 
        col("CNFRMD_SCRP_QTY"), 
        col("RTNG_TYP_CD"), 
        col("MFG_ORDR_STTS_CD"), 
        col("BOM_CAT_CD"), 
        col("PLNT_CD"), 
        col("DIR_SHIP_CUST_ADDR_NUM"), 
        col("MFR_OF_FNL_SKU"), 
        col("CHG_DTTM"), 
        col("CRTD_DTTM"), 
        col("ACT_RLSE_DTTM"), 
        col("PLAN_RLSE_DTTM"), 
        col("SCH_REL_DTTM"), 
        col("CNFRMD_END_DTTM"), 
        col("PRDTN_END_DTTM"), 
        col("END_DTTM"), 
        col("SCHD_END_DTTM"), 
        col("STRT_DTTM"), 
        col("SCHD_STRT_DTTM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_l0_deleted_").alias("_deleted_")
    )
