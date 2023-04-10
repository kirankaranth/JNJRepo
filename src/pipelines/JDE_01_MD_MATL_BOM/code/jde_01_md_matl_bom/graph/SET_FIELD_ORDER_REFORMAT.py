from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_bom.config.ConfigStore import *
from jde_01_md_matl_bom.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("PLNT_CD"), 
        col("BOM_USG_CD"), 
        col("BOM_NUM"), 
        col("ALT_BOM_NUM"), 
        col("CRT_DTTM"), 
        col("CHG_DTTM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_pk_"), 
        col("_deleted_"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_")
    )
