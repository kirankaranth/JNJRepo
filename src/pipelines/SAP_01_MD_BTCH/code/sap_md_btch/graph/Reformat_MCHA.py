from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_btch.config.ConfigStore import *
from sap_md_btch.udfs.UDFs import *

def Reformat_MCHA(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SRC_TBL_NM"), 
        col("MATL_NUM"), 
        col("BTCH_NUM"), 
        col("PLNT_CD"), 
        col("DEL_IND"), 
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
        col("SUI_IND"), 
        col("LOT_GRADE"), 
        col("PARENT_CODE"), 
        col("SHRT_MATL_NUM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_deleted_")
    )
