from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut.config.ConfigStore import *
from sap_md_matl_valut.udfs.UDFs import *

def SELECT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("VALUT_AREA_CD"), 
        col("VALUT_TYPE_CD"), 
        col("PRC_CNTL_IND"), 
        col("TOT_STK_QTY"), 
        col("TOT_VAL_AMT"), 
        col("MVG_AVG_PRC_AMT"), 
        col("PRC_AMT"), 
        col("PRC_UNIT_NBR"), 
        col("VALUT_CLS_CD"), 
        col("BASE_UOM_CD"), 
        col("DEL_FL"), 
        col("MATL_USG"), 
        col("MATL_ORIG"), 
        col("INHS_PRODCD"), 
        col("CST_ELMNT_ORIG_GRP"), 
        col("COST_OVHD_GRP"), 
        col("TMST"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
