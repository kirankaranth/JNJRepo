from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("PLNT_CD"), 
        col("PLNT_NM"), 
        col("CTRY_CD"), 
        col("PLNT_CAT_CD"), 
        col("RGN_CD"), 
        col("VALUT_AREA"), 
        col("CO_CD"), 
        col("CAL_NUM"), 
        col("CTRY_SHRT_NM"), 
        col("ADDR_LINE_1_TXT"), 
        col("PSTL_CD_NUM"), 
        col("CITY_NM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_"), 
        col("`CRNCY_KEY   `").alias("CRNCY_KEY   ")
    )
