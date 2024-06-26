from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_crncy_svs_bwi_atl.config.ConfigStore import *
from sap_md_crncy_svs_bwi_atl.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("CRNCY_CD"), 
        col("ISO_CRNCY_CD"), 
        col("CRNCY_ALT_CD"), 
        col("VLD_TO_DTTM"), 
        col("PRMRY_SAP_CRNCY_CD_ISO_CD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
