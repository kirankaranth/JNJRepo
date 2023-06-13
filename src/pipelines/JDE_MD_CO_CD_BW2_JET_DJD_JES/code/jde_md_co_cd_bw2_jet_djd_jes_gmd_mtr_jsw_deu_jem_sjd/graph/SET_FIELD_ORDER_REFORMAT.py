from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.config.ConfigStore import *
from jde_md_co_cd_bw2_jet_djd_jes_gmd_mtr_jsw_deu_jem_sjd.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("CO_CD"), 
        col("CUST_NUM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_deleted_")
    )
