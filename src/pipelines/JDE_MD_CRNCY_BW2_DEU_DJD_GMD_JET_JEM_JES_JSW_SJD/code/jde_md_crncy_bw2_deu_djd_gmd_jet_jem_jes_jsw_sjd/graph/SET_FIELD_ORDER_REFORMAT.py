from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.config.ConfigStore import *
from jde_md_crncy_bw2_deu_djd_gmd_jet_jem_jes_jsw_sjd.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("CRNCY_CD"), 
        col("ISO_CRNCY_CD"), 
        col("DEC_PLACE_CNT"), 
        col("CRNCY_LONG_NM"), 
        col("NUM_OF_DEC_PLACES"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
