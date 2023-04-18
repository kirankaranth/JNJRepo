from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.config.ConfigStore import *
from md_matl_alt_uom_bw2_deu_djd_gmd_jem_jes_jet_jsw_mtr_sdj.udfs.UDFs import *

def FINAL_SELECT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("ALT_UOM_CD"), 
        col("BASE_UOM_CD"), 
        col("STD_UOM_CD"), 
        col("FACT_NUMRTR_MEAS"), 
        col("_deleted_"), 
        col("_l0_upt_"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_")
    )
