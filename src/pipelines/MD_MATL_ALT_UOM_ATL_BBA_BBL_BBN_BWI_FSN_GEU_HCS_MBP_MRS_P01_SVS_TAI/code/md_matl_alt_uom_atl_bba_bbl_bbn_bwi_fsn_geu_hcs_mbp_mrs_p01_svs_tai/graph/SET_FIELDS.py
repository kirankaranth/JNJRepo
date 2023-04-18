from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.config.ConfigStore import *
from md_matl_alt_uom_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai.udfs.UDFs import *

def SET_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("ALT_UOM_CD"), 
        col("BASE_UOM_CD"), 
        col("LCL_FACT_NUMRTR_MEAS"), 
        col("LCL_FACT_DENOM_MEAS"), 
        col("FACT_NUMRTR_MEAS"), 
        col("FACT_DENOM_MEAS"), 
        col("GTIN_NUM"), 
        col("GTIN_CAT_CD"), 
        col("WDTH_MEAS"), 
        col("DIM_UOM_CD"), 
        col("GRS_WT_MEAS"), 
        col("WT_UOM_CD"), 
        col("LGTH_MEAS"), 
        col("HGHT_MEAS"), 
        col("VOL_MEAS"), 
        col("GTIN_NUM_HRMZD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("DAI_UPDT_DTTM"), 
        col("_deleted_")
    )
