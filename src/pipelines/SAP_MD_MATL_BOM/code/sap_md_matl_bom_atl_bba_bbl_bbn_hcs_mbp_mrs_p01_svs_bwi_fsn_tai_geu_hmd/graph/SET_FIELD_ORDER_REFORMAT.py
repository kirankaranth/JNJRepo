from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.config.ConfigStore import *
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.udfs.UDFs import *

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
        col("FROM_LOT_SIZE_QTY"), 
        col("TO_LOT_SIZE_QTY"), 
        col("USER_WHO_CRT_REC"), 
        col("NM_OF_PRSN_WHO_CHG_OBJ"), 
        col("CNFG_MATL_IN"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_pk_"), 
        col("_deleted_"), 
        col("_l0_upt_"), 
        col("_pk_md5_"), 
        col("_l1_upt_")
    )
