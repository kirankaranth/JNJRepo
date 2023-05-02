from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("BOM_CAT_CD"), 
        col("BOM_NUM"), 
        col("ALT_BOM_NUM"), 
        col("BOM_CNTR_NBR"), 
        col("BOM_VLD_FROM_DTTM"), 
        col("CHG_NUM"), 
        col("PREV_HDR_CNTR_NBR"), 
        col("CRT_DTTM"), 
        col("CHG_DTTM"), 
        col("BOM_UOM_CD"), 
        col("BOM_TXT"), 
        col("BOM_STS_CD"), 
        col("BOM_VLD_TO_DTTM"), 
        col("DEL_IND"), 
        col("BOM_BASE_QTY"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
