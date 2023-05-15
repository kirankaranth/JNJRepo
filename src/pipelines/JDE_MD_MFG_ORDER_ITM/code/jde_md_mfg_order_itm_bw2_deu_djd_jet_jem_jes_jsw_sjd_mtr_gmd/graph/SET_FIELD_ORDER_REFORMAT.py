from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd.config.ConfigStore import *
from jde_md_mfg_order_itm_bw2_deu_djd_jet_jem_jes_jsw_sjd_mtr_gmd.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MFG_ORDR_TYP_CD"), 
        col("MFG_ORDR_NUM"), 
        col("LN_ITM_NBR"), 
        col("PRDTN_UOM_CD"), 
        col("BTCH_NUM"), 
        col("DLV_CMPLT_IND"), 
        col("MATL_NUM"), 
        col("SCRP_QTY"), 
        col("ITM_QTY"), 
        col("PLNT_CD"), 
        col("RCVD_QTY"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
