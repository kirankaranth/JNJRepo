from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_md_delv_line_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT_F43121(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("DELV_NUM"), 
        col("DELV_LINE_NBR"), 
        col("DELV_TYP_CD"), 
        col("CO_CD"), 
        col("DOC_REF_NUM"), 
        col("ACTL_GI_DTTM"), 
        col("ACTL_SLS_UNIT_DELV_QTY"), 
        col("BTCH_NUM"), 
        col("MATL_NUM"), 
        col("SLS_ORDR_LINE_NBR_REF"), 
        col("SLS_UOM_CD"), 
        col("SHIPPING_PLNT_CD"), 
        col("SLOC_CD"), 
        col("CRT_DTTM"), 
        col("ORDR_SFX"), 
        col("MATCH_TYPE"), 
        col("LINE_TYPE_DELV"), 
        col("SRC_TBL_NM"), 
        col("ORIG_QTY_DELV_ITM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
