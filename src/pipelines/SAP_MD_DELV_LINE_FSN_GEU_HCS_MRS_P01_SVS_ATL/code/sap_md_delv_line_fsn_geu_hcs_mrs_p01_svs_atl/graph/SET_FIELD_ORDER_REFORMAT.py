from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.config.ConfigStore import *
from sap_md_delv_line_fsn_geu_hcs_mrs_p01_svs_atl.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("DELV_NUM"), 
        col("DELV_LINE_NBR"), 
        col("DELV_TYP_CD"), 
        col("CO_CD"), 
        col("MATL_SHRT_DESC"), 
        col("MATL_MVMT_TYPE_CD"), 
        col("DOC_REF_NUM"), 
        col("PICK_CNTL_STS_CD"), 
        col("LINE_ITEM_CAT_CD"), 
        col("SPL_STK_TYPE_CD"), 
        col("PARNT_BTCH_SPLT_CNTR_NBR"), 
        col("PARNT_BOM_CNTR_NBR"), 
        col("FCTR_NMRTR_MEAS"), 
        col("FCTR_DNMNTR_MEAS"), 
        col("ACTL_GI_DTTM"), 
        col("ACTL_SLS_UNIT_DELV_QTY"), 
        col("ACTL_SKU_DELV_QTY"), 
        col("BASE_UOM_CD"), 
        col("BTCH_NUM"), 
        col("BILL_ICMPT_STS_CD"), 
        col("CNFRM_STS_CD"), 
        col("MFG_DTTM"), 
        col("DELV_BILL_STS_CD"), 
        col("DELV_CMPLT_IND"), 
        col("DELV_ICMPT_STS_CD"), 
        col("DELV_STS_CD"), 
        col("DELV_TOT_STS_CD"), 
        col("EXP_DTTM"), 
        col("GM_ICMPT_STS_CD"), 
        col("GM_STS_CD"), 
        col("INTCO_BILL_STS_CD"), 
        col("MATL_NUM"), 
        col("ICMPT_STS_CD"), 
        col("ORDR_BILL_STS_CD"), 
        col("PACK_ICMPT_STS_CD"), 
        col("PACK_STS_CD"), 
        col("PICK_CNFRM_STS_CD"), 
        col("PICK_ICMPT_STS_CD"), 
        col("PICK_STS_CD"), 
        col("PRC_ICMPT_STS_CD"), 
        col("PRCSG_TOT_STS_CD"), 
        col("RECV_PLNT_CD"), 
        col("REF_STS_CD"), 
        col("REF_TOT_STS_CD"), 
        col("REJ_STS_CD"), 
        col("SLS_ORDR_LINE_NBR_REF"), 
        col("SLS_UOM_CD"), 
        col("SHIPPING_PLNT_CD"), 
        col("SLOC_CD"), 
        col("VEND_BTCH_NUM"), 
        col("WM_STS_CD"), 
        col("CRT_DTTM"), 
        col("ITM_TYPE"), 
        col("ITM_OF_REF_DOC"), 
        col("GRS_WT_MEAS"), 
        col("NET_WT_MEAS"), 
        col("WT_UOM_CD"), 
        col("VOL_MEAS"), 
        col("VOL_UOM_CD"), 
        col("NET_VAL_AMT"), 
        col("PROD_HIER_CD"), 
        col("MATL_GRP_4"), 
        col("DSTR_CHNL_CD"), 
        col("ITM_BILL_BLK_STS_CD"), 
        col("ITM_OVRL_DELV_BLK_STS_CD"), 
        col("ENT_MATL_NUM"), 
        col("DIVISION_CD"), 
        col("DOC_CAT_SD"), 
        col("MATL_GRP_CD"), 
        col("MATL_TYPE_CD"), 
        col("CUM_BTCH_QTY"), 
        col("ORDR_SFX"), 
        col("MATCH_TYPE"), 
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
