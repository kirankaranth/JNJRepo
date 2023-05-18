from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_dstn_chn_bwi.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_bwi.udfs.UDFs import *

def ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("SL_ORG_NUM"), 
        col("DSTR_CHNL_CD"), 
        col("PROD_HIER_CD"), 
        col("DELV_PLNT_CD"), 
        col("MATL_SLS_CAT_GRP_CD"), 
        col("ACTG_GRP_CD"), 
        col("DSTN_CHN_STS_CD"), 
        col("VLD_FROM_DTTM"), 
        col("ENTRP_DSTN_CHN_STS_CD"), 
        col("MATL_BASE_CD"), 
        col("VOL_REBT_GRP"), 
        col("MATL_PRC_GRP"), 
        col("MATL_GRP_1"), 
        col("MATL_GRP_2"), 
        col("MATL_GRP_3"), 
        col("MATL_GRP_4"), 
        col("MATL_GRP_5"), 
        col("PRC_REF_MATL"), 
        col("MDD_SALEABLE"), 
        col("PHARMA_SALEABLE"), 
        col("CNSMR_SALEABLE"), 
        col("COMMSN_GRP"), 
        col("RD_PRFL"), 
        col("CASH_DISC_IN"), 
        col("MATL_STATS_GRP"), 
        col("MATL_DSTN_CHN"), 
        col("DEL_IN"), 
        col("MIN_ORDR_QTY"), 
        col("MIN_DELV_QTY"), 
        col("MIN_MTO_QTY"), 
        col("DELV_UNIT"), 
        col("UOM_DELV_UNIT"), 
        col("SLS_UNIT"), 
        col("ASRTMNT_GRADE"), 
        col("EXTRNL_ASRTMNT_PRIR"), 
        col("LIST_PCDR_STR_ASRTMNT_CAT"), 
        col("LIST_PCDR_DC_ASRTMNT_CAT"), 
        col("LIST_FUNC_ASRTMNT_ACT"), 
        col("STR_LIST_FROM_DTTM"), 
        col("STR_LIST_TO_DTTM"), 
        col("DC_LIST_FROM_DTTM"), 
        col("DC_LIST_TO_DTTM"), 
        col("STR_SOLD_FRM_DTTM"), 
        col("STR_SOLD_TO_DTTM"), 
        col("DC_SOLD_FRM_DTTM"), 
        col("DC_SOLD_TO_DTTM"), 
        col("PROD_ATTR_ID_4"), 
        col("PROD_ATTR_ID_5"), 
        col("PROD_ATTR_ID_6"), 
        col("PROD_ATTR_ID_7"), 
        col("PROD_ATTR_ID_8"), 
        col("PROD_ATTR_ID_9"), 
        col("PROD_ATTR_ID_10"), 
        col("UOM_GRP"), 
        col("MAX_DELV_QTY"), 
        col("RACKJOBBER_MATL"), 
        col("PRC_FIX_IN"), 
        col("VAR_SLS_UNIT_IN"), 
        col("MATL_CMPT_CHAR"), 
        col("MATL_SORT"), 
        col("PRC_BND_CAT"), 
        col("MATL_SLS_CAT_GRP_DESC"), 
        col("PROD_HIER_LVL_NUM"), 
        col("PROD_HIER_DESC"), 
        col("DSTN_CHN_STS_CD_DESC"), 
        col("BLOK_FOR_SLS_ORDR"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_pk_md5_"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_deleted_")
    )
