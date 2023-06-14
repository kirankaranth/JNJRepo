from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_gmd_deu_jem_jes_djd.config.ConfigStore import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("PLNT_CD"), 
        col("PRCTR_CD"), 
        col("SRC_LIST_IND"), 
        col("LD_GRP_CD"), 
        col("SPEC_MATL_PLNT_STS_CD"), 
        col("LOT_SIZE_VAL"), 
        col("MATL_PLNR_NUM"), 
        col("PRDTN_SUPR_CD"), 
        col("MATL_ABC_CLSN_CD"), 
        col("AVLBLTY_CHK_IND"), 
        col("SPL_PRCMT_TYPE_CD"), 
        col("PRCMT_TYPE_CD"), 
        col("MRP_TYPE_CD"), 
        col("ORIG_CTRY_CD"), 
        col("PLNG_STRTGY_GRP_CD"), 
        col("RD_VAL_QTY"), 
        col("LOT_SIZE_FX_QTY"), 
        col("LOT_SIZE_MAX_QTY"), 
        col("LOT_SIZE_MIN_QTY"), 
        col("SFTY_STK_QTY"), 
        col("PLNG_TIME_FENCE_DAYS_CNT"), 
        col("PLAN_DELV_DAYS_CNT"), 
        col("INHS_PRDTN_DAYS_CNT"), 
        col("PRCHSNG_GRP_CD"), 
        col("VMI_IND"), 
        col("MSTR_PLNG_FMLY_CD"), 
        col("ENTR_PRCMT_TYPE_CD"), 
        col("VALUT_CAT"), 
        col("MRP_PRFL"), 
        col("FLLP_MATL"), 
        col("TOT_REPLN_LT"), 
        col("CMMDTY_CD"), 
        col("MRP_GRP"), 
        col("CNTL_CODE"), 
        col("MM_DFLT_SUPP_AREA"), 
        col("MTS_MTO_FL"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_"), 
        col("BUY_NUM"), 
        col("LINE_TYPE"), 
        col("SHIPPING_CMMDTY_CLS")
    )
