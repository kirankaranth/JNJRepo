from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def FIELD_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("MATL_TYPE_CD"), 
        col("BRND_CD"), 
        col("FRANCHISE_CD"), 
        col("LCL_PLNG_SUB_FRAN_CD"), 
        col("MATL_GRP_CD"), 
        col("BASE_UOM_CD"), 
        col("TOT_SHLF_LIF_DAYS_CNT"), 
        col("MIN_SHLF_RMN_LIF_DAYS_CNT"), 
        col("MATL_STS_CD"), 
        col("DSTN_CHN_STS_CD"), 
        col("PROD_HIER_CD"), 
        col("STRG_CONDS_CD"), 
        col("BTCH_MNG_IND"), 
        col("MATL_DOC_NUM"), 
        col("MATL_DOC_VERS_NUM"), 
        col("MATL_SHRT_DESC"), 
        col("MATL_CATLG_NUM"), 
        col("SRC_SECTR_CD"), 
        col("MATL_PARNT_CD"), 
        col("MATL_SUB_TYPE_CD"), 
        col("FIN_HIER_BASE_CD"), 
        col("IMPLNT_INSTM_IND"), 
        col("KIT_IND"), 
        col("DIR_PART_MRKNG_CD"), 
        col("MATL_CAT_GRP_CD"), 
        col("PLNG_HIER3_CD"), 
        col("MATL_SPEC_NUM"), 
        col("MATL_SPEC_VERS_NUM"), 
        col("CHG_BY"), 
        col("WT_UNIT"), 
        col("LAST_CHG_DT_TIME_DTTM"), 
        col("VOL_UNIT"), 
        col("VOL"), 
        col("DOC_TYPE"), 
        col("EAN_UPC"), 
        col("MAIN_STRG_LOC"), 
        col("PROD_LINE"), 
        col("MAKE_BUY_IN"), 
        col("MATL_TYPE_DESC"), 
        col("FRAN_CD_DESC"), 
        col("TYPE_OF_MATERIAL"), 
        col("STERILE"), 
        col("BRAVO_MINOR_CODE"), 
        col("BRAVO_MINOR_CODE_DESC"), 
        col("MATL_GRP_DESC"), 
        col("SHRT_MATL_NUM"), 
        col("CMMDTY"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("MATL_GRP_DESC_2")
    )
