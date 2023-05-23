from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hcs.config.ConfigStore import *
from sap_md_matl_hcs.udfs.UDFs import *

def SELECT_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("MATL_TYPE_CD"), 
        col("BRND_CD"), 
        col("FRANCHISE_CD"), 
        col("LCL_PLNG_SUB_FRAN_CD"), 
        col("PRCHSNG_VAL_KEY_CD"), 
        col("DEL_IND"), 
        col("MATL_GRP_CD"), 
        col("INDSTR_SECTR_CD"), 
        col("BASE_UOM_CD"), 
        col("TOT_SHLF_LIF_DAYS_CNT"), 
        col("MIN_SHLF_RMN_LIF_DAYS_CNT"), 
        col("MATL_STS_CD"), 
        col("DSTN_CHN_STS_CD"), 
        col("NET_WT_MEAS"), 
        col("PROD_HIER_CD"), 
        col("PRCMT_QUAL_MGMT_IND"), 
        col("STRG_CONDS_CD"), 
        col("LBL_TEMP_RNG"), 
        col("TRSPN_GRP_CD"), 
        col("BTCH_MNG_IND"), 
        col("MATL_DOC_NUM"), 
        col("MATL_DOC_VERS_NUM"), 
        col("MATL_SHRT_DESC"), 
        col("MATL_CAT_GRP_CD"), 
        col("CHG_BY"), 
        col("DOC_CHG_NUM"), 
        col("CNTNR_REQ"), 
        col("OLD_MATL_NUM"), 
        col("GRS_WT"), 
        col("ORDR_UNIT_PUR_UOM"), 
        col("CRT_BY"), 
        col("CRT_ON_DTTM"), 
        col("LBL_TYPE"), 
        col("LBL_FORM"), 
        col("EXTRNL_MATL_GRP"), 
        col("MAX_LVL"), 
        col("WT_UNIT"), 
        col("SIZE_DIM"), 
        col("PER_IN"), 
        col("LAST_CHG_DT_TIME_DTTM"), 
        col("MATL_GRP_PKGNG_MATL"), 
        col("STRG_PCT"), 
        col("VAL_FROM_XPLNT_DTTM"), 
        col("VAL_FROM_XDC_DTTM"), 
        col("INDSTR_STD_DESC"), 
        col("RD_RUL_SLED"), 
        col("SER_LVL"), 
        col("MATL_HAZ_CD"), 
        col("VAR_ORDR_UNT"), 
        col("PKGNG_MATL_TYPE"), 
        col("VOL_UNIT"), 
        col("VOL"), 
        col("BSC_MATL"), 
        col("DOC_TYPE"), 
        col("DOC_PG_FMT"), 
        col("EAN_UPC"), 
        col("EAN_CAT"), 
        col("GTIN_VRNT"), 
        col("EAN_UPC_HRMZD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_upt_").alias("_l0_upt_"), 
        col("_pk_L1").alias("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_L1").alias("_deleted_"), 
        col("MMS_SURGERY_TYPE_CD"), 
        col("MMS_MATL_TYPE_CD"), 
        col("MMS_FIN_CLSN_CD"), 
        col("MMS_TEMP_SENS_IND"), 
        col("MMS_STERILIZATION_IND"), 
        col("MATL_TYPE_DESC"), 
        col("MATL_GRP_DESC"), 
        col("MATL_GRP_DESC_2"), 
        col("SHRT_MATL_NUM")
    )
