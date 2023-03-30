from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(Config.sourceSystem).alias("SRC_SYS_CD"), 
        col("matnr").alias("MATL_NUM"), 
        col("werks").alias("PLNT_CD"), 
        trim(col("awsls")).alias("VAR_CNTL_CD"), 
        trim(col("kautb")).alias("AUTO_PO_IND"), 
        trim(col("prctr")).alias("PRCTR_CD"), 
        trim(col("kordb")).alias("SRC_LIST_IND"), 
        trim(col("ladgr")).alias("LD_GRP_CD"), 
        trim(col("ssqss")).alias("QUAL_MGMT_CNTL_CD"), 
        col("minbe").cast(DecimalType(18, 4)).alias("REORDR_QTY"), 
        col("losgr").cast(DecimalType(18, 4)).alias("COST_LOT_SIZE_VAL"), 
        trim(col("sernp")).alias("SERIZATION_PRFL_CD"), 
        trim(col("mmsta")).alias("SPEC_MATL_PLNT_STS_CD"), 
        when((col("mmstd") == lit("00000000")), lit(None))\
          .otherwise(to_timestamp(col("MMSTD"), "yyyyMMdd"))\
          .alias("SPEC_MATL_PLNT_STS_DTTM"), 
        trim(col("lvorm")).alias("DEL_IND"), 
        trim(col("disls")).alias("LOT_SIZE_VAL"), 
        trim(col("dispo")).alias("MATL_PLNR_NUM"), 
        trim(col("fevor")).alias("PRDTN_SUPR_CD"), 
        trim(col("frtme")).alias("PRDTN_UOM_CD"), 
        trim(col("maabc")).alias("MATL_ABC_CLSN_CD"), 
        trim(col("mtvfp")).alias("AVLBLTY_CHK_IND"), 
        trim(col("shflg")).alias("SFTY_TIME_IND"), 
        trim(col("sobsl")).alias("SPL_PRCMT_TYPE_CD"), 
        trim(col("beskz")).alias("PRCMT_TYPE_CD"), 
        trim(col("dismm")).alias("MRP_TYPE_CD"), 
        trim(col("herkl")).alias("ORIG_CTRY_CD"), 
        trim(col("strgr")).alias("PLNG_STRTGY_GRP_CD"), 
        col("bstrf").cast(DecimalType(18, 4)).alias("RD_VAL_QTY"), 
        col("bstfe").cast(DecimalType(18, 4)).alias("LOT_SIZE_FX_QTY"), 
        col("webaz").cast(DecimalType(18, 4)).alias("GOOD_RCPT_LEAD_DAYS_QTY"), 
        col("bstma").cast(DecimalType(22, 4)).alias("LOT_SIZE_MAX_QTY"), 
        col("bstmi").cast(DecimalType(18, 4)).alias("LOT_SIZE_MIN_QTY"), 
        col("eisbe").cast(DecimalType(18, 4)).alias("SFTY_STK_QTY"), 
        trim(col("fxhor")).alias("PLNG_TIME_FENCE_DAYS_CNT"), 
        col("mabst").cast(DecimalType(18, 4)).alias("MAX_STK_LVL_QTY"), 
        trim(col("shzet")).alias("SFTY_TIME_DAYS_CNT"), 
        col("plifz").cast(DecimalType(18, 4)).alias("PLAN_DELV_DAYS_CNT"), 
        col("ausss").cast(DecimalType(18, 4)).alias("SCRAP_PCT"), 
        col("dzeit").cast(DecimalType(18, 4)).alias("INHS_PRDTN_DAYS_CNT"), 
        col("eislo").cast(DecimalType(18, 4)).alias("MIN_SFTY_STK_QTY"), 
        col("vint1").cast(DecimalType(18, 4)).alias("BKWRD_CNSMPTN_DAYS_CNT"), 
        col("vint2").cast(DecimalType(18, 4)).alias("FRWD_CNSMPTN_DAYS_CNT"), 
        trim(col("vrmod")).alias("CNSMPTN_MODE_CD"), 
        trim(col("ekgrp")).alias("PRCHSNG_GRP_CD"), 
        trim(col("zzmmsficlass")).alias("MMS_FIN_CLSN_CD"), 
        trim(col("zzsmiind")).alias("VMI_IND"), 
        trim(col("zzmpfamily")).alias("MSTR_PLNG_FMLY_CD"), 
        trim(col("zzmtomtsind")).alias("MTS_MTO_FL"), 
        lit(Config.DAI_ETL_ID).alias("DAI_ETL_ID"), 
        current_timestamp().alias("DAI_CRT_DTTM"), 
        current_timestamp().alias("DAI_UPDT_DTTM"), 
        col("_upt_").alias("_l0_upt_"), 
        to_json(expr("named_struct('SCR_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD)")).alias("_pk_"), 
        md5(to_json(expr("named_struct('SCR_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD)")))\
          .alias("_pk_md5_"), 
        current_timestamp().alias("_l1_upt_"), 
        lit("F").alias("_deleted_")
    )
