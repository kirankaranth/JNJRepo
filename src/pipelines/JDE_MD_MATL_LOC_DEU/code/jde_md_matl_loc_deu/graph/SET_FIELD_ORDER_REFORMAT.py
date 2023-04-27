from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_deu.config.ConfigStore import *
from jde_md_matl_loc_deu.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(Config.sourceSystem).alias("SRC_SYS_CD"), 
        col("iblitm").alias("MATL_NUM"), 
        col("ibmcu").alias("PLNT_CD"), 
        trim(col("ibsrp2")).alias("PRCTR_CD"), 
        trim(col("ibsrp4")).alias("SRC_LIST_IND"), 
        trim(col("ibsrp1")).alias("LD_GRP_CD"), 
        trim(col("ibstkt")).alias("SPEC_MATL_PLNT_STS_CD"), 
        trim(col("ibopc")).alias("LOT_SIZE_VAL"), 
        trim(col("ibanpl")).alias("MATL_PLNR_NUM"), 
        trim(col("ibsrp5")).alias("PRDTN_SUPR_CD"), 
        trim(col("ibabcs")).alias("MATL_ABC_CLSN_CD"), 
        trim(col("ibckav")).alias("AVLBLTY_CHK_IND"), 
        trim(col("ibsrp0")).alias("PRCMT_TYPE_CD"), 
        trim(col("ibprp1")).alias("MRP_TYPE_CD"), 
        trim(col("iborig")).alias("ORIG_CTRY_CD"), 
        trim(col("ibprp2")).alias("PLNG_STRTGY_GRP_CD"), 
        trim(col("ibmult")).cast(DecimalType(18, 4)).alias("RD_VAL_QTY"), 
        trim(col("ibopv")).cast(DecimalType(18, 4)).alias("LOT_SIZE_FX_QTY"), 
        trim(col("ibrqmx")).cast(DecimalType(22, 4)).alias("LOT_SIZE_MAX_QTY"), 
        trim(col("ibrqmn")).cast(DecimalType(18, 4)).alias("LOT_SIZE_MIN_QTY"), 
        trim(col("ibsafe")).cast(DecimalType(18, 4)).alias("SFTY_STK_QTY"), 
        trim(col("ibltlv")).alias("PLNG_TIME_FENCE_DAYS_CNT"), 
        trim(col("ibltlv")).cast(DecimalType(18, 4)).alias("INHS_PRDTN_DAYS_CNT"), 
        trim(col("ibvend")).alias("PRCHSNG_GRP_CD"), 
        trim(col("ibsrp7")).alias("MSTR_PLNG_FMLY_CD"), 
        trim(col("ibsrp3")).alias("ENTR_PRCMT_TYPE_CD"), 
        trim(col("ibglpt")).alias("VALUT_CAT"), 
        trim(col("ibsrp5")).alias("MRP_PRFL"), 
        trim(col("ibltcm")).cast(DecimalType(18, 4)).alias("TOT_REPLN_LT"), 
        trim(col("ibshcm")).alias("CMMDTY_CD"), 
        trim(col("ibsrp6")).alias("MRP_GRP"), 
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
