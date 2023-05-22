from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_gmd_deu_jem_jes_djd.config.ConfigStore import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("iblitm"))\
        .withColumn("PLNT_CD", col("ibmcu"))\
        .withColumn("PRCTR_CD", expr(Config.PRCTR_CD))\
        .withColumn("SRC_LIST_IND", expr(Config.SRC_LIST_IND))\
        .withColumn("LD_GRP_CD", expr(Config.LD_GRP_CD))\
        .withColumn("SPEC_MATL_PLNT_STS_CD", trim(col("ibstkt")))\
        .withColumn("LOT_SIZE_VAL", trim(col("ibopc")))\
        .withColumn("MATL_PLNR_NUM", trim(col("ibanpl")))\
        .withColumn("PRDTN_SUPR_CD", expr(Config.PRDTN_SUPR_CD))\
        .withColumn("MATL_ABC_CLSN_CD", trim(col("ibabcs")))\
        .withColumn("AVLBLTY_CHK_IND", trim(col("ibckav")))\
        .withColumn("SPL_PRCMT_TYPE_CD", expr(Config.SPL_PRCMT_TYPE_CD))\
        .withColumn("PRCMT_TYPE_CD", expr(Config.PRCMT_TYPE_CD))\
        .withColumn("MRP_TYPE_CD", expr(Config.MRP_TYPE_CD))\
        .withColumn("ORIG_CTRY_CD", trim(col("iborig")))\
        .withColumn("PLNG_STRTGY_GRP_CD", expr(Config.PLNG_STRTGY_GRP_CD))\
        .withColumn("RD_VAL_QTY", col("ibmult").cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_FX_QTY", col("ibopv").cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_MAX_QTY", col("ibrqmx").cast(DecimalType(22, 4)))\
        .withColumn("LOT_SIZE_MIN_QTY", col("ibrqmn").cast(DecimalType(18, 4)))\
        .withColumn("SFTY_STK_QTY", col("ibsafe").cast(DecimalType(18, 4)))\
        .withColumn("PLNG_TIME_FENCE_DAYS_CNT", trim(col("ibltlv")))\
        .withColumn("PLAN_DELV_DAYS_CNT", expr(Config.PLAN_DELV_DAYS_CNT))\
        .withColumn("INHS_PRDTN_DAYS_CNT", expr(Config.INHS_PRDTN_DAYS_CNT))\
        .withColumn("PRCHSNG_GRP_CD", expr(Config.PRCHSNG_GRP_CD))\
        .withColumn("VMI_IND", expr(Config.VMI_IND))\
        .withColumn("MSTR_PLNG_FMLY_CD", expr(Config.MSTR_PLNG_FMLY_CD))\
        .withColumn("ENTR_PRCMT_TYPE_CD", expr(Config.ENTR_PRCMT_TYPE_CD))\
        .withColumn("VALUT_CAT", expr(Config.VALUT_CAT))\
        .withColumn("MRP_PRFL", expr(Config.MRP_PRFL))\
        .withColumn("FLLP_MATL", expr(Config.FLLP_MATL))\
        .withColumn("TOT_REPLN_LT", expr(Config.TOT_REPLN_LT))\
        .withColumn("CMMDTY_CD", expr(Config.CMMDTY_CD))\
        .withColumn("MRP_GRP", expr(Config.MRP_GRP))\
        .withColumn("CNTL_CODE", expr(Config.CNTL_CODE))\
        .withColumn("MM_DFLT_SUPP_AREA", expr(Config.MM_DFLT_SUPP_AREA))\
        .withColumn("MTS_MTO_FL", expr(Config.MTS_MTO_FL))\
        .withColumn("BUY_NUM", trim(col("IBBUYR")))\
        .withColumn("LINE_TYPE", trim(col("IBLNTY")))\
        .withColumn("SHIPPING_CMMDTY_CLS", trim(col("IBSHCM")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD)")))\
        .withColumn(
          "_pk_md5_",
          md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD)")))
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
