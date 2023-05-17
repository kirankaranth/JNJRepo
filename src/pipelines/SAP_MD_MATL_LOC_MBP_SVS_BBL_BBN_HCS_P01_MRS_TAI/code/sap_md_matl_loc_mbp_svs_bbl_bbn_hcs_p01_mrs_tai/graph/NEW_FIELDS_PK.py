from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbl_bbn_hcs_p01_mrs_tai.udfs.UDFs import *

def NEW_FIELDS_PK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn("VAR_CNTL_CD", trim(col("AWSLS")))\
        .withColumn("AUTO_PO_IND", trim(col("KAUTB")))\
        .withColumn("PRCTR_CD", trim(col("PRCTR")))\
        .withColumn("SRC_LIST_IND", trim(col("KORDB")))\
        .withColumn("LD_GRP_CD", trim(col("LADGR")))\
        .withColumn("QUAL_MGMT_CNTL_CD", trim(col("SSQSS")))\
        .withColumn("REORDR_QTY", col("MINBE").cast(DecimalType(18, 4)))\
        .withColumn("COST_LOT_SIZE_VAL", col("LOSGR").cast(DecimalType(18, 4)))\
        .withColumn("SERIZATION_PRFL_CD", trim(col("SERNP")))\
        .withColumn("SPEC_MATL_PLNT_STS_CD", trim(col("MMSTA")))\
        .withColumn(
          "SPEC_MATL_PLNT_STS_DTTM",
          when((col("MMSTD") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("MMSTD"), "yyyyMMdd"))
        )\
        .withColumn("DEL_IND", trim(col("LVORM")))\
        .withColumn("LOT_SIZE_VAL", trim(col("DISLS")))\
        .withColumn("MATL_PLNR_NUM", trim(col("DISPO")))\
        .withColumn("PRDTN_SUPR_CD", trim(col("FEVOR")))\
        .withColumn("PRDTN_UOM_CD", trim(col("FRTME")))\
        .withColumn("MATL_ABC_CLSN_CD", trim(col("MAABC")))\
        .withColumn("AVLBLTY_CHK_IND", trim(col("MTVFP")))\
        .withColumn("SFTY_TIME_IND", trim(col("SHFLG")))\
        .withColumn("SPL_PRCMT_TYPE_CD", trim(col("SOBSL")))\
        .withColumn("PRCMT_TYPE_CD", trim(col("BESKZ")))\
        .withColumn("MRP_TYPE_CD", trim(col("DISMM")))\
        .withColumn("ORIG_CTRY_CD", trim(col("HERKL")))\
        .withColumn("PLNG_STRTGY_GRP_CD", trim(col("STRGR")))\
        .withColumn("RD_VAL_QTY", col("BSTRF").cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_FX_QTY", col("BSTFE").cast(DecimalType(18, 4)))\
        .withColumn("GOOD_RCPT_LEAD_DAYS_QTY", col("WEBAZ").cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_MAX_QTY", col("BSTMA").cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_MIN_QTY", col("BSTMI").cast(DecimalType(18, 4)))\
        .withColumn("SFTY_STK_QTY", col("EISBE").cast(DecimalType(18, 4)))\
        .withColumn("PLNG_TIME_FENCE_DAYS_CNT", trim(col("FXHOR")))\
        .withColumn("MAX_STK_LVL_QTY", col("MABST").cast(DecimalType(18, 4)))\
        .withColumn("SFTY_TIME_DAYS_CNT", trim(col("SHZET")))\
        .withColumn("PLAN_DELV_DAYS_CNT", col("PLIFZ").cast(DecimalType(18, 4)))\
        .withColumn("SCRAP_PCT", col("AUSSS").cast(DecimalType(18, 4)))\
        .withColumn("INHS_PRDTN_DAYS_CNT", col("DZEIT").cast(DecimalType(18, 4)))\
        .withColumn("MIN_SFTY_STK_QTY", col("EISLO").cast(DecimalType(18, 4)))\
        .withColumn("BKWRD_CNSMPTN_DAYS_CNT", col("VINT1").cast(DecimalType(18, 4)))\
        .withColumn("FRWD_CNSMPTN_DAYS_CNT", col("VINT2").cast(DecimalType(18, 4)))\
        .withColumn("CNSMPTN_MODE_CD", trim(col("VRMOD")))\
        .withColumn("PRCHSNG_GRP_CD", trim(col("EKGRP")))\
        .withColumn("MMS_FIN_CLSN_CD", expr(Config.MMS_FIN_CLSN_CD))\
        .withColumn("VMI_IND", expr(Config.VMI_IND))\
        .withColumn("MSTR_PLNG_FMLY_CD", expr(Config.MSTR_PLNG_FMLY_CD))\
        .withColumn("VALUT_CAT", trim(col("BWTTY")))\
        .withColumn("BTCH_MGMT_IN_INTRNL", trim(col("XCHAR")))\
        .withColumn("MRP_PRFL", trim(col("DISPR")))\
        .withColumn("PER_IN", trim(col("PERKZ")))\
        .withColumn("DEPN_REQ_IN", trim(col("SBDKZ")))\
        .withColumn("STRG_COST_PCT_CD", trim(col("LAGPR")))\
        .withColumn("DSCNT_IN", trim(col("KZAUS")))\
        .withColumn(
          "EFF_OUT_DTTM",
          when((col("AUSDT") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("AUSDT"), "yyyyMMdd"))
        )\
        .withColumn("FLLP_MATL", trim(col("NFMAT")))\
        .withColumn("RQR_GRP_IN", trim(col("KZBED")))\
        .withColumn("MIX_MRP_IN", trim(col("MISKZ")))\
        .withColumn("BASE_QTY", trim(col("BASMG")))\
        .withColumn("MAX_STRG_PER", trim(col("MAXLZ")))\
        .withColumn("UNIT_FOR_MAX_STRG", trim(col("LZEIH")))\
        .withColumn("OVR_DELV_TLRNC", trim(col("UEETO")))\
        .withColumn("UNLTD_OVER_DELV_IN", trim(col("UEETK")))\
        .withColumn("UND_DELV_TLRNC", trim(col("UNETO")))\
        .withColumn("TOT_REPLN_LT", trim(col("WZEIT")))
