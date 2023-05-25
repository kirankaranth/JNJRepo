from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
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
        .withColumn("LOT_SIZE_MAX_QTY", col("BSTMA").cast(DecimalType(22, 4)))\
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
        .withColumn("BASE_QTY", col("BASMG").cast(DecimalType(18, 4)))\
        .withColumn("MAX_STRG_PER", col("MAXLZ").cast(DecimalType(18, 4)))\
        .withColumn("UNIT_FOR_MAX_STRG", trim(col("LZEIH")))\
        .withColumn("OVR_DELV_TLRNC", col("UEETO").cast(DecimalType(18, 4)))\
        .withColumn("UNLTD_OVER_DELV_IN", trim(col("UEETK")))\
        .withColumn("UND_DELV_TLRNC", col("UNETO").cast(DecimalType(18, 4)))\
        .withColumn("TOT_REPLN_LT", col("WZEIT").cast(DecimalType(18, 4)))\
        .withColumn("INSP_STK", trim(col("INSMK")))\
        .withColumn("BTCH_MGMT_PLNT_IN", trim(col("XCHPF")))\
        .withColumn("FISC_YR_VRNT", trim(col("PERIV")))\
        .withColumn("CMMDTY_CD", trim(col("STAWN")))\
        .withColumn("RGN_OF_ORIG_OF_MATL", trim(col("HERKR")))\
        .withColumn("CMMDTY_CD_UOM", trim(col("EXPME")))\
        .withColumn("REP_MFG_IN", trim(col("SAUFT")))\
        .withColumn("VERS_IN", trim(col("VERKZ")))\
        .withColumn("ALT_BOM", trim(col("STLAL")))\
        .withColumn("BOM_USG", trim(col("STLAN")))\
        .withColumn("KEY_TASK_LIST", trim(col("PLNNR")))\
        .withColumn("GRP_CNTR", trim(col("APLAL")))\
        .withColumn("ISS_STRG_LOC", trim(col("LGPRO")))\
        .withColumn("MRP_GRP", trim(col("DISGR")))\
        .withColumn("CMPNT_SCRAP_PCT", col("KAUSF").cast(DecimalType(18, 4)))\
        .withColumn("CERT_TYPE", trim(col("QZGTP")))\
        .withColumn("INSP_SETUP_FOR_MATL", trim(col("QMATV")))\
        .withColumn("PHY_INV_CC", trim(col("ABCIN")))\
        .withColumn("CONFIG_MATL", trim(col("STDPD")))\
        .withColumn("REP_MFG_PRFL", trim(col("SFEPR")))\
        .withColumn("NEG_STK_PLNT", trim(col("XMCNG")))\
        .withColumn("PLNG_CYC", trim(col("LFRHY")))\
        .withColumn("RD_PRFL", trim(col("RDPRF")))\
        .withColumn("CO_PROD_IN", trim(col("KZKUP")))\
        .withColumn("BULK_MATL_IN", trim(col("SCHGT")))\
        .withColumn("FIX_CC_IN", trim(col("CCFIX")))\
        .withColumn("PROD_SCHDLNG_PRFL", trim(col("SFCPF")))\
        .withColumn("CUR_PER", col("LFMON").cast(IntegerType()))\
        .withColumn("INSP_INTV", col("PRFRQ").cast(DecimalType(18, 4)))\
        .withColumn("DOC_REQ_IN", trim(col("KZDKZ")))\
        .withColumn("MATL_FRGHT_GRP", trim(col("MFRGR")))\
        .withColumn("MATL_AUTH_QM", trim(col("QMATA")))\
        .withColumn("CNTL_CODE", trim(col("STEUC")))\
        .withColumn("UNIT_OF_ISS", trim(col("AUSME")))\
        .withColumn("ALT_BOM_METH", trim(col("ALTSL")))\
        .withColumn("SCHDLNG_MRGN_KEY", trim(col("FHORI")))\
        .withColumn("EXPT_IMPT_GRP", trim(col("MTVER")))\
        .withColumn("BACKFLUSH_IN", trim(col("RGEKZ")))\
        .withColumn("MM_DFLT_SUPP_AREA", trim(col("VSPVB")))\
        .withColumn("BTCH_ENT_PO", trim(col("KZECH")))\
        .withColumn("ORIG_BATCH_MGMT", trim(col("UCHKZ")))\
        .withColumn("MATL_CFOP_CAT", trim(col("INDUS")))\
        .withColumn("STRG_LOC_EXTRNL_PRCMT", trim(col("LGFSB")))\
        .withColumn("STK_DTRMN_GRP", trim(col("EPRIO")))\
        .withColumn("DO_NOT_COST_IN", trim(col("NCOST")))\
        .withColumn("SPL_PRCMT_TYPE_COST", trim(col("SOBSK")))\
        .withColumn("CVGE_PRFL_RNG", trim(col("RWPRO")))\
        .withColumn("EXPT_CERT_NUM", trim(col("PRENO")))\
        .withColumn(
          "EXPT_CERT_DTTM",
          when((col("PREND") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("PREND"), "yyyyMMdd"))
        )\
        .withColumn("ORIG_BATCH_REF_MATL", trim(col("UCMAT")))\
        .withColumn("RESET_FCST_MDL", trim(col("AUTRU")))\
        .withColumn("SRVC_LVL", col("LGRAD").cast(DecimalType(18, 4)))\
        .withColumn("MATL_STS_DESC", trim(lookup("LU_SAP_T141T", col("MMSTA")).getField("MTSTB")))\
        .withColumn("MATL_PLNR_NM", trim(lookup("LU_SAP_T024D", col("WERKS"), col("DISPO")).getField("DSNAM")))\
        .withColumn("PRCHSNG_GRP_DESC", trim(lookup("LU_SAP_T024", col("EKGRP")).getField("EKNAM")))\
        .withColumn("MAINT_STS", trim(col("PSTAT")))\
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
        .withColumn("_deleted_", col("_deleted_"))\
        .withColumn("PLNG_PLNT_CD", trim(lookup("LU_SAP_T460A", col("WERKS"), col("SOBSL")).getField("WRK02")))\
        .withColumn("MTS_MTO_FL", expr(Config.MTS_MTO_FL))
