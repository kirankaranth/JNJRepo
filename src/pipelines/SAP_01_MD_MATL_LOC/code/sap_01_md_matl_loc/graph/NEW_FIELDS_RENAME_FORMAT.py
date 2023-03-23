from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("NSDM_V_MARC_MATNR"))\
        .withColumn("PLNT_CD", col("NSDM_V_MARC_WERKS"))\
        .withColumn("VAR_CNTL_CD", trim(col("AWSLS")))\
        .withColumn("AUTO_PO_IND", trim(col("KAUTB")))\
        .withColumn("PRCTR_CD", trim(col("PRCTR")))\
        .withColumn("SRC_LIST_IND", trim(col("KORDB")))\
        .withColumn("LD_GRP_CD", trim(col("LADGR")))\
        .withColumn("QUAL_MGMT_CNTL_CD", trim(col("SSQSS")))\
        .withColumn("REORDR_QTY", trim(col("MINBE")).cast(DecimalType(18, 4)))\
        .withColumn("COST_LOT_SIZE_VAL", trim(col("LOSGR")).cast(DecimalType(18, 4)))\
        .withColumn("SERIZATION_PRFL_CD", trim(col("SERNP")))\
        .withColumn("SPEC_MATL_PLNT_STS_CD", trim(col("MMSTA")))\
        .withColumn(
          "SPEC_MATL_PLNT_STS_DTTM",
          when((col("MMSTD") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("MMSTD"), "yyyyMMdd"))\
            .alias("SPEC_MATL_PLNT_STS_DTTM")
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
        .withColumn("RD_VAL_QTY", trim(col("BSTRF")).cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_FX_QTY", trim(col("BSTFE")).cast(DecimalType(18, 4)))\
        .withColumn("GOOD_RCPT_LEAD_DAYS_QTY", trim(col("WEBAZ")).cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_MAX_QTY", trim(col("BSTMA")).cast(DecimalType(22, 4)))\
        .withColumn("LOT_SIZE_MIN_QTY", trim(col("BSTMI")).cast(DecimalType(18, 4)))\
        .withColumn("SFTY_STK_QTY", trim(col("EISBE")).cast(DecimalType(18, 4)))\
        .withColumn("PLNG_TIME_FENCE_DAYS_CNT", trim(col("FXHOR")))\
        .withColumn("MAX_STK_LVL_QTY", trim(col("MABST")).cast(DecimalType(18, 4)))\
        .withColumn("SFTY_TIME_DAYS_CNT", trim(col("SHZET")))\
        .withColumn("PLAN_DELV_DAYS_CNT", trim(col("PLIFZ")).cast(DecimalType(18, 4)))\
        .withColumn("SCRAP_PCT", trim(col("AUSSS")).cast(DecimalType(18, 4)))\
        .withColumn("INHS_PRDTN_DAYS_CNT", trim(col("DZEIT")).cast(DecimalType(18, 4)))\
        .withColumn("MIN_SFTY_STK_QTY", trim(col("EISLO")).cast(DecimalType(18, 4)))\
        .withColumn("BKWRD_CNSMPTN_DAYS_CNT", trim(col("VINT1")).cast(DecimalType(18, 4)))\
        .withColumn("FRWD_CNSMPTN_DAYS_CNT", trim(col("VINT2")).cast(DecimalType(18, 4)))\
        .withColumn("CNSMPTN_MODE_CD", trim(col("VRMOD")))\
        .withColumn("PRCHSNG_GRP_CD", trim(col("EKGRP")))\
        .withColumn("MMS_FIN_CLSN_CD", lit(None).cast(StringType()))\
        .withColumn("VMI_IND", lit(None).cast(StringType()))\
        .withColumn("MSTR_PLNG_FMLY_CD", lit(None).cast(StringType()))\
        .withColumn("ENTR_SPEC_MATL_PLNT_STS_CD", lit(None).cast(StringType()))\
        .withColumn("ENTR_PRCMT_TYPE_CD", lit(None).cast(StringType()))\
        .withColumn("MATL_HAZ_CD", lit(None).cast(StringType()))\
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
            .otherwise(to_timestamp(col("AUSDT"), "yyyyMMdd"))\
            .alias("EFF_OUT_DTTM")
        )\
        .withColumn("FLLP_MATL", trim(col("NFMAT")))\
        .withColumn("RQR_GRP_IN", trim(col("KZBED")))\
        .withColumn("MIX_MRP_IN", trim(col("MISKZ")))\
        .withColumn("BASE_QTY", trim(col("BASMG")).cast(DecimalType(18, 4)))\
        .withColumn("MAX_STRG_PER", trim(col("MAXLZ")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_FOR_MAX_STRG", trim(col("LZEIH")))\
        .withColumn("OVR_DELV_TLRNC", trim(col("UEETO")).cast(DecimalType(18, 4)))\
        .withColumn("UNLTD_OVER_DELV_IN", trim(col("UEETK")))\
        .withColumn("UND_DELV_TLRNC", trim(col("UNETO")).cast(DecimalType(18, 4)))\
        .withColumn("TOT_REPLN_LT", trim(col("WZEIT")).cast(DecimalType(18, 4)))\
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
        .withColumn("CMPNT_SCRAP_PCT", trim(col("KAUSF")).cast(DecimalType(18, 4)))\
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
        .withColumn("CUR_PER", trim(col("LFMON")).cast(IntegerType()))\
        .withColumn("INSP_INTV", trim(col("PRFRQ")).cast(DecimalType(18, 4)))\
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
            .otherwise(to_timestamp(col("PREND"), "yyyyMMdd"))\
            .alias("EXPT_CERT_DTTM")
        )\
        .withColumn("ORIG_BATCH_REF_MATL", trim(col("UCMAT")))\
        .withColumn("RESET_FCST_MDL", trim(col("AUTRU")))\
        .withColumn("SRVC_LVL", trim(col("LGRAD")).cast(DecimalType(18, 4)))\
        .withColumn("ACT_PUSH", trim(col("ZZ1_ACTIVATE_PUSH_PLT")))\
        .withColumn("MTS_MTO_FL", trim(col("ZZ1_MTS_MTO_FLAG_PLT")))\
        .withColumn("CNSMPTN_MODE", trim(col("ZZ1_CONSUMPTION_MODE_PLT")))\
        .withColumn("DUAL_SRCNG_WIP", trim(col("ZZ1_DUAL_SRC_WIP_PROJ_PLT")))\
        .withColumn("MATL_STS_DESC", col("MTSTB"))\
        .withColumn("MATL_PLNR_NM", col("DSNAM"))\
        .withColumn("PRDTN_SUPR_NM", col("TXT"))\
        .withColumn("PRCHSNG_GRP_DESC", col("EKNAM"))\
        .withColumn("EXCSE_TAX_RLVNCE", trim(col("EXCISE_TAX_RLVNCE")))\
        .withColumn("SEQ_NUM", trim(col("ZZ1_SEQUENCE_NUMBER_PLT")))\
        .withColumn("MAINT_STS", trim(col("PSTAT")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
