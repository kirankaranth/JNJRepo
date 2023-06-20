from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_hmd.config.ConfigStore import *
from sap_md_matl_loc_hmd.udfs.UDFs import *

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
        lit(Config.DAI_ETL_ID).alias("DAI_ETL_ID"), 
        current_timestamp().alias("DAI_CRT_DTTM"), 
        current_timestamp().alias("DAI_UPDT_DTTM"), 
        col("_upt_").alias("_l0_upt_"), 
        to_json(expr("named_struct('SCR_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD)")).alias("_pk_"), 
        md5(to_json(expr("named_struct('SCR_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD)")))\
          .alias("_pk_md5_"), 
        current_timestamp().alias("_l1_upt_"), 
        lit("F").alias("_deleted_"), 
        trim(col("BWTTY")).alias("VALUT_CAT"), 
        trim(col("XCHAR")).alias("BTCH_MGMT_IN_INTRNL"), 
        trim(col("DISPR")).alias("MRP_PRFL"), 
        trim(col("PERKZ")).alias("PER_IN"), 
        trim(col("SBDKZ")).alias("DEPN_REQ_IN"), 
        trim(col("LAGPR")).alias("STRG_COST_PCT_CD"), 
        trim(col("KZAUS")).alias("DSCNT_IN"), 
        when((col("AUSDT") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("AUSDT"), "yyyyMMdd"))\
          .alias("EFF_OUT_DTTM"), 
        trim(col("NFMAT")).alias("FLLP_MATL"), 
        trim(col("KZBED")).alias("RQR_GRP_IN"), 
        trim(col("MISKZ")).alias("MIX_MRP_IN"), 
        col("BASMG").cast(DecimalType(18, 4)).alias("BASE_QTY"), 
        col("MAXLZ").cast(DecimalType(18, 4)).alias("MAX_STRG_PER"), 
        trim(col("LZEIH")).alias("UNIT_FOR_MAX_STRG"), 
        col("UEETO").cast(DecimalType(18, 4)).alias("OVR_DELV_TLRNC"), 
        trim(col("UEETK")).alias("UNLTD_OVER_DELV_IN"), 
        col("UNETO").cast(DecimalType(18, 4)).alias("UND_DELV_TLRNC"), 
        col("WZEIT").cast(DecimalType(18, 4)).alias("TOT_REPLN_LT"), 
        trim(col("INSMK")).alias("INSP_STK"), 
        trim(col("XCHPF")).alias("BTCH_MGMT_PLNT_IN"), 
        trim(col("PERIV")).alias("FISC_YR_VRNT"), 
        trim(col("STAWN")).alias("CMMDTY_CD"), 
        trim(col("HERKR")).alias("RGN_OF_ORIG_OF_MATL"), 
        trim(col("EXPME")).alias("CMMDTY_CD_UOM"), 
        trim(col("SAUFT")).alias("REP_MFG_IN"), 
        trim(col("VERKZ")).alias("VERS_IN"), 
        trim(col("STLAL")).alias("ALT_BOM"), 
        trim(col("STLAN")).alias("BOM_USG"), 
        trim(col("PLNNR")).alias("KEY_TASK_LIST"), 
        trim(col("APLAL")).alias("GRP_CNTR"), 
        trim(col("LGPRO")).alias("ISS_STRG_LOC"), 
        trim(col("DISPR")).alias("MRP_GRP"), 
        col("KAUSF").cast(DecimalType(18, 4)).alias("CMPNT_SCRAP_PCT"), 
        trim(col("QZGTP")).alias("CERT_TYPE"), 
        trim(col("QMATV")).alias("INSP_SETUP_FOR_MATL"), 
        trim(col("ABCIN")).alias("PHY_INV_CC"), 
        trim(col("STDPD")).alias("CONFIG_MATL"), 
        trim(col("SFEPR")).alias("REP_MFG_PRFL"), 
        trim(col("XMCNG")).alias("NEG_STK_PLNT"), 
        trim(col("LFRHY")).alias("PLNG_CYC"), 
        trim(col("RDPRF")).alias("RD_PRFL"), 
        trim(col("KZKUP")).alias("CO_PROD_IN"), 
        trim(col("SCHGT")).alias("BULK_MATL_IN"), 
        trim(col("CCFIX")).alias("FIX_CC_IN"), 
        trim(col("SFCPF")).alias("PROD_SCHDLNG_PRFL"), 
        col("LFMON").cast(IntegerType()).alias("CUR_PER"), 
        col("PRFRQ").cast(DecimalType(18, 4)).alias("INSP_INTV"), 
        trim(col("KZDKZ")).alias("DOC_REQ_IN"), 
        trim(col("MFRGR")).alias("MATL_FRGHT_GRP"), 
        trim(col("QMATA")).alias("MATL_AUTH_QM"), 
        trim(col("STEUC")).alias("CNTL_CODE"), 
        trim(col("AUSME")).alias("UNIT_OF_ISS"), 
        trim(col("ALTSL")).alias("ALT_BOM_METH"), 
        trim(col("FHORI")).alias("SCHDLNG_MRGN_KEY"), 
        trim(col("MTVER")).alias("EXPT_IMPT_GRP"), 
        trim(col("RGEKZ")).alias("BACKFLUSH_IN"), 
        trim(col("VSPVB")).alias("MM_DFLT_SUPP_AREA"), 
        trim(col("KZECH")).alias("BTCH_ENT_PO"), 
        trim(col("UCHKZ")).alias("ORIG_BATCH_MGMT"), 
        trim(col("INDUS")).alias("MATL_CFOP_CAT"), 
        trim(col("LGFSB")).alias("STRG_LOC_EXTRNL_PRCMT"), 
        trim(col("EPRIO")).alias("STK_DTRMN_GRP"), 
        trim(col("NCOST")).alias("DO_NOT_COST_IN"), 
        trim(col("SOBSK")).alias("SPL_PRCMT_TYPE_COST"), 
        trim(col("RWPRO")).alias("CVGE_PRFL_RNG"), 
        trim(col("PRENO")).alias("EXPT_CERT_NUM"), 
        when((col("PREND") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("PREND"), "yyyyMMdd"))\
          .alias("EXPT_CERT_DTTM"), 
        trim(col("UCMAT")).alias("ORIG_BATCH_REF_MATL"), 
        trim(col("AUTRU")).alias("RESET_FCST_MDL"), 
        trim(col("LGRAD")).cast(DecimalType(18, 4)).alias("SRVC_LVL"), 
        trim(col("ZZ1_ACTIVATE_PUSH_PLT")).alias("ACT_PUSH"), 
        trim(col("ZZ1_MTS_MTO_FLAG_PLT")).alias("MTS_MTO_FL"), 
        trim(col("ZZ1_CONSUMPTION_MODE_PLT")).alias("CNSMPTN_MODE"), 
        trim(col("ZZ1_DUAL_SRC_WIP_PROJ_PLT")).alias("DUAL_SRCNG_WIP"), 
        trim(col("MTSTB")).alias("MATL_STS_DESC"), 
        trim(col("DSNAM")).alias("MATL_PLNR_NM"), 
        trim(col("TXT")).alias("PRDTN_SUPR_NM"), 
        trim(col("EKNAM")).alias("PRCHSNG_GRP_DESC"), 
        trim(col("EXCISE_TAX_RLVNCE")).alias("EXCSE_TAX_RLVNCE"), 
        trim(col("ZZ1_SEQUENCE_NUMBER_PLT")).alias("SEQ_NUM"), 
        trim(col("PSTAT")).alias("MAINT_STS")
    )
