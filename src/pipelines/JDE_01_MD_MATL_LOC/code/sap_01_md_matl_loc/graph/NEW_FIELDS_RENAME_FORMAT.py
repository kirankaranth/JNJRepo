from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.SRC_SYS_CD))\
        .withColumn("MATL_NUM", col("iblitm"))\
        .withColumn("PLNT_CD", col("ibmcu"))\
        .withColumn("VAR_CNTL_CD", lit(None).cast(StringType()))\
        .withColumn("AUTO_PO_IND", lit(None).cast(StringType()))\
        .withColumn("PRCTR_CD", lit(None).cast(StringType()))\
        .withColumn("SRC_LIST_IND", lit(None).cast(StringType()))\
        .withColumn("LD_GRP_CD", lit(None).cast(StringType()))\
        .withColumn("QUAL_MGMT_CNTL_CD", lit(None).cast(StringType()))\
        .withColumn("REORDR_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("COST_LOT_SIZE_VAL", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("SERIZATION_PRFL_CD", lit(None).cast(StringType()))\
        .withColumn("SPEC_MATL_PLNT_STS_CD", trim(col("IBSTKT")))\
        .withColumn("SPEC_MATL_PLNT_STS_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("DEL_IND", lit(None).cast(StringType()))\
        .withColumn("LOT_SIZE_VAL", trim(col("IBOPC")))\
        .withColumn("MATL_PLNR_NUM", trim(col("IBANPL")))\
        .withColumn("PRDTN_SUPR_CD", lit(None).cast(StringType()))\
        .withColumn("PRDTN_UOM_CD", lit(None).cast(StringType()))\
        .withColumn("MATL_ABC_CLSN_CD", trim(col("IBABCS")))\
        .withColumn("AVLBLTY_CHK_IND", trim(col("IBCKAV")))\
        .withColumn("SFTY_TIME_IND", lit(None).cast(StringType()))\
        .withColumn("SPL_PRCMT_TYPE_CD", lit(Config.SPL_PRCMT_TYPE_CD))\
        .withColumn("PRCMT_TYPE_CD", trim(col("IBSTKT")))\
        .withColumn("MRP_TYPE_CD", trim(col("IBSRP6")))\
        .withColumn("ORIG_CTRY_CD", trim(col("IBORIG")))\
        .withColumn("PLNG_STRTGY_GRP_CD", lit(None).cast(StringType()))\
        .withColumn("RD_VAL_QTY", col("IBMULT").cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_FX_QTY", col("IBOPV").cast(DecimalType(18, 4)))\
        .withColumn("GOOD_RCPT_LEAD_DAYS_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("LOT_SIZE_MAX_QTY", col("IBRQMX").cast(DecimalType(22, 4)))\
        .withColumn("LOT_SIZE_MIN_QTY", col("IBRQMN").cast(DecimalType(18, 4)))\
        .withColumn("SFTY_STK_QTY", col("IBSAFE").cast(DecimalType(18, 4)))\
        .withColumn("PLNG_TIME_FENCE_DAYS_CNT", trim(col("IBLTLV")))\
        .withColumn("MAX_STK_LVL_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("SFTY_TIME_DAYS_CNT", lit(None).cast(StringType()))\
        .withColumn("PLAN_DELV_DAYS_CNT", lit(Config.PLAN_DELV_DAYS_CNT))\
        .withColumn("SCRAP_PCT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("INHS_PRDTN_DAYS_CNT", lit(Config.INHS_PRDTN_DAYS_CNT))\
        .withColumn("MIN_SFTY_STK_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("BKWRD_CNSMPTN_DAYS_CNT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("FRWD_CNSMPTN_DAYS_CNT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("CNSMPTN_MODE_CD", lit(None).cast(StringType()))\
        .withColumn("PRCHSNG_GRP_CD", lit(Config.PRCHSNG_GRP_CD))\
        .withColumn("MMS_FIN_CLSN_CD", lit(None).cast(StringType()))\
        .withColumn("VMI_IND", lit(Config.VMI_IND))\
        .withColumn("MSTR_PLNG_FMLY_CD", lit(Config.MSTR_PLNG_FMLY_CD))\
        .withColumn("ENTR_SPEC_MATL_PLNT_STS_CD", lit(None).cast(StringType()))\
        .withColumn("ENTR_PRCMT_TYPE_CD", lit(None).cast(StringType()))\
        .withColumn("MATL_HAZ_CD", lit(None).cast(StringType()))\
        .withColumn("VALUT_CAT", lit(Config.VALUT_CAT))\
        .withColumn("BTCH_MGMT_IN_INTRNL", lit(None).cast(StringType()))\
        .withColumn("MRP_PRFL", lit(None).cast(StringType()))\
        .withColumn("PER_IN", lit(None).cast(StringType()))\
        .withColumn("DEPN_REQ_IN", lit(None).cast(StringType()))\
        .withColumn("STRG_COST_PCT_CD", lit(None).cast(StringType()))\
        .withColumn("DSCNT_IN", lit(None).cast(StringType()))\
        .withColumn("EFF_OUT_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("FLLP_MATL", lit(Config.FLLP_MATL))\
        .withColumn("RQR_GRP_IN", lit(None).cast(StringType()))\
        .withColumn("MIX_MRP_IN", lit(None).cast(StringType()))\
        .withColumn("BASE_QTY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("MAX_STRG_PER", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_FOR_MAX_STRG", lit(None).cast(StringType()))\
        .withColumn("OVR_DELV_TLRNC", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("UNLTD_OVER_DELV_IN", lit(None).cast(StringType()))\
        .withColumn("UND_DELV_TLRNC", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("TOT_REPLN_LT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("INSP_STK", lit(None).cast(StringType()))\
        .withColumn("BTCH_MGMT_PLNT_IN", lit(None).cast(StringType()))\
        .withColumn("FISC_YR_VRNT", lit(None).cast(StringType()))\
        .withColumn("CMMDTY_CD", lit(None).cast(StringType()))\
        .withColumn("RGN_OF_ORIG_OF_MATL", lit(None).cast(StringType()))\
        .withColumn("CMMDTY_CD_UOM", lit(None).cast(StringType()))\
        .withColumn("REP_MFG_IN", lit(None).cast(StringType()))\
        .withColumn("VERS_IN", lit(None).cast(StringType()))\
        .withColumn("ALT_BOM", lit(None).cast(StringType()))\
        .withColumn("BOM_USG", lit(None).cast(StringType()))\
        .withColumn("KEY_TASK_LIST", lit(None).cast(StringType()))\
        .withColumn("GRP_CNTR", lit(None).cast(StringType()))\
        .withColumn("ISS_STRG_LOC", lit(None).cast(StringType()))\
        .withColumn("MRP_GRP", lit(None).cast(StringType()))\
        .withColumn("CMPNT_SCRAP_PCT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("CERT_TYPE", lit(None).cast(StringType()))\
        .withColumn("INSP_SETUP_FOR_MATL", lit(None).cast(StringType()))\
        .withColumn("PHY_INV_CC", lit(None).cast(StringType()))\
        .withColumn("CONFIG_MATL", lit(None).cast(StringType()))\
        .withColumn("REP_MFG_PRFL", lit(None).cast(StringType()))\
        .withColumn("NEG_STK_PLNT", lit(None).cast(StringType()))\
        .withColumn("PLNG_CYC", lit(None).cast(StringType()))\
        .withColumn("RD_PRFL", lit(None).cast(StringType()))\
        .withColumn("CO_PROD_IN", lit(None).cast(StringType()))\
        .withColumn("BULK_MATL_IN", lit(None).cast(StringType()))\
        .withColumn("FIX_CC_IN", lit(None).cast(StringType()))\
        .withColumn("PROD_SCHDLNG_PRFL", lit(None).cast(StringType()))\
        .withColumn("CUR_PER", lit(None).cast(IntegerType()))\
        .withColumn("INSP_INTV", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("DOC_REQ_IN", lit(None).cast(StringType()))\
        .withColumn("MATL_FRGHT_GRP", lit(None).cast(StringType()))\
        .withColumn("MATL_AUTH_QM", lit(None).cast(StringType()))\
        .withColumn("CNTL_CODE", lit(Config.CNTL_CODE))\
        .withColumn("UNIT_OF_ISS", lit(None).cast(StringType()))\
        .withColumn("ALT_BOM_METH", lit(None).cast(StringType()))\
        .withColumn("SCHDLNG_MRGN_KEY", lit(None).cast(StringType()))\
        .withColumn("EXPT_IMPT_GRP", lit(None).cast(StringType()))\
        .withColumn("BACKFLUSH_IN", lit(None).cast(StringType()))\
        .withColumn("MM_DFLT_SUPP_AREA", lit(Config.MM_DFLT_SUPP_AREA))\
        .withColumn("BTCH_ENT_PO", lit(None).cast(StringType()))\
        .withColumn("ORIG_BATCH_MGMT", lit(None).cast(StringType()))\
        .withColumn("MATL_CFOP_CAT", lit(None).cast(StringType()))\
        .withColumn("STRG_LOC_EXTRNL_PRCMT", lit(None).cast(StringType()))\
        .withColumn("STK_DTRMN_GRP", lit(None).cast(StringType()))\
        .withColumn("DO_NOT_COST_IN", lit(None).cast(StringType()))\
        .withColumn("SPL_PRCMT_TYPE_COST", lit(None).cast(StringType()))\
        .withColumn("CVGE_PRFL_RNG", lit(None).cast(StringType()))\
        .withColumn("EXPT_CERT_NUM", lit(None).cast(StringType()))\
        .withColumn("EXPT_CERT_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("ORIG_BATCH_REF_MATL", lit(None).cast(StringType()))\
        .withColumn("RESET_FCST_MDL", lit(None).cast(StringType()))\
        .withColumn("SRVC_LVL", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("ACT_PUSH", lit(None).cast(StringType()))\
        .withColumn("MTS_MTO_FL", lit(Config.MTS_MTO_FL))\
        .withColumn("CNSMPTN_MODE", lit(None).cast(StringType()))\
        .withColumn("DUAL_SRCNG_WIP", lit(None).cast(StringType()))\
        .withColumn("MATL_STS_DESC", lit(None).cast(StringType()))\
        .withColumn("MATL_PLNR_NM", lit(None).cast(StringType()))\
        .withColumn("PRDTN_SUPR_NM", lit(None).cast(StringType()))\
        .withColumn("PRCHSNG_GRP_DESC", lit(None).cast(StringType()))\
        .withColumn("EXCSE_TAX_RLVNCE", lit(None).cast(StringType()))\
        .withColumn("SEQ_NUM", lit(None).cast(StringType()))\
        .withColumn("MAINT_STS", lit(None).cast(StringType()))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", to_timestamp(current_timestamp(), "yyyy-MM-dd"))\
        .withColumn("DAI_UPDT_DTTM", to_timestamp(current_timestamp(), "yyyy-MM-dd"))
