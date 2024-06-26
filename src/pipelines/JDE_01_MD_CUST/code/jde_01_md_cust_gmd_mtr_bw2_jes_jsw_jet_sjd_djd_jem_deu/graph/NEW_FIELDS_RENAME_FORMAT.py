from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.config.ConfigStore import *
from jde_01_md_cust_gmd_mtr_bw2_jes_jsw_jet_sjd_djd_jem_deu.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CUST_NUM", col("ABAN8"))\
        .withColumn("BLOCK_TYPE_CD", lit(None).cast(StringType()))\
        .withColumn("GLN1_NBR", lit(None).cast(StringType()))\
        .withColumn("GLN2_NBR", lit(None).cast(StringType()))\
        .withColumn("GLOBL_COT_CD", lit(None).cast(StringType()))\
        .withColumn("COT_CD", lit(None).cast(StringType()))\
        .withColumn("COT3_CD", lit(None).cast(StringType()))\
        .withColumn("COT4_CD", lit(None).cast(StringType()))\
        .withColumn("COT5_CD", lit(None).cast(StringType()))\
        .withColumn("SGMNT_CD", lit(None).cast(StringType()))\
        .withColumn("GLN3_NBR", lit(None).cast(StringType()))\
        .withColumn("SLS_ORDR_BLK_CD", lit(None).cast(StringType()))\
        .withColumn("CFOP_CAT_CD", lit(None).cast(StringType()))\
        .withColumn("CMPT_IND", lit(None).cast(StringType()))\
        .withColumn("CNSMR_IND", lit(None).cast(StringType()))\
        .withColumn("CRT_ON_DTTM", to_timestamp(lit(None)))\
        .withColumn("SLS_INVC_BLK_CD", lit(None).cast(StringType()))\
        .withColumn("TAX_TYPE_CD", lit(None).cast(StringType()))\
        .withColumn("CUST_TYPE_CD", lit(None).cast(StringType()))\
        .withColumn("CUST_NM1", trim(col("ABALPH")))\
        .withColumn("CUST_NM2", trim(col("ABALP1")))\
        .withColumn("CUST_NM3", lit(None).cast(StringType()))\
        .withColumn("CUST_NM4", lit(None).cast(StringType()))\
        .withColumn("NLSN_CD", lit(None).cast(StringType()))\
        .withColumn("REGN_MKT_CD", lit(None).cast(StringType()))\
        .withColumn("CUST_SHRT_NM", trim(col("ABALKY")))\
        .withColumn("LANG_CD", lit(None).cast(StringType()))\
        .withColumn("TAX_NUM1", lit(None).cast(StringType()))\
        .withColumn("TAX_NUM2", lit(None).cast(StringType()))\
        .withColumn("TAX_NUM3", lit(None).cast(StringType()))\
        .withColumn("TAX_NUM4", lit(None).cast(StringType()))\
        .withColumn("TAX_NUM5", lit(None).cast(StringType()))\
        .withColumn("VAT_NUM", lit(None).cast(StringType()))\
        .withColumn("NTRL_PRSN_IND", lit(None).cast(StringType()))\
        .withColumn("ICMS_TAX_CD", lit(None).cast(StringType()))\
        .withColumn("IPI_TAX_CD", lit(None).cast(StringType()))\
        .withColumn("CHG_ON_DTTM", to_timestamp(lit(None)))\
        .withColumn("TRAD_PTNR_CO_CD", lit(None).cast(StringType()))\
        .withColumn("ICMS_EXPT_IND", lit(None).cast(StringType()))\
        .withColumn("SUBST_TAX_GRP_CD", lit(None).cast(StringType()))\
        .withColumn("IPI_EXPT_IND", lit(None).cast(StringType()))\
        .withColumn("ADDR_NUM", lit(None).cast(StringType()))\
        .withColumn("STREET_HS_NUM", lit(None).cast(StringType()))\
        .withColumn("CTRY_RGN_KEY", lit(None).cast(StringType()))\
        .withColumn("CITY", lit(None).cast(StringType()))\
        .withColumn("PSTL_CD", lit(None).cast(StringType()))\
        .withColumn("RGN", lit(None).cast(StringType()))\
        .withColumn("TRSPN_ZN", lit(None).cast(StringType()))\
        .withColumn("SLS_DOC_BLOK_RSN_TEXT", lit(None).cast(StringType()))\
        .withColumn("DESC_INDSTR_KEY", lit(None).cast(StringType()))\
        .withColumn("FST_PHN_NUM", lit(None).cast(StringType()))\
        .withColumn("FAX_NUM", lit(None).cast(StringType()))\
        .withColumn("IN_ACCT_ONE_TIME_ACCT", lit(None).cast(StringType()))\
        .withColumn("SRCH_TERM_MATCH_CD_SRCH_1", lit(None).cast(StringType()))\
        .withColumn("SRCH_TERM_MATCH_CD_SRCH_2", lit(None).cast(StringType()))\
        .withColumn("SRCH_TERM_MATCH_CD_SRCH_3", lit(None).cast(StringType()))\
        .withColumn("TITLE_ANRED", lit(None).cast(StringType()))\
        .withColumn("EXP_TRAIN_STN", lit(None).cast(StringType()))\
        .withColumn("TRAIN_STN", lit(None).cast(StringType()))\
        .withColumn("AUTH_GRP", lit(None).cast(StringType()))\
        .withColumn("DATA_COMM_LINE_NUM", lit(None).cast(StringType()))\
        .withColumn("NM_PRSN_CRT_OBJ", lit(None).cast(StringType()))\
        .withColumn("IN_UNLD_PT_EXIST", lit(None).cast(StringType()))\
        .withColumn("ACCT_NUM_MSTR_REC_FISC_ADDR", lit(None).cast(StringType()))\
        .withColumn("WRK_TIME_CAL", lit(None).cast(StringType()))\
        .withColumn("ACCT_NUM_ALT_PYR", lit(None).cast(StringType()))\
        .withColumn("GRP_KEY", lit(None).cast(StringType()))\
        .withColumn("CUST_CLSN", lit(None).cast(StringType()))\
        .withColumn("ACCT_NUM_SUP", lit(None).cast(StringType()))\
        .withColumn("CENT_DELV_BLK_CUST", lit(None).cast(StringType()))\
        .withColumn("CITY_COORDNT", lit(None).cast(StringType()))\
        .withColumn("CENT_DEL_FL_MSTR_REC", lit(None).cast(StringType()))\
        .withColumn("DSTRC", lit(None).cast(StringType()))\
        .withColumn("PO_BOX", lit(None).cast(StringType()))\
        .withColumn("PO_BOX_PSTL_CD", lit(None).cast(StringType()))\
        .withColumn("CNTY_CD", lit(None).cast(StringType()))\
        .withColumn("CITY_CD", lit(None).cast(StringType()))\
        .withColumn("CENT_PSTNG_BLK", lit(None).cast(StringType()))\
        .withColumn("IN_BUSN_PTNR_SUBJ_EQLZTN_TAX", lit(None).cast(StringType()))\
        .withColumn("LIAB_VAT", lit(None).cast(StringType()))\
        .withColumn("TELEBOX_NUM", lit(None).cast(StringType()))\
        .withColumn("SEC_PHN_NUM", lit(None).cast(StringType()))\
        .withColumn("TELETEX_NUM", lit(None).cast(StringType()))\
        .withColumn("TELEX_NUM", lit(None).cast(StringType()))\
        .withColumn("IN_ALT_PYR_ALLW_DOC", lit(None).cast(StringType()))\
        .withColumn("IN_SLS_PTNR", lit(None).cast(StringType()))\
        .withColumn("IN_SLS_PROSP", lit(None).cast(StringType()))\
        .withColumn("IN_CUST_TYPE_4", lit(None).cast(StringType()))\
        .withColumn("ID_DFLT_SOLD_TO_PRTY", lit(None).cast(StringType()))\
        .withColumn("LEGAL_STS", lit(None).cast(StringType()))\
        .withColumn("INIT_CNTCT", lit(None).cast(StringType()))\
        .withColumn("ANNL_SLS_UMSAT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("YR_SLS_GVN", lit(None).cast(StringType()))\
        .withColumn("CRNCY_SLS_FIG", lit(None).cast(StringType()))\
        .withColumn("YR_NUM_EMP", lit(None).cast(StringType()))\
        .withColumn("YR_NUM_EMP_GVN", lit(None).cast(StringType()))\
        .withColumn("ATTR_1", lit(None).cast(StringType()))\
        .withColumn("ATTR_2", lit(None).cast(StringType()))\
        .withColumn("ATTR_3", lit(None).cast(StringType()))\
        .withColumn("ATTR_4", lit(None).cast(StringType()))\
        .withColumn("ATTR_5", lit(None).cast(StringType()))\
        .withColumn("ATTR_6", lit(None).cast(StringType()))\
        .withColumn("ATTR_7", lit(None).cast(StringType()))\
        .withColumn("ATTR_8", lit(None).cast(StringType()))\
        .withColumn("ATTR_9", lit(None).cast(StringType()))\
        .withColumn("ATTR_10", lit(None).cast(StringType()))\
        .withColumn("ANNL_SLS_UMSA1", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("TAX_JURIS", lit(None).cast(StringType()))\
        .withColumn("FISC_YR_VRNT", lit(None).cast(StringType()))\
        .withColumn("USG_IN", lit(None).cast(StringType()))\
        .withColumn("INSP_CARR_OUT_BY_CUST", lit(None).cast(StringType()))\
        .withColumn("INSP_DELV_NOTE_AFTER_OUTB_DELV", lit(None).cast(StringType()))\
        .withColumn("REF_ACCT_GRP_ONE_TIME_ACCT", lit(None).cast(StringType()))\
        .withColumn("PO_BOX_CITY", lit(None).cast(StringType()))\
        .withColumn("PLNT", lit(None).cast(StringType()))\
        .withColumn("RPT_KEY_DATA_MED_EXCH", lit(None).cast(StringType()))\
        .withColumn("INSTR_KEY_DATA_MED_EXCH", lit(None).cast(StringType()))\
        .withColumn("STS_DATA_TFR_INTO_SUBSQ_RLSE", lit(None).cast(StringType()))\
        .withColumn("ASGNMT_TO_HIER", lit(None).cast(StringType()))\
        .withColumn("PMT_BLK", lit(None).cast(StringType()))\
        .withColumn("IS_R_LABLNG_CUST_PLNT_GRP", lit(None).cast(StringType()))\
        .withColumn("ID_MN_NON_MIL_USE", lit(None).cast(StringType()))\
        .withColumn("ID_MN_MIL_USE", lit(None).cast(StringType()))\
        .withColumn("CUST_COND_GRP_1", lit(None).cast(StringType()))\
        .withColumn("CUST_COND_GRP_2", lit(None).cast(StringType()))\
        .withColumn("CUST_COND_GRP_3", lit(None).cast(StringType()))\
        .withColumn("CUST_COND_GRP_4", lit(None).cast(StringType()))\
        .withColumn("CUST_COND_GRP_5", lit(None).cast(StringType()))\
        .withColumn("IN_ALT_PYR_USE_ACCT_NUM", lit(None).cast(StringType()))\
        .withColumn("TAX_NUM_TYPE", lit(None).cast(StringType()))\
        .withColumn("IN_BIOCH_WARF_LEGAL_CNTL", lit(None).cast(StringType()))\
        .withColumn("IN_NUCLR_NONPROLIF_LEGAL_CNTL", lit(None).cast(StringType()))\
        .withColumn("IN_NATL_SCTY_LEGAL_CNTL", lit(None).cast(StringType()))\
        .withColumn("IN_MISSILE_TECH_LEGAL_CNTL", lit(None).cast(StringType()))\
        .withColumn("UNIFM_RSRS_LCTR", lit(None).cast(StringType()))\
        .withColumn("NM_REP", lit(None).cast(StringType()))\
        .withColumn("TYPE_BUSN", lit(None).cast(StringType()))\
        .withColumn("TYPE_INDSTR", lit(None).cast(StringType()))\
        .withColumn("STS_CHG_AUTH", lit(None).cast(StringType()))\
        .withColumn("CENT_DEL_BLK_MSTR_REC", lit(None).cast(StringType()))\
        .withColumn("BUSN_PRPS_CMPLT_FL", lit(None).cast(StringType()))\
        .withColumn("SUFRAMA_CD", lit(None).cast(StringType()))\
        .withColumn("RG_NUM", lit(None).cast(StringType()))\
        .withColumn("ISS_BY", lit(None).cast(StringType()))\
        .withColumn("ST", lit(None).cast(StringType()))\
        .withColumn("RG_ISU_DTTM", to_timestamp(lit(None)))\
        .withColumn("RIC_NUM", lit(None).cast(StringType()))\
        .withColumn("FRGN_NATL_REGS", lit(None).cast(StringType()))\
        .withColumn("RNE_ISU_DTTM", to_timestamp(lit(None)))\
        .withColumn("CNAE", lit(None).cast(StringType()))\
        .withColumn("LEGAL_NATR", lit(None).cast(StringType()))\
        .withColumn("CRT_NUM", lit(None).cast(StringType()))\
        .withColumn("ICMS_TAXPY", lit(None).cast(StringType()))\
        .withColumn("INDSTR_MN_TYPE", lit(None).cast(StringType()))\
        .withColumn("TAX_DCLTN_TYPE", lit(None).cast(StringType()))\
        .withColumn("CO_SIZE", lit(None).cast(StringType()))\
        .withColumn("DCLTN_RGMN_PIS_COFINS", lit(None).cast(StringType()))\
        .withColumn("MAX_STCK_HGHT_PKGNG_MATL", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_LGTH_PKGNG_MATL", lit(None).cast(StringType()))\
        .withColumn("CUST_RLTD_PACK_EA_PKGNG_MATL", lit(None).cast(StringType()))\
        .withColumn("PKGNG_MATL_CUST_VSO", lit(None).cast(StringType()))\
        .withColumn("NUM_LYR_UND_INTER_PLLT", lit(None).cast(StringType()))\
        .withColumn("PACK_MATL_SPEC_EA_PKGNG_MATL", lit(None).cast(StringType()))\
        .withColumn("PACK_ONLY_ONE_PKG_TYPE_EA_PKM", lit(None).cast(StringType()))\
        .withColumn("SIDE_PREF_LD_UNLD", lit(None).cast(StringType()))\
        .withColumn("FRN_BK_PREF_LD_UNLD", lit(None).cast(StringType()))\
        .withColumn("COLL_UNLD_PT_VSO", lit(None).cast(StringType()))\
        .withColumn("AGN_LOC_CD", lit(None).cast(StringType()))\
        .withColumn("PMT_OFF", lit(None).cast(StringType()))\
        .withColumn("FEE_SCHED", lit(None).cast(StringType()))\
        .withColumn("DUNS_NUM", lit(None).cast(StringType()))\
        .withColumn("DUNS_4", lit(None).cast(StringType()))\
        .withColumn("PROC_GRP", lit(None).cast(StringType()))\
        .withColumn("SUBLDGR_ACCT_PRPRC_PCDR", lit(None).cast(StringType()))\
        .withColumn("NM_1", lit(None).cast(StringType()))\
        .withColumn("NM_2", lit(None).cast(StringType()))\
        .withColumn("NM_3", lit(None).cast(StringType()))\
        .withColumn("FST_NM", lit(None).cast(StringType()))\
        .withColumn("TITLE_PSOTL", lit(None).cast(StringType()))\
        .withColumn("HS_NUM_NO_LONG_USED_FROM_RLSE", lit(None).cast(StringType()))\
        .withColumn("STREET_NO_LONG_USED_FROM_RLSE", lit(None).cast(StringType()))\
        .withColumn("DESC_PSOO1", lit(None).cast(StringType()))\
        .withColumn("DESC_PSOO2", lit(None).cast(StringType()))\
        .withColumn("DESC_PSOO3", lit(None).cast(StringType()))\
        .withColumn("DESC_PSOO4", lit(None).cast(StringType()))\
        .withColumn("DESC_PSOO5", lit(None).cast(StringType()))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("VLD_FROM_DTTM", to_timestamp(lit(None)))\
        .withColumn("VLD_TO_DTTM", to_timestamp(lit(None)))\
        .withColumn("TIME_LAST_CHG_CNFRM", to_timestamp(lit(None)))\
        .withColumn("OBSOL_LAST_CHG_ON", to_timestamp(lit(None)))\
        .withColumn("LAST_SETLM_DTTM", to_timestamp(lit(None)))\
        .withColumn("CUR_SETLM_DTTM", to_timestamp(lit(None)))\
        .withColumn("HRS_PER_MO", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("HRS_PER_DAY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("NUM_OF_DAYS_MO", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("LAST_CHG_CNFRM_DTTM", to_timestamp(lit(None)))
