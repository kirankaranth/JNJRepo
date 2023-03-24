from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def sql_MD_CUST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CUST_NUM,\ncast('' as string) as BLOCK_TYPE_CD,\ncast('' as string) as GLN1_NBR,\ncast('' as string) as GLN2_NBR,\ncast('' as string) as GLOBL_COT_CD,\ncast('' as string) as COT_CD,\ncast('' as string) as COT3_CD,\ncast('' as string) as COT4_CD,\ncast('' as string) as COT5_CD,\ncast('' as string) as SGMNT_CD,\ncast('' as string) as GLN3_NBR,\ncast('' as string) as SLS_ORDR_BLK_CD,\ncast('' as string) as CFOP_CAT_CD,\ncast('' as string) as CMPT_IND,\ncast('' as string) as CNSMR_IND,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as SLS_INVC_BLK_CD,\ncast('' as string) as TAX_TYPE_CD,\ncast('' as string) as CUST_TYPE_CD,\ncast('' as string) as CUST_NM1,\ncast('' as string) as CUST_NM2,\ncast('' as string) as CUST_NM3,\ncast('' as string) as CUST_NM4,\ncast('' as string) as NLSN_CD,\ncast('' as string) as REGN_MKT_CD,\ncast('' as string) as CUST_SHRT_NM,\ncast('' as string) as LANG_CD,\ncast('' as string) as TAX_NUM1,\ncast('' as string) as TAX_NUM2,\ncast('' as string) as TAX_NUM3,\ncast('' as string) as TAX_NUM4,\ncast('' as string) as TAX_NUM5,\ncast('' as string) as VAT_NUM,\ncast('' as string) as NTRL_PRSN_IND,\ncast('' as string) as ICMS_TAX_CD,\ncast('' as string) as IPI_TAX_CD,\ncast('' as timestamp) as CHG_ON_DTTM,\ncast('' as string) as TRAD_PTNR_CO_CD,\ncast('' as string) as ICMS_EXPT_IND,\ncast('' as string) as SUBST_TAX_GRP_CD,\ncast('' as string) as IPI_EXPT_IND,\ncast('' as string) as ADDR_NUM,\ncast('' as string) as DESC_INDSTR_CD_1,\ncast('' as string) as DESC_INDSTR_CD_2,\ncast('' as string) as DESC_INDSTR_CD_3,\ncast('' as string) as ACCT_GRP_NM,\ncast('' as string) as CUST_ACCT_GRP_NM,\ncast('' as string) as INDSTR_CD,\ncast('' as string) as STREET_HS_NUM,\ncast('' as string) as ENTRP_COT_CD,\ncast('' as string) as DSTN_MODE_CD,\ncast('' as string) as MD_SECTR_IND,\ncast('' as string) as PHARM_SECTR_IND,\ncast('' as string) as CUST_STS_CD,\ncast('' as timestamp) as VLD_FROM_DTTM,\ncast('' as timestamp) as VLD_TO_DTTM,\ncast('' as string) as CTRY_RGN_KEY,\ncast('' as string) as CITY,\ncast('' as string) as PSTL_CD,\ncast('' as string) as RGN,\ncast('' as string) as LANG_KEY,\ncast('' as string) as TRSPN_ZN,\ncast('' as string) as SLS_DOC_BLOK_RSN_TEXT,\ncast('' as string) as DESC_INDSTR_KEY,\ncast('' as string) as FST_PHN_NUM,\ncast('' as string) as FAX_NUM,\ncast('' as string) as IN_ACCT_ONE_TIME_ACCT,\ncast('' as string) as SRCH_TERM_MATCH_CD_SRCH_1,\ncast('' as string) as SRCH_TERM_MATCH_CD_SRCH_2,\ncast('' as string) as SRCH_TERM_MATCH_CD_SRCH_3,\ncast('' as string) as TITLE_ANRED,\ncast('' as string) as EXP_TRAIN_STN,\ncast('' as string) as TRAIN_STN,\ncast('' as string) as AUTH_GRP,\ncast('' as string) as DATA_COMM_LINE_NUM,\ncast('' as string) as NM_PRSN_CRT_OBJ,\ncast('' as string) as IN_UNLD_PT_EXIST,\ncast('' as string) as ACCT_NUM_MSTR_REC_FISC_ADDR,\ncast('' as string) as WRK_TIME_CAL,\ncast('' as string) as ACCT_NUM_ALT_PYR,\ncast('' as string) as GRP_KEY,\ncast('' as string) as CUST_CLSN,\ncast('' as string) as ACCT_NUM_SUP,\ncast('' as string) as CENT_DELV_BLK_CUST,\ncast('' as string) as CITY_COORDNT,\ncast('' as string) as CENT_DEL_FL_MSTR_REC,\ncast('' as string) as DSTRC,\ncast('' as string) as PO_BOX,\ncast('' as string) as PO_BOX_PSTL_CD,\ncast('' as string) as CNTY_CD,\ncast('' as string) as CITY_CD,\ncast('' as string) as CENT_PSTNG_BLK,\ncast('' as string) as IN_BUSN_PTNR_SUBJ_EQLZTN_TAX,\ncast('' as string) as LIAB_VAT,\ncast('' as string) as TELEBOX_NUM,\ncast('' as string) as SEC_PHN_NUM,\ncast('' as string) as TELETEX_NUM,\ncast('' as string) as TELEX_NUM,\ncast('' as string) as IN_ALT_PYR_ALLW_DOC,\ncast('' as string) as IN_SLS_PTNR,\ncast('' as string) as IN_SLS_PROSP,\ncast('' as string) as IN_CUST_TYPE_4,\ncast('' as string) as ID_DFLT_SOLD_TO_PRTY,\ncast('' as string) as LEGAL_STS,\ncast('' as string) as INIT_CNTCT,\ncast('' as decimal(18,4)) as ANNL_SLS_UMSAT,\ncast('' as string) as YR_SLS_GVN,\ncast('' as string) as CRNCY_SLS_FIG,\ncast('' as string) as YR_NUM_EMP,\ncast('' as string) as YR_NUM_EMP_GVN,\ncast('' as string) as ATTR_1,\ncast('' as string) as ATTR_2,\ncast('' as string) as ATTR_3,\ncast('' as string) as ATTR_4,\ncast('' as string) as ATTR_5,\ncast('' as string) as ATTR_6,\ncast('' as string) as ATTR_7,\ncast('' as string) as ATTR_8,\ncast('' as string) as ATTR_9,\ncast('' as string) as ATTR_10,\ncast('' as decimal(18,4)) as ANNL_SLS_UMSA1,\ncast('' as string) as TAX_JURIS,\ncast('' as string) as FISC_YR_VRNT,\ncast('' as string) as USG_IN,\ncast('' as string) as INSP_CARR_OUT_BY_CUST,\ncast('' as string) as INSP_DELV_NOTE_AFTER_OUTB_DELV,\ncast('' as string) as REF_ACCT_GRP_ONE_TIME_ACCT,\ncast('' as string) as PO_BOX_CITY,\ncast('' as string) as PLNT,\ncast('' as string) as RPT_KEY_DATA_MED_EXCH,\ncast('' as string) as INSTR_KEY_DATA_MED_EXCH,\ncast('' as string) as STS_DATA_TFR_INTO_SUBSQ_RLSE,\ncast('' as string) as ASGNMT_TO_HIER,\ncast('' as string) as PMT_BLK,\ncast('' as string) as IS_R_LABLNG_CUST_PLNT_GRP,\ncast('' as string) as ID_MN_NON_MIL_USE,\ncast('' as string) as ID_MN_MIL_USE,\ncast('' as string) as CUST_COND_GRP_1,\ncast('' as string) as CUST_COND_GRP_2,\ncast('' as string) as CUST_COND_GRP_3,\ncast('' as string) as CUST_COND_GRP_4,\ncast('' as string) as CUST_COND_GRP_5,\ncast('' as string) as IN_ALT_PYR_USE_ACCT_NUM,\ncast('' as string) as TAX_NUM_TYPE,\ncast('' as string) as TAX_NUM_6,\ncast('' as string) as IN_BIOCH_WARF_LEGAL_CNTL,\ncast('' as string) as IN_NUCLR_NONPROLIF_LEGAL_CNTL,\ncast('' as string) as IN_NATL_SCTY_LEGAL_CNTL,\ncast('' as string) as IN_MISSILE_TECH_LEGAL_CNTL,\ncast('' as string) as TW_BOND_AREA_CNFRM_BUSN_PTNR_EXTN,\ncast('' as string) as TW_DNT_MK_BUSN_PTNR_EXTN,\ncast('' as string) as CNSD_INVC_TW,\ncast('' as string) as TW_ALLW_TYPE_BUSN_PTNR_EXTN,\ncast('' as string) as TW_CH_MODE_BUSN_PTNR_EXTN,\ncast('' as string) as UNIFM_RSRS_LCTR,\ncast('' as string) as NM_REP,\ncast('' as string) as TYPE_BUSN,\ncast('' as string) as TYPE_INDSTR,\ncast('' as string) as STS_CHG_AUTH,\ncast('' as timestamp) as LAST_CHG_CNFRM_DTTM,\ncast('' as string) as CENT_DEL_BLK_MSTR_REC,\ncast('' as string) as DELV_DT_RULE,\ncast('' as string) as BUSN_PRPS_CMPLT_FL,\ncast('' as string) as SUFRAMA_CD,\ncast('' as string) as RG_NUM,\ncast('' as string) as ISS_BY,\ncast('' as string) as ST,\ncast('' as timestamp) as RG_ISU_DTTM,\ncast('' as string) as RIC_NUM,\ncast('' as string) as FRGN_NATL_REGS,\ncast('' as timestamp) as RNE_ISU_DTTM,\ncast('' as string) as CNAE,\ncast('' as string) as LEGAL_NATR,\ncast('' as string) as CRT_NUM,\ncast('' as string) as ICMS_TAXPY,\ncast('' as string) as INDSTR_MN_TYPE,\ncast('' as string) as TAX_DCLTN_TYPE,\ncast('' as string) as CO_SIZE,\ncast('' as string) as DCLTN_RGMN_PIS_COFINS,\ncast('' as string) as PH_BUSN_STYL_TEXT,\ncast('' as string) as PMT_RSN,\ncast('' as string) as DATA_ELMNT_FOR_CUST,\ncast('' as string) as ACCT_EXCLU_FROM_RULE_BAS_ASGNMT,\ncast('' as string) as CUST_GENL_ADDR_DEP_EXTN,\ncast('' as decimal(18,4)) as MAX_STCK_HGHT_PKGNG_MATL,\ncast('' as string) as UNIT_LGTH_PKGNG_MATL,\ncast('' as string) as CUST_RLTD_PACK_EA_PKGNG_MATL,\ncast('' as string) as PKGNG_MATL_CUST_VSO,\ncast('' as string) as NUM_LYR_UND_INTER_PLLT,\ncast('' as string) as PACK_MATL_SPEC_EA_PKGNG_MATL,\ncast('' as string) as PACK_ONLY_ONE_PKG_TYPE_EA_PKM,\ncast('' as string) as SIDE_PREF_LD_UNLD,\ncast('' as string) as FRN_BK_PREF_LD_UNLD,\ncast('' as string) as COLL_UNLD_PT_VSO,\ncast('' as string) as AGN_LOC_CD,\ncast('' as string) as PMT_OFF,\ncast('' as string) as FEE_SCHED,\ncast('' as string) as DUNS_NUM,\ncast('' as string) as DUNS_4,\ncast('' as string) as SYS_AWRD_MGMT_UNIQ_ENTITY_ID,\ncast('' as string) as SYS_AWRD_MGMT_ELCTRNC_FUND_TFR_IND,\ncast('' as string) as PROC_GRP,\ncast('' as string) as SUBLDGR_ACCT_PRPRC_PCDR,\ncast('' as string) as NM_1,\ncast('' as string) as NM_2,\ncast('' as string) as NM_3,\ncast('' as string) as FST_NM,\ncast('' as string) as TITLE_PSOTL,\ncast('' as string) as HS_NUM_NO_LONG_USED_FROM_RLSE,\ncast('' as string) as STREET_NO_LONG_USED_FROM_RLSE,\ncast('' as string) as DESC_PSOO1,\ncast('' as string) as DESC_PSOO2,\ncast('' as string) as DESC_PSOO3,\ncast('' as string) as DESC_PSOO4,\ncast('' as string) as DESC_PSOO5,\ncast('' as string) as ECC_NUM,\ncast('' as string) as EXCSE_REGS_NUM,\ncast('' as string) as EXCSE_RNG,\ncast('' as string) as EXCSE_DIV,\ncast('' as string) as EXCSE_COMMSN,\ncast('' as string) as CENT_SLS_TAX_NUM,\ncast('' as string) as LCL_SLS_TAX_NUM,\ncast('' as string) as PERM_ACCT_NUM,\ncast('' as string) as EXCSE_TAX_IN_FOR_CUST,\ncast('' as timestamp) as OBSOL_LAST_CHG_ON_DTTM,\ncast('' as string) as OBSOL_CHG_BY_USER,\ncast('' as string) as SRVC_TAX_REGS_NUM,\ncast('' as string) as PAN_REF_NUM,\ncast('' as string) as GST_TDS_REGS,\ncast('' as string) as RCPNT_TYPE,\ncast('' as string) as REF_TYPE_RCPNT,\ncast('' as string) as WBS_ELMNT,\ncast('' as string) as ORDR_NUM,\ncast('' as string) as EXTRNL_SOLD_TO_PRTY,\ncast('' as string) as CUST_INTRNL_SETLM,\ncast('' as string) as DUMMY_RCPNT,\ncast('' as string) as STD_RCPNT,\ncast('' as string) as STRG_LOC,\ncast('' as string) as CNTL_AREA,\ncast('' as string) as COST_CTR,\ncast('' as string) as RET_DT_CNTS_PERF_BAS_EQMNT,\ncast('' as string) as RET_DT_CNTS_TIME_BAS_EQMNT,\ncast('' as string) as SETLM_TYPE,\ncast('' as decimal(18,4)) as HRS_PER_MO,\ncast('' as decimal(18,4)) as HRS_PER_DAY,\ncast('' as decimal(18,4)) as NUM_OF_DAYS_MO,\ncast('' as string) as FUNC_SETL_ACQ_DT_PBE,\ncast('' as string) as FILL_PBE_DOC_IN,\ncast('' as string) as IN_TAKE_MLT_USG_PER_INTO_ACCT,\ncast('' as string) as IN_RLVNT_SHRT_OPR_PER,\ncast('' as string) as BLOK_IN_DOC_ENT,\ncast('' as string) as IN_CALC_PBE_TBE,\ncast('' as string) as CAL_ID,\ncast('' as string) as IN_CMPLT_MO,\ncast('' as string) as SETLM_IN,\ncast('' as timestamp) as LAST_SETLM_DTTM,\ncast('' as timestamp) as CUR_SETLM_DTTM,\ncast('' as string) as IN_ACQ_DT_CNTS,\ncast('' as string) as NGTV_QTY_ALLW_EVE,\ncast('' as string) as IN_DEAD_LOGIC,\ncast('' as string) as MIN_USG_PER_RSTK_USG,\ncast('' as string) as RLSE_NTF_ALLW_UNRSTK_USG,\ncast('' as string) as SURC_FOR_UNRSTK_USG,\ncast('' as string) as IN_SHFT_PRC,\ncast('' as string) as STS_CNSTR_SITE_INV,\ncast('' as string) as PLNG_AREA,\ncast('' as string) as IN_SETLM_QTY_RLVNT_EQMNT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
