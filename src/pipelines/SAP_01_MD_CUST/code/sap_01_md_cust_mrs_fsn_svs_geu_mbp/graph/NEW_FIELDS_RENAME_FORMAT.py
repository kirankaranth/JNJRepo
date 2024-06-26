from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.config.ConfigStore import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CUST_NUM", col("KUNNR"))\
        .withColumn("BLOCK_TYPE_CD", trim(col("AUFSD")))\
        .withColumn("GLN1_NBR", trim(col("BBBNR")))\
        .withColumn("GLN2_NBR", trim(col("BBSNR")))\
        .withColumn("GLOBL_COT_CD", trim(col("BRAN1")))\
        .withColumn("COT_CD", trim(col("BRAN2")))\
        .withColumn("COT3_CD", trim(col("BRAN3")))\
        .withColumn("COT4_CD", trim(col("BRAN4")))\
        .withColumn("COT5_CD", trim(col("BRAN5")))\
        .withColumn("SGMNT_CD", trim(col("BRSCH")))\
        .withColumn("GLN3_NBR", trim(col("BUBKZ")))\
        .withColumn("SLS_ORDR_BLK_CD", trim(col("CASSD")))\
        .withColumn("CFOP_CAT_CD", trim(col("CFOPC")))\
        .withColumn("CMPT_IND", trim(col("DEAR1")))\
        .withColumn("CNSMR_IND", trim(col("DEAR6")))\
        .withColumn(
          "CRT_ON_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("SLS_INVC_BLK_CD", trim(col("FAKSD")))\
        .withColumn("TAX_TYPE_CD", trim(col("FITYP")))\
        .withColumn("CUST_TYPE_CD", trim(col("KTOKD")))\
        .withColumn("CUST_NM1", trim(col("NAME1")))\
        .withColumn("CUST_NM2", trim(col("NAME2")))\
        .withColumn("CUST_NM3", trim(col("NAME3")))\
        .withColumn("CUST_NM4", trim(col("NAME4")))\
        .withColumn("NLSN_CD", trim(col("NIELS")))\
        .withColumn("REGN_MKT_CD", trim(col("RPMKR")))\
        .withColumn("CUST_SHRT_NM", trim(col("SORTL")))\
        .withColumn("LANG_CD", trim(col("SPRAS")))\
        .withColumn("TAX_NUM1", trim(col("STCD1")))\
        .withColumn("TAX_NUM2", trim(col("STCD2")))\
        .withColumn("TAX_NUM3", trim(col("STCD3")))\
        .withColumn("TAX_NUM4", trim(col("STCD4")))\
        .withColumn("TAX_NUM5", expr(Config.TAX_NUM5))\
        .withColumn("VAT_NUM", trim(col("STCEG")))\
        .withColumn("NTRL_PRSN_IND", trim(col("STKZN")))\
        .withColumn("ICMS_TAX_CD", trim(col("TXLW1")))\
        .withColumn("IPI_TAX_CD", trim(col("TXLW2")))\
        .withColumn(
          "CHG_ON_DTTM",
          when((col("UPDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("UPDAT"), "yyyyMMdd"))
        )\
        .withColumn("TRAD_PTNR_CO_CD", trim(col("VBUND")))\
        .withColumn("ICMS_EXPT_IND", trim(col("XICMS")))\
        .withColumn("SUBST_TAX_GRP_CD", trim(col("XSUBT")))\
        .withColumn("IPI_EXPT_IND", trim(col("XXIPI")))\
        .withColumn("ADDR_NUM", trim(col("ADRNR")))\
        .withColumn("STREET_HS_NUM", trim(col("STRAS")))\
        .withColumn("CTRY_RGN_KEY", trim(col("LAND1")))\
        .withColumn("CITY", trim(col("ORT01")))\
        .withColumn("PSTL_CD", trim(col("PSTLZ")))\
        .withColumn("RGN", trim(col("REGIO")))\
        .withColumn("TRSPN_ZN", trim(col("LZONE")))\
        .withColumn("SLS_DOC_BLOK_RSN_TEXT", lookup("LU_SAP_TVAST", col("AUFSD")).getField("VTEXT"))\
        .withColumn("DESC_INDSTR_KEY", lookup("LU_SAP_T016T", col("BRSCH")).getField("BRTXT"))\
        .withColumn("FST_PHN_NUM", trim(col("TELF1")))\
        .withColumn("FAX_NUM", trim(col("TELFX")))\
        .withColumn("IN_ACCT_ONE_TIME_ACCT", trim(col("XCPDK")))\
        .withColumn("SRCH_TERM_MATCH_CD_SRCH_1", trim(col("MCOD1")))\
        .withColumn("SRCH_TERM_MATCH_CD_SRCH_2", trim(col("MCOD2")))\
        .withColumn("SRCH_TERM_MATCH_CD_SRCH_3", trim(col("MCOD3")))\
        .withColumn("TITLE_ANRED", trim(col("ANRED")))\
        .withColumn("EXP_TRAIN_STN", trim(col("BAHNE")))\
        .withColumn("TRAIN_STN", trim(col("BAHNS")))\
        .withColumn("AUTH_GRP", trim(col("BEGRU")))\
        .withColumn("DATA_COMM_LINE_NUM", trim(col("DATLT")))\
        .withColumn("NM_PRSN_CRT_OBJ", trim(col("ERNAM")))\
        .withColumn("IN_UNLD_PT_EXIST", trim(col("EXABL")))\
        .withColumn("ACCT_NUM_MSTR_REC_FISC_ADDR", trim(col("FISKN")))\
        .withColumn("WRK_TIME_CAL", trim(col("KNAZK")))\
        .withColumn("ACCT_NUM_ALT_PYR", trim(col("KNRZA")))\
        .withColumn("GRP_KEY", trim(col("KONZS")))\
        .withColumn("CUST_CLSN", trim(col("KUKLA")))\
        .withColumn("ACCT_NUM_SUP", trim(col("LIFNR")))\
        .withColumn("CENT_DELV_BLK_CUST", trim(col("LIFSD")))\
        .withColumn("CITY_COORDNT", trim(col("LOCCO")))\
        .withColumn("CENT_DEL_FL_MSTR_REC", trim(col("LOEVM")))\
        .withColumn("DSTRC", trim(col("ORT02")))\
        .withColumn("PO_BOX", trim(col("PFACH")))\
        .withColumn("PO_BOX_PSTL_CD", trim(col("PSTL2")))\
        .withColumn("CNTY_CD", trim(col("COUNC")))\
        .withColumn("CITY_CD", trim(col("CITYC")))\
        .withColumn("CENT_PSTNG_BLK", trim(col("SPERR")))\
        .withColumn("IN_BUSN_PTNR_SUBJ_EQLZTN_TAX", trim(col("STKZA")))\
        .withColumn("LIAB_VAT", trim(col("STKZU")))\
        .withColumn("TELEBOX_NUM", trim(col("TELBX")))\
        .withColumn("SEC_PHN_NUM", trim(col("TELF2")))\
        .withColumn("TELETEX_NUM", trim(col("TELTX")))\
        .withColumn("TELEX_NUM", trim(col("TELX1")))\
        .withColumn("IN_ALT_PYR_ALLW_DOC", trim(col("XZEMP")))\
        .withColumn("IN_SLS_PTNR", trim(col("DEAR2")))\
        .withColumn("IN_SLS_PROSP", trim(col("DEAR3")))\
        .withColumn("IN_CUST_TYPE_4", trim(col("DEAR4")))\
        .withColumn("ID_DFLT_SOLD_TO_PRTY", trim(col("DEAR5")))\
        .withColumn("LEGAL_STS", trim(col("GFORM")))\
        .withColumn("INIT_CNTCT", trim(col("EKONT")))\
        .withColumn("ANNL_SLS_UMSAT", col("UMSAT").cast(DecimalType(18, 4)))\
        .withColumn("YR_SLS_GVN", trim(col("UMJAH")))\
        .withColumn("CRNCY_SLS_FIG", trim(col("UWAER")))\
        .withColumn("YR_NUM_EMP", trim(col("JMZAH")))\
        .withColumn("YR_NUM_EMP_GVN", trim(col("JMJAH")))\
        .withColumn("ATTR_1", trim(col("KATR1")))\
        .withColumn("ATTR_2", trim(col("KATR2")))\
        .withColumn("ATTR_3", trim(col("KATR3")))\
        .withColumn("ATTR_4", trim(col("KATR4")))\
        .withColumn("ATTR_5", trim(col("KATR5")))\
        .withColumn("ATTR_6", trim(col("KATR6")))\
        .withColumn("ATTR_7", trim(col("KATR7")))\
        .withColumn("ATTR_8", trim(col("KATR8")))\
        .withColumn("ATTR_9", trim(col("KATR9")))\
        .withColumn("ATTR_10", trim(col("KATR10")))\
        .withColumn("ANNL_SLS_UMSA1", col("UMSA1").cast(DecimalType(18, 4)))\
        .withColumn("TAX_JURIS", trim(col("TXJCD")))\
        .withColumn("FISC_YR_VRNT", trim(col("PERIV")))\
        .withColumn("USG_IN", trim(col("ABRVW")))\
        .withColumn("INSP_CARR_OUT_BY_CUST", trim(col("INSPBYDEBI")))\
        .withColumn("INSP_DELV_NOTE_AFTER_OUTB_DELV", trim(col("INSPATDEBI")))\
        .withColumn("REF_ACCT_GRP_ONE_TIME_ACCT", trim(col("KTOCD")))\
        .withColumn("PO_BOX_CITY", trim(col("PFORT")))\
        .withColumn("PLNT", trim(col("WERKS")))\
        .withColumn("RPT_KEY_DATA_MED_EXCH", trim(col("DTAMS")))\
        .withColumn("INSTR_KEY_DATA_MED_EXCH", trim(col("DTAWS")))\
        .withColumn("STS_DATA_TFR_INTO_SUBSQ_RLSE", trim(col("DUEFL")))\
        .withColumn("ASGNMT_TO_HIER", trim(col("HZUOR")))\
        .withColumn("PMT_BLK", trim(col("SPERZ")))\
        .withColumn("IS_R_LABLNG_CUST_PLNT_GRP", trim(col("ETIKG")))\
        .withColumn("ID_MN_NON_MIL_USE", trim(col("CIVVE")))\
        .withColumn("ID_MN_MIL_USE", trim(col("MILVE")))\
        .withColumn("CUST_COND_GRP_1", trim(col("KDKG1")))\
        .withColumn("CUST_COND_GRP_2", trim(col("KDKG2")))\
        .withColumn("CUST_COND_GRP_3", trim(col("KDKG3")))\
        .withColumn("CUST_COND_GRP_4", trim(col("KDKG4")))\
        .withColumn("CUST_COND_GRP_5", trim(col("KDKG5")))\
        .withColumn("IN_ALT_PYR_USE_ACCT_NUM", trim(col("XKNZA")))\
        .withColumn("TAX_NUM_TYPE", trim(col("STCDT")))\
        .withColumn("TAX_NUM_6", expr(Config.TAX_NUM_6))\
        .withColumn("IN_BIOCH_WARF_LEGAL_CNTL", trim(col("CCC01")))\
        .withColumn("IN_NUCLR_NONPROLIF_LEGAL_CNTL", trim(col("CCC02")))\
        .withColumn("IN_NATL_SCTY_LEGAL_CNTL", trim(col("CCC03")))\
        .withColumn("IN_MISSILE_TECH_LEGAL_CNTL", trim(col("CCC04")))\
        .withColumn("UNIFM_RSRS_LCTR", trim(col("KNURL")))\
        .withColumn("NM_REP", trim(col("J_1KFREPRE")))\
        .withColumn("TYPE_BUSN", trim(col("J_1KFTBUS")))\
        .withColumn("TYPE_INDSTR", trim(col("J_1KFTIND")))\
        .withColumn("STS_CHG_AUTH", trim(col("CONFS")))\
        .withColumn(
          "LAST_CHG_CNFRM_DTTM",
          when((col("UPTIM") == lit("000000")), lit(None)).otherwise(to_timestamp(col("UPTIM"), "HHmmSS"))
        )\
        .withColumn("CENT_DEL_BLK_MSTR_REC", trim(col("NODEL")))\
        .withColumn("BUSN_PRPS_CMPLT_FL", expr(Config.BUSN_PRPS_CMPLT_FL))\
        .withColumn("SUFRAMA_CD", expr(Config.SUFRAMA_CD))\
        .withColumn("RG_NUM", expr(Config.RG_NUM))\
        .withColumn("ISS_BY", expr(Config.ISS_BY))\
        .withColumn("ST", expr(Config.ST))\
        .withColumn("RG_ISU_DTTM", expr(Config.RG_ISU_DTTM))\
        .withColumn("RIC_NUM", expr(Config.RIC_NUM))\
        .withColumn("FRGN_NATL_REGS", expr(Config.FRGN_NATL_REGS))\
        .withColumn("RNE_ISU_DTTM", expr(Config.RNE_ISU_DTTM))\
        .withColumn("CNAE", expr(Config.CNAE))\
        .withColumn("LEGAL_NATR", expr(Config.LEGAL_NATR))\
        .withColumn("CRT_NUM", expr(Config.CRT_NUM))\
        .withColumn("ICMS_TAXPY", expr(Config.ICMS_TAXPY))\
        .withColumn("INDSTR_MN_TYPE", expr(Config.INDSTR_MN_TYPE))\
        .withColumn("TAX_DCLTN_TYPE", expr(Config.TAX_DCLTN_TYPE))\
        .withColumn("CO_SIZE", expr(Config.CO_SIZE))\
        .withColumn("DCLTN_RGMN_PIS_COFINS", expr(Config.DCLTN_RGMN_PIS_COFINS))\
        .withColumn("MAX_STCK_HGHT_PKGNG_MATL", expr(Config.MAX_STCK_HGHT_PKGNG_MATL))\
        .withColumn("UNIT_LGTH_PKGNG_MATL", expr(Config.UNIT_LGTH_PKGNG_MATL))\
        .withColumn("CUST_RLTD_PACK_EA_PKGNG_MATL", expr(Config.CUST_RLTD_PACK_EA_PKGNG_MATL))\
        .withColumn("PKGNG_MATL_CUST_VSO", expr(Config.PKGNG_MATL_CUST_VSO))\
        .withColumn("NUM_LYR_UND_INTER_PLLT", expr(Config.NUM_LYR_UND_INTER_PLLT))\
        .withColumn("PACK_MATL_SPEC_EA_PKGNG_MATL", expr(Config.PACK_MATL_SPEC_EA_PKGNG_MATL))\
        .withColumn("PACK_ONLY_ONE_PKG_TYPE_EA_PKM", expr(Config.PACK_ONLY_ONE_PKG_TYPE_EA_PKM))\
        .withColumn("SIDE_PREF_LD_UNLD", expr(Config.SIDE_PREF_LD_UNLD))\
        .withColumn("FRN_BK_PREF_LD_UNLD", expr(Config.COLL_UNLD_PT_VSO))\
        .withColumn("COLL_UNLD_PT_VSO", expr(Config.COLL_UNLD_PT_VSO))\
        .withColumn("AGN_LOC_CD", trim(col("ALC")))\
        .withColumn("PMT_OFF", trim(col("PMT_OFFICE")))\
        .withColumn("FEE_SCHED", expr(Config.FEE_SCHED))\
        .withColumn("DUNS_NUM", expr(Config.DUNS_NUM))\
        .withColumn("DUNS_4", expr(Config.DUNS_4))\
        .withColumn("PROC_GRP", trim(col("PSOFG")))\
        .withColumn("SUBLDGR_ACCT_PRPRC_PCDR", trim(col("PSOIS")))\
        .withColumn("NM_1", trim(col("PSON1")))\
        .withColumn("NM_2", trim(col("PSON2")))\
        .withColumn("NM_3", trim(col("PSON3")))\
        .withColumn("FST_NM", trim(col("PSOVN")))\
        .withColumn("TITLE_PSOTL", trim(col("PSOTL")))\
        .withColumn("HS_NUM_NO_LONG_USED_FROM_RLSE", trim(col("PSOHS")))\
        .withColumn("STREET_NO_LONG_USED_FROM_RLSE", trim(col("PSOST")))\
        .withColumn("DESC_PSOO1", trim(col("PSOO1")))\
        .withColumn("DESC_PSOO2", trim(col("PSOO2")))\
        .withColumn("DESC_PSOO3", trim(col("PSOO3")))\
        .withColumn("DESC_PSOO4", trim(col("PSOO4")))\
        .withColumn("DESC_PSOO5", trim(col("PSOO5")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
