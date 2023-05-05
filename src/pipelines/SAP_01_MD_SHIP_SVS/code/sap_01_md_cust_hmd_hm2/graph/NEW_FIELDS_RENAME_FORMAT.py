from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_hmd_hm2.config.ConfigStore import *
from sap_01_md_cust_hmd_hm2.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(""))\
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
        .withColumn("TAX_NUM5", trim(col("STCD5")))\
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
        .withColumn("ANNL_SLS_UMSAT", trim(col("UMSAT")).cast(DecimalType(18, 4)))\
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
        .withColumn("ANNL_SLS_UMSA1", trim(col("UMSA1")).cast(DecimalType(18, 4)))\
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
        .withColumn("TAX_NUM_6", trim(col("STCD6")))\
        .withColumn("IN_BIOCH_WARF_LEGAL_CNTL", trim(col("CCC01")))\
        .withColumn("IN_NUCLR_NONPROLIF_LEGAL_CNTL", trim(col("CCC02")))\
        .withColumn("IN_NATL_SCTY_LEGAL_CNTL", trim(col("CCC03")))\
        .withColumn("IN_MISSILE_TECH_LEGAL_CNTL", trim(col("CCC04")))\
        .withColumn("TW_BOND_AREA_CNFRM_BUSN_PTNR_EXTN", trim(col("BONDED_AREA_CONFIRM")))\
        .withColumn("TW_DNT_MK_BUSN_PTNR_EXTN", trim(col("DONATE_MARK")))\
        .withColumn("CNSD_INVC_TW", trim(col("CONSOLIDATE_INVOICE")))\
        .withColumn("TW_ALLW_TYPE_BUSN_PTNR_EXTN", trim(col("ALLOWANCE_TYPE")))\
        .withColumn("TW_CH_MODE_BUSN_PTNR_EXTN", trim(col("EINVOICE_MODE")))\
        .withColumn("UNIFM_RSRS_LCTR", trim(col("KNURL")))\
        .withColumn("NM_REP", trim(col("J_1KFREPRE")))\
        .withColumn("TYPE_BUSN", trim(col("J_1KFTBUS")))\
        .withColumn("TYPE_INDSTR", trim(col("J_1KFTIND")))\
        .withColumn("STS_CHG_AUTH", trim(col("CONFS")))\
        .withColumn(
          "LAST_CHG_CNFRM_DTTM",
          when((col("UPTIM") == lit("000000")), lit(None)).otherwise(to_timestamp(col("UPTIM"), "HHmmSS"))
        )\
        .withColumn("CENT_DEL_BLK_MSTR_REC\r\n", trim(col("NODEL")))\
        .withColumn("DELV_DT_RULE", trim(col("DELIVERY_DATE_RULE")))\
        .withColumn("BUSN_PRPS_CMPLT_FL", trim(col("CVP_XBLCK")))\
        .withColumn("SUFRAMA_CD", trim(col("SUFRAMA")))\
        .withColumn("RG_NUM", trim(col("RG")))\
        .withColumn("ISS_BY", trim(col("EXP")))\
        .withColumn("ST", trim(col("UF")))\
        .withColumn(
          "RG_ISU_DTTM",
          when((col("RGDATE") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("RGDATE"), "yyyyMMdd"))
        )\
        .withColumn("RIC_NUM", trim(col("RIC")))\
        .withColumn("FRGN_NATL_REGS", trim(col("RNE")))\
        .withColumn(
          "RNE_ISU_DTTM",
          when((col("RNEDATE") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("RNEDATE"), "yyyyMMdd"))
        )\
        .withColumn("CNAE", trim(col("CNAE")))\
        .withColumn("LEGAL_NATR", trim(col("LEGALNAT")))\
        .withColumn("CRT_NUM", trim(col("CRTN")))\
        .withColumn("ICMS_TAXPY", trim(col("ICMSTAXPAY")))\
        .withColumn("INDSTR_MN_TYPE", trim(col("INDTYP")))\
        .withColumn("TAX_DCLTN_TYPE", trim(col("TDT")))\
        .withColumn("CO_SIZE", trim(col("COMSIZE")))\
        .withColumn("DCLTN_RGMN_PIS_COFINS", trim(col("DECREGPC")))\
        .withColumn("PH_BUSN_STYL_TEXT", trim(col("PH_BIZ_STYLE")))\
        .withColumn("PMT_RSN", trim(col("PAYTRSN")))\
        .withColumn("DATA_ELMNT_FOR_CUST", trim(col("KNA1_EEW_CUST")))\
        .withColumn("ACCT_EXCLU_FROM_RULE_BAS_ASGNMT", trim(col("RULE_EXCLUSION")))\
        .withColumn("CUST_GENL_ADDR_DEP_EXTN", trim(col("KNA1_ADDR_EEW_CUST")))\
        .withColumn("MAX_STCK_HGHT_PKGNG_MATL", trim(col("_VSO_R_PALHGT")))\
        .withColumn("UNIT_LGTH_PKGNG_MATL", trim(col("_VSO_R_PAL_UL")))\
        .withColumn("CUST_RLTD_PACK_EA_PKGNG_MATL", trim(col("_VSO_R_PK_MAT")))\
        .withColumn("PKGNG_MATL_CUST_VSO", trim(col("_VSO_R_MATPAL")))\
        .withColumn("NUM_LYR_UND_INTER_PLLT", trim(col("_VSO_R_I_NO_LYR")))\
        .withColumn("PACK_MATL_SPEC_EA_PKGNG_MATL", trim(col("_VSO_R_ONE_MAT")))\
        .withColumn("PACK_ONLY_ONE_PKG_TYPE_EA_PKM", trim(col("_VSO_R_ONE_SORT")))\
        .withColumn("SIDE_PREF_LD_UNLD", trim(col("_VSO_R_ULD_SIDE")))\
        .withColumn("FRN_BK_PREF_LD_UNLD", trim(col("_VSO_R_LOAD_PREF")))\
        .withColumn("COLL_UNLD_PT_VSO", trim(col("_VSO_R_DPOINT")))\
        .withColumn("AGN_LOC_CD", trim(col("ALC")))\
        .withColumn("PMT_OFF", trim(col("PMT_OFFICE")))\
        .withColumn("FEE_SCHED", trim(col("FEE_SCHEDULE")))\
        .withColumn("DUNS_NUM", trim(col("DUNS")))\
        .withColumn("DUNS_4", trim(col("DUNS4")))\
        .withColumn("SYS_AWRD_MGMT_UNIQ_ENTITY_ID", trim(col("SAM_UE_ID")))\
        .withColumn("SYS_AWRD_MGMT_ELCTRNC_FUND_TFR_IND", trim(col("SAM_EFT_IND")))\
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
        .withColumn("ECC_NUM", trim(col("J_1IEXCD")))\
        .withColumn("EXCSE_REGS_NUM", trim(col("J_1IEXRN")))\
        .withColumn("EXCSE_RNG", trim(col("J_1IEXRG")))\
        .withColumn("EXCSE_DIV", trim(col("J_1IEXDI")))\
        .withColumn("EXCSE_COMMSN", trim(col("J_1IEXCO")))\
        .withColumn("CENT_SLS_TAX_NUM", trim(col("J_1ICSTNO")))\
        .withColumn("LCL_SLS_TAX_NUM", trim(col("J_1ILSTNO")))\
        .withColumn("PERM_ACCT_NUM", trim(col("J_1IPANNO")))\
        .withColumn("EXCSE_TAX_IN_FOR_CUST", trim(col("J_1IEXCICU")))\
        .withColumn(
          "OBSOL_LAST_CHG_ON_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn("OBSOL_CHG_BY_USER", trim(col("USNAM")))\
        .withColumn("SRVC_TAX_REGS_NUM", trim(col("J_1ISERN")))\
        .withColumn("PAN_REF_NUM", trim(col("J_1IPANREF")))\
        .withColumn("GST_TDS_REGS", trim(col("GST_TDS")))\
        .withColumn("RCPNT_TYPE", trim(col("J_3GETYP")))\
        .withColumn("REF_TYPE_RCPNT", trim(col("J_3GREFTYP")))\
        .withColumn("WBS_ELMNT", trim(col("PSPNR")))\
        .withColumn("ORDR_NUM", trim(col("COAUFNR")))\
        .withColumn("EXTRNL_SOLD_TO_PRTY", trim(col("J_3GAGEXT")))\
        .withColumn("CUST_INTRNL_SETLM", trim(col("J_3GAGINT")))\
        .withColumn("DUMMY_RCPNT", trim(col("J_3GAGDUMI")))\
        .withColumn("STD_RCPNT", trim(col("J_3GAGSTDI")))\
        .withColumn("STRG_LOC", trim(col("LGORT")))\
        .withColumn("CNTL_AREA", trim(col("KOKRS")))\
        .withColumn("COST_CTR", trim(col("KOSTL")))\
        .withColumn("RET_DT_CNTS_PERF_BAS_EQMNT", trim(col("J_3GABGLG")))\
        .withColumn("RET_DT_CNTS_TIME_BAS_EQMNT", trim(col("J_3GABGVG")))\
        .withColumn("SETLM_TYPE", trim(col("J_3GABRART")))\
        .withColumn("HRS_PER_MO", trim(col("J_3GSTDMON")))\
        .withColumn("HRS_PER_DAY", trim(col("J_3GSTDTAG")))\
        .withColumn("NUM_OF_DAYS_MO", trim(col("J_3GTAGMON")))\
        .withColumn("FUNC_SETL_ACQ_DT_PBE", trim(col("J_3GZUGTAG")))\
        .withColumn("FILL_PBE_DOC_IN\r\n", trim(col("J_3GMASCHB")))\
        .withColumn("IN_TAKE_MLT_USG_PER_INTO_ACCT", trim(col("J_3GMEINSA")))\
        .withColumn("IN_RLVNT_SHRT_OPR_PER", trim(col("J_3GKEINSA")))\
        .withColumn("BLOK_IN_DOC_ENT", trim(col("J_3GBLSPER")))\
        .withColumn("IN_CALC_PBE_TBE", trim(col("J_3GKLEIVO")))\
        .withColumn("CAL_ID", trim(col("J_3GCALID")))\
        .withColumn("IN_CMPLT_MO", trim(col("J_3GVMONAT")))\
        .withColumn("SETLM_IN", trim(col("J_3GABRKEN")))\
        .withColumn(
          "LAST_SETLM_DTTM",
          when((col("J_3GAABRECH") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(col("J_3GAABRECH"), "yyyyMMdd"))
        )\
        .withColumn(
          "CUR_SETLM_DTTM",
          when((col("J_3GAABRECH") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(col("J_3GAABRECH"), "yyyyMMdd"))
        )\
        .withColumn("IN_ACQ_DT_CNTS", trim(col("J_3GZUTVHLG")))\
        .withColumn("NGTV_QTY_ALLW_EVE", trim(col("J_3GNEGMEN")))\
        .withColumn("IN_DEAD_LOGIC", trim(col("J_3GFRISTLO")))\
        .withColumn("MIN_USG_PER_RSTK_USG", trim(col("J_3GEMINBE")))\
        .withColumn("RLSE_NTF_ALLW_UNRSTK_USG", trim(col("J_3GFMGUE")))\
        .withColumn("SURC_FOR_UNRSTK_USG", trim(col("J_3GZUSCHUE")))\
        .withColumn("IN_SHFT_PRC", trim(col("J_3GSCHPRS")))\
        .withColumn("STS_CNSTR_SITE_INV", trim(col("J_3GINVSTA")))\
        .withColumn("PLNG_AREA", trim(col("_SAPCEM_DBER")))\
        .withColumn("IN_SETLM_QTY_RLVNT_EQMNT", trim(col("_SAPCEM_KVMEQ")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CUST_NUM', CUST_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
