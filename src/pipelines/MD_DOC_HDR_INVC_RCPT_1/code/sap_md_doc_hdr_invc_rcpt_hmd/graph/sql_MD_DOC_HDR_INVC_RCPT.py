from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_doc_hdr_invc_rcpt_hmd.config.ConfigStore import *
from sap_md_doc_hdr_invc_rcpt_hmd.udfs.UDFs import *

def sql_MD_DOC_HDR_INVC_RCPT(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
TRIM(RBKP.BELNR) AS DOC_NUM_OF_INVC_DOC,
TRIM(RBKP.GJAHR) AS FISC_YR,
TRIM(RBKP.BLART) AS DOC_TYPE,
CASE WHEN RBKP.BLDAT = '00000000' THEN NULL ELSE to_timestamp(RBKP.BLDAT,\"yyyyMMdd\") END AS IN_DOC_DTTM,
CASE WHEN RBKP.BUDAT = '00000000' THEN NULL ELSE to_timestamp(RBKP.BUDAT,\"yyyyMMdd\") END AS PSTNG_IN_DOC_DTTM,
TRIM(RBKP.USNAM) AS USER_NM,
TRIM(RBKP.TCODE) AS TRX_CD,
CASE WHEN RBKP.CPUDT = '00000000000000' THEN NULL ELSE to_timestamp(CONCAT(RBKP.CPUDT,RBKP.CPUTM),'yyyyMMddHHmmss') END AS ACTG_DOC_ENT_DTTM,
TRIM(RBKP.VGART) AS TRX_TYPE_IN_AG08_DOC,
TRIM(RBKP.XBLNR) AS REF_DOC_NUM,
TRIM(RBKP.BUKRS) AS CO_CD,
TRIM(RBKP.LIFNR) AS DIFF_INVC_PRTY,
TRIM(RBKP.WAERS) AS CRNCY_KEY,
CAST(RBKP.KURSF AS DECIMAL(18, 4)) AS EXCH_RT,
CAST(RBKP.RMWWR AS DECIMAL(18, 4)) AS GRS_INVC_AMT_DOC_CRNCY,
CAST(RBKP.BEZNK AS DECIMAL(18, 4)) AS NOT_PLAN_DELV_COST,
CAST(RBKP.WMWST1 AS DECIMAL(18, 4)) AS TAX_AMT_WTH_SIGN,
TRIM(RBKP.MWSKZ1) AS TAX_CD,
CAST(RBKP.WMWST2 AS DECIMAL(18, 4)) AS OBSOL_TAX_AMT_DO_NOT_USE,
TRIM(RBKP.MWSKZ2) AS OBSOL_SLS_TAX_CD,
TRIM(RBKP.ZTERM) AS TERM_OF_PMT_KEY,
TRIM(RBKP.ZBD1T) AS CASH_DISC_DAYS_1,
CAST(RBKP.ZBD1P AS DECIMAL(18, 4)) AS CASH_DISC_PCT_1,
TRIM(RBKP.ZBD2T) AS CASH_DISC_DAYS_2,
CAST(RBKP.ZBD2P AS DECIMAL(18, 4)) AS CASH_DISC_PCT_2,
TRIM(RBKP.ZBD3T) AS NET_PMT_TERM_PER,
CAST(RBKP.WSKTO AS DECIMAL(18, 4)) AS CASH_DISC_AMT_DOC_CRNCY,
TRIM(RBKP.XRECH) AS IN_POST_INVC,
TRIM(RBKP.BKTXT) AS DOC_HDR_TEXT,
TRIM(RBKP.SAPRL) AS SAP_RLSE,
TRIM(RBKP.LOGSYS) AS LOGL_SYS,
TRIM(RBKP.XMWST) AS CALC_TAX_AUTMT,
TRIM(RBKP.STBLG) AS RVSL_DOC_NUM,
TRIM(RBKP.STJAH) AS FISC_YR_OF_RVSL_DOC,
TRIM(RBKP.MWSKZ_BNK) AS TAX_CD_BANK,
TRIM(RBKP.TXJCD_BNK) AS TAX_JURIS,
TRIM(RBKP.IVTYP) AS ORIG_LGSTCS_INVC_VERIF_DOC,
TRIM(RBKP.XRBTX) AS IN_MORE_ONE_TAX_CD,
TRIM(RBKP.REPART) AS IN_INVC_VERIF_TYPE,
TRIM(RBKP.RBSTAT) AS INVC_DOC_STS,
TRIM(RBKP.KNUMVE) AS DOC_COND_OWN_COND,
TRIM(RBKP.KNUMVL) AS DOC_COND_VEND_ERR,
CAST(RBKP.ARKUEN AS DECIMAL(18, 4)) AS AUTO_INVC_RDCTN_AMT,
CAST(RBKP.ARKUEMW AS DECIMAL(18, 4)) AS SLS_TAX_AUTO_INVC_RDCTN_AMT,
CAST(RBKP.MAKZN AS DECIMAL(18, 4)) AS MAN_ACPT_NET_DIFF_AMT,
CAST(RBKP.MAKZMW AS DECIMAL(18, 4)) AS TAX_AMT_ACPT_MAN,
CAST(RBKP.LIEFFN AS DECIMAL(18, 4)) AS VEND_ERR,
CAST(RBKP.LIEFFMW AS DECIMAL(18, 4)) AS TAX_IN_VEND_ERR,
TRIM(RBKP.XAUTAKZ) AS IN_AUTMT_ACPT_INVC,
TRIM(RBKP.ESRNR) AS ISR_SBSCR_NUM,
TRIM(RBKP.ESRPZ) AS ISR_CHK_DIG,
TRIM(RBKP.ESRRE) AS ISR_REF_NUM,
CAST(RBKP.QSSHB AS DECIMAL(18, 4)) AS WTHL_TAX_BASE_AMT,
CAST(RBKP.QSFBT AS DECIMAL(18, 4)) AS WTHL_TAX_EXPT_AMT_DOC,
TRIM(RBKP.QSSKZ) AS WTHL_TAX_CD,
TRIM(RBKP.DIEKZ) AS SRVC_IN,
TRIM(RBKP.LANDL) AS SUPL_CTRY,
TRIM(RBKP.LZBKZ) AS ST_CENT_BANK_IN,
CAST(RBKP.TXKRS AS DECIMAL(18, 4)) AS EXCH_RT_FOR_TAX,
TRIM(RBKP.EMPFB) AS PAYEE_PYR,
TRIM(RBKP.BVTYP) AS PTNR_BANK_TYPE,
TRIM(RBKP.HBKID) AS SHRT_KEY_FOR_HS_BANK,
TRIM(RBKP.ZUONR) AS ASGNMT_NUM,
TRIM(RBKP.ZLSPR) AS PMT_BLK_KEY,
TRIM(RBKP.ZLSCH) AS PMT_METH,
CASE WHEN RBKP.ZFBDT = '00000000' THEN NULL ELSE to_timestamp(RBKP.ZFBDT,\"yyyyMMdd\") END AS BSLN_DUE_DT_CALC_DTTM,
TRIM(RBKP.KIDNO) AS PMT_REF,
TRIM(RBKP.REBZG) AS INVC_REF_DOC_NUM_INVC_REF,
TRIM(RBKP.REBZJ) AS FISC_YR_OF_RLVNT_INVC_CR,
TRIM(RBKP.XINVE) AS IN_CAP_GOODS_AFFCTD,
TRIM(RBKP.EGMLD) AS RPTG_CTRY_DELV_GOODS_EU,
TRIM(RBKP.XEGDR) AS IN_TRI_DEAL_WTHN_EU,
CASE WHEN RBKP.VATDATE = '00000000' THEN NULL ELSE to_timestamp(RBKP.VATDATE,\"yyyyMMdd\") END AS TAX_RPTG_DTTM,
TRIM(RBKP.HKONT) AS GENL_LDGR_ACCT,
TRIM(RBKP.J_1BNFTYPE) AS Nota_FISC_TYPE,
TRIM(RBKP.BRNCH) AS BRNCH_NUM,
TRIM(RBKP.ERFPR) AS ENT_PRFL_LGSTCS_INVC_VERIF,
TRIM(RBKP.SECCO) AS SECTN_CD,
TRIM(RBKP.NAME1) AS NM_1,
TRIM(RBKP.NAME2) AS NM_2,
TRIM(RBKP.NAME3) AS NM_3,
TRIM(RBKP.NAME4) AS NM_4,
TRIM(RBKP.PSTLZ) AS PSTL_CD,
TRIM(RBKP.ORT01) AS CITY,
TRIM(RBKP.LAND1) AS CTRY_KEY,
TRIM(RBKP.STRAS) AS HS_NUM_AND_STREET,
TRIM(RBKP.PFACH) AS PO_BOX,
TRIM(RBKP.PSTL2) AS PO_BOX_PSTL_CD,
TRIM(RBKP.PSKTO) AS ACCT_NUM_BANK_ACCT_POST_OFF,
TRIM(RBKP.BANKN) AS BANK_ACCT_NUM,
TRIM(RBKP.BANKL) AS BANK_NUM,
TRIM(RBKP.BANKS) AS BANK_CTRY_KEY,
TRIM(RBKP.STCD1) AS TAX_NUM_1,
TRIM(RBKP.STCD2) AS TAX_NUM_2,
TRIM(RBKP.STKZU) AS LIAB_FOR_VAT,
TRIM(RBKP.STKZA) AS IN_PTNR_SUBJ_EQLZTN_TAXI,
TRIM(RBKP.REGIO) AS RGN,
TRIM(RBKP.BKONT) AS BANK_CNTL_KEY,
TRIM(RBKP.DTAWS) AS INSTR_KEY_DATA_MED_EXCH,
TRIM(RBKP.DTAMS) AS IN_FOR_DATA_MED_EXCH,
TRIM(RBKP.SPRAS) AS CHAR_FLD_OF_LGTH_1,
TRIM(RBKP.XCPDK) AS IN_IS_ACCT_ONE_TIME_ACCT,
TRIM(RBKP.EMPFG) AS PAYEE_CD,
TRIM(RBKP.FITYP) AS TAX_TYPE,
TRIM(RBKP.STCDT) AS TAX_NUM_TYPE,
TRIM(RBKP.STKZN) AS NTRL_PRSN,
TRIM(RBKP.STCD3) AS TAX_NUM_3,
TRIM(RBKP.STCD4) AS TAX_NUM_4,
TRIM(RBKP.BKREF) AS REF_SPEC_FOR_BANK_DTL,
TRIM(RBKP.J_1KFREPRE) AS NM_OF_REP,
TRIM(RBKP.J_1KFTBUS) AS TYPE_OF_BUSN,
TRIM(RBKP.J_1KFTIND) AS TYPE_OF_INDSTR,
TRIM(RBKP.ANRED) AS TITLE,
TRIM(RBKP.STCEG) AS VAT_REGS_NUM,
TRIM(RBKP.ERNAME) AS ENT_BY_EXTRNL_SYS_USER,
CASE WHEN RBKP.REINDAT = '00000000' THEN NULL ELSE to_timestamp(RBKP.REINDAT,\"yyyyMMdd\") END AS INVC_RCPT_DTTM,
TRIM(RBKP.UZAWE) AS PMT_METH_SPLMN,
TRIM(RBKP.FDLEV) AS PLNG_LVL,
CASE WHEN RBKP.FDTAG = '00000000' THEN NULL ELSE to_timestamp(RBKP.FDTAG,\"yyyyMMdd\") END AS PLNG_DTTM,
TRIM(RBKP.ZBFIX) AS FX_PMT_TERM,
TRIM(RBKP.FRGKZ) AS IN_RLSE_REQ,
TRIM(RBKP.ERFNAM) AS NM_OF_PROC_ENT_OBJ,
TRIM(RBKP.BUPLA) AS BUSN_PLACE,
TRIM(RBKP.FILKD) AS ACCT_NUM_OF_BRNCH,
TRIM(RBKP.GSBER) AS BUSN_AREA,
TRIM(RBKP.LOTKZ) AS LOT_NUM_FOR_DOC,
TRIM(RBKP.SGTXT) AS ITM_TEXT,
TRIM(RBKP.INV_TRAN) AS TRX_IN_LGSTCS_INVC_VERIF,
TRIM(RBKP.PREPAY_STATUS) AS PRPMT_STS,
TRIM(RBKP.PREPAY_AWKEY) AS INVC_DOC_NUM_CRT_PRPMT,
TRIM(RBKP.ASSIGN_STATUS) AS INVC_IN_ASGNMT_TEST_PRCS,
CASE WHEN RBKP.ASSIGN_NEXT_DATE = '00000000' THEN NULL ELSE to_timestamp(RBKP.ASSIGN_NEXT_DATE,\"yyyyMMdd\") END AS NEXT_ASGNMT_TEST_DTTM,
CASE WHEN RBKP.ASSIGN_END_DATE = '00000000' THEN NULL ELSE to_timestamp(RBKP.ASSIGN_END_DATE,\"yyyyMMdd\") END AS END_ASGNMT_TEST_DTTM,
TRIM(RBKP.COPY_BY_BELNR) AS INVC_DOC_NUM_ORIG_INVC,
TRIM(RBKP.COPY_BY_YEAR) AS FISC_YR_OF_ORIG_INVC,
TRIM(RBKP.COPY_TO_BELNR) AS INVC_DOC_NUM_COPY_INVC,
TRIM(RBKP.COPY_TO_YEAR) AS FISC_YR_OF_COPY_INVC,
TRIM(RBKP.COPY_USER) AS USER_RVRS_AND_COPY_INVC,
CAST(RBKP.KURSX AS DECIMAL(18, 4)) AS MKT_DATA_EXCH_RT,
CASE WHEN RBKP.WWERT = '00000000' THEN NULL ELSE to_timestamp(RBKP.WWERT,\"yyyyMMdd\") END AS TRNL_DTTM,
TRIM(RBKP.XREF3) AS REF_KEY_FOR_LINE_ITM,
CASE WHEN RBKP.TXDAT = '00000000' THEN NULL ELSE to_timestamp(RBKP.TXDAT,\"yyyyMMdd\") END AS DTRMN_TAX_RT_DTTM,
CASE WHEN RBKP.TXDAT_FROM = '00000000' THEN NULL ELSE to_timestamp(RBKP.TXDAT_FROM,\"yyyyMMdd\") END AS VLD_FROM_TAX_RT_DTTM,
CAST(RBKP.CTXKRS AS DECIMAL(18, 4)) AS RT_TAX_VAL_RPTG_CRNCY,
TRIM(RBKP.HKTID) AS ID_FOR_ACCT_DTL,
TRIM(RBKP.MONAT) AS FISC_PER,
TRIM(RBKP.STCD5) AS TAX_NUM5,
TRIM(RBKP.INTAD) AS INET_ADDR_PTNR_CO_CLERK,
TRIM(RBKP.GLO_RE1_OT) AS CTRY_RGN_SPEC_ACCT_DATA,
TRIM(RBKP.BUSINESS_NETWORK_ORIGIN) AS ORIG_BUSN_NTWK_DOC,
TRIM(RBKP.ISEOPBLOCKED) AS BUSN_PRPS_CMPLT,
CASE WHEN RBKP.LASTCHANGEDATETIME = '00000000000000' THEN NULL ELSE to_timestamp(substring(RBKP.LASTCHANGEDATETIME,1,14),\"yyyyMMddHHmmss\") END AS UTC_TIME_STMP_LONG_DTTM,
TRIM(RBKP.GLO_REF1_HD) AS CTRY_RGN_SPEC_REF1_DOC,
CASE WHEN RBKP.GLO_DAT1_HD = '00000000' THEN NULL ELSE to_timestamp(RBKP.GLO_DAT1_HD,\"yyyyMMdd\") END AS CTRY_RGN_SPEC_DOC1_DTTM,
TRIM(RBKP.GLO_REF2_HD) AS CTRY_RGN_SPEC_REF2_DOC,
CASE WHEN RBKP.GLO_DAT2_HD = '00000000' THEN NULL ELSE to_timestamp(RBKP.GLO_DAT2_HD,\"yyyyMMdd\") END AS CTRY_RGN_SPEC_DOC2_DTTM,
TRIM(RBKP.GLO_REF3_HD) AS CTRY_RGN_SPEC_REF3_DOC,
CASE WHEN RBKP.GLO_DAT3_HD = '00000000' THEN NULL ELSE to_timestamp(RBKP.GLO_DAT3_HD,\"yyyyMMdd\") END AS CTRY_RGN_SPEC_DOC3_DTTM,
TRIM(RBKP.GLO_REF4_HD) AS CTRY_RGN_SPEC_REF4_DOC,
CASE WHEN RBKP.GLO_DAT4_HD = '00000000' THEN NULL ELSE to_timestamp(RBKP.GLO_DAT4_HD,\"yyyyMMdd\") END AS CTRY_RGN_SPEC_DOC4_DTTM,
TRIM(RBKP.GLO_REF5_HD) AS CTRY_RGN_SPEC_REF5_DOC,
CASE WHEN RBKP.GLO_DAT5_HD = '00000000' THEN NULL ELSE to_timestamp(RBKP.GLO_DAT5_HD,\"yyyyMMdd\") END AS CTRY_RGN_SPEC_DOC5_DTTM,
TRIM(RBKP.GLO_BP1_HD) AS CTRY_RGN_SPEC_BUSN_PTNR1,
TRIM(RBKP.GLO_BP2_HD) AS CTRY_RGN_SPEC_BUSN_PTNR2,
CASE WHEN RBKP.CIM_REPLICATIONTIMESTAMP = '00000000000000' THEN NULL ELSE to_timestamp(substring(RBKP.CIM_REPLICATIONTIMESTAMP,1,14),\"yyyyMMddHHmmss\") END AS UTC_TIME_STMP_LONG2_DTTM,
TRIM(RBKP.TMTYPE) AS TRSPN_MGMT_VERS,
TRIM(RBKP.TAX_COUNTRY) AS TAX_RPTG_CTRY_RGN,
TRIM(RBKP.TAX_COUNTRY_BNK) AS TAX_RPTG_CTRY_RGN2,
CASE WHEN RBKP.FULFILLDATE = '00000000' THEN NULL ELSE to_timestamp(RBKP.FULFILLDATE,\"yyyyMMdd\") END AS TAX_FFL_DTTM,
TRIM(RBKP.NODE_KEY) AS GUID_MM_SUP_INVC_IN1,
TRIM(RBKP.PARENT_KEY) AS GUID_MM_SUP_INVC_IN2,
TRIM(RBKP.ROOT_KEY) AS GUID_MM_SUP_INVC_IN3,
TRIM(RBKP.J_1TPBUPL) AS BRNCH_CD,
TRIM(RBKP.LOGMX_UUID) AS OBSOL_MEX_UUID,
TRIM(RBKP.ANXTYPE) AS INVC_TYPE,
CAST(RBKP.ANXAMNT AS DECIMAL(18, 4)) AS ANNEXATION_AMT,
CAST(RBKP.ANXPERC AS DECIMAL(18, 4)) AS ANNEXATION_PCT,
CASE WHEN RBKP.ZVAT_INDC = '00000000' THEN NULL ELSE to_timestamp(RBKP.ZVAT_INDC,\"yyyyMMdd\") END AS VAT_CONT_RUN_VAT_DTTM,
TRIM(RBKP.GST_PART) AS GST_PTNR,
TRIM(RBKP.PLC_SUP) AS PLACE_OF_SUPL,
TRIM(RBKP.IRN) AS INVC_REF_NUM,
TRIM(RBKP.PYBASTYP) AS TYPE_OF_PMT_BASIS_DOC,
TRIM(RBKP.PYBASNO) AS PMT_BASIS_DOC_NUM,
CASE WHEN RBKP.PYBASDAT = '00000000' THEN NULL ELSE to_timestamp(RBKP.PYBASDAT,\"yyyyMMdd\") END AS PMT_BASIS_DOC_DTTM,
TRIM(RBKP.PYIBAN) AS IBAN_INTNL_BANK_ACCT_NUM,
TRIM(RBKP.INWARDNO_HD) AS INCM_DOC_NUM,
CASE WHEN RBKP.INWARDDT_HD = '00000000' THEN NULL ELSE to_timestamp(RBKP.INWARDDT_HD,\"yyyyMMdd\") END AS INCM_DOC_DTTM,
TRIM(T003T.LTEXT) AS DOC_TYPE_DESC,
rbkp._upt_ as _l0_upt_,
rbkp._deleted_
FROM
  {Config.sourceDatabase}.RBKP RBKP
  LEFT JOIN {Config.sourceDatabase}.T003T T003T on RBKP.BLART=T003T.BLART 
  and T003T.spras = 'E' and T003T._deleted_ = 'F' and T003T.MANDT = {Config.MANDT}
WHERE RBKP._deleted_ = 'F' and RBKP.MANDT =  {Config.MANDT} 
 
"""
    )

    return out0
