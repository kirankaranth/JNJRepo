from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_ordr_sched_line_delv_tai.config.ConfigStore import *
from sap_md_sls_ordr_sched_line_delv_tai.udfs.UDFs import *

def sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    select
'{Config.sourceSystem}' AS SRC_SYS_CD,
VBEP.posnr as SCHED_LINE_ITM_NUM,
VBEP.vbeln AS SLS_ORDR_DOC_ID,
VBEP.etenr AS SCHED_LINE_NUM,
VBAK.bukrs_vf AS CO_CD,
VBAK.auart AS SLS_ORDR_TYPE_CD,
CASE WHEN VBEP.mbdat = '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(CONCAT(VBEP.mbdat,VBEP.mbuhr),'yyyyMMddHHmmss') END AS MATL_AVLBLTY_DTTM,
CAST(TRIM(VBEP.bmeng) AS DECIMAL(18,4)) AS CNFRM_QTY,
CASE WHEN VBEP.edatu = '00000000' then null else to_timestamp(VBEP.edatu,\"yyyyMMdd\") end as RQST_DELV_DTTM,
CASE WHEN VBEP.wadat = '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(CONCAT(VBEP.wadat,VBEP.wauhr),'yyyyMMddHHmmss') END AS GI_DTTM,
CAST(TRIM(VBEP.wmeng) AS DECIMAL(18,4)) AS SLS_UNIT_ORDR_QTY,
TRIM(VBEP.vrkme) AS SLS_UOM_CD,
TRIM(VBEP.lifsp) AS DELV_BLK_CD,
TRIM(TVLST.vtext) AS DELV_BLK_DESC,
CASE WHEN VBEP.tddat = '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(CONCAT(VBEP.tddat,VBEP.tduhr),'yyyyMMddHHmmss') END AS TRSPN_PLAN_DTTM,
CASE WHEN VBEP.lddat = '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(CONCAT(VBEP.lddat,VBEP.lduhr),'yyyyMMddHHmmss') END AS LD_DTTM,
TRIM(ETTYP) SCHED_LINE_CAT,
TRIM(LFREL) ITM_RLVNT_DELV,
case when VBEP.EDATU = '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(concat(VBEP.EDATU,VBEP.EZEIT),'yyyyMMddHHmmss') END AS ARR_DTTM,
cast(trim(LMENG) as decimal(18,4)) REQ_QTY_MGMT_SKU,
TRIM(MEINS) BASE_UNIT_OF_MEAS,
case when BDDAT = '00000000' then cast(null as TIMESTAMP) else to_timestamp(BDDAT,\"yyyyMMdd\") end as REQ_DTTM,
TRIM(BDART) REQ_TYPE,
TRIM(PLART) PLNG_TYPE,
TRIM(VBELE) BUSN_DOC_NUM,
TRIM(POSNE) BUSN_ITM_NUM,
TRIM(ETENE) SCHED_LINE,
case when RSDAT = '00000000' then cast(null as TIMESTAMP) else to_timestamp(RSDAT,\"yyyyMMdd\") end as EARLY_POSBL_RESV_DTTM,
TRIM(IDNNR) MAINT_RQST,
TRIM(BANFN) PRCH_REQSN_NUM,
TRIM(BSART) ORDR_TYPE,
TRIM(BSTYP) PRCHSNG_DOC_CAT,
TRIM(WEPOS) CNFRM_STS_SCHED_LINE,
TRIM(REPOS) INVC_RCPT_IN,
case when LRGDT = '00000000' then cast(null as TIMESTAMP) else to_timestamp(LRGDT,\"yyyyMMdd\") end as RTN_RTRN_PKGNG_DTTM,
TRIM(PRGRS) DT_TYPE,
cast(TRIM(CMENG) as decimal(18,4)) CORR_QTY_SLS_UNIT,
TRIM(GRSTR) GRP_DEF_STRC_DATA,
TRIM(ABART) RLSE_TYPE,
TRIM(ABRUF) FCST_DELV_SCHED_NUM,
cast(TRIM(ROMS1) as decimal(18,4)) CMT_QTY,
cast(TRIM(ROMS2) as decimal(18,4)) SIZE_2,
cast(TRIM(ROMS3) as decimal(18,4)) SIZE_3,
TRIM(ROMEI) UNIT_MEAS_SIZE_1_TO_3,
TRIM(RFORM) FRML_KEY_VAR_SIZE_ITM,
TRIM(UMVKZ) NUMRTR_CONV_SLS_QTY_INTO_SKU,
TRIM(UMVKN) DENOM_CONV_SLS_QTY_INTO_SKU,
TRIM(VERFP) AVLBLTY_CNFRM_AUTMT,
TRIM(BWART) MVMT_TYPE,
TRIM(BNFPO) ITM_NUM_PRCH_REQSN,
TRIM(ETART) SCHED_LINE_TYPE_EDI,
TRIM(VBEP.AUFNR) ORDR_NUM,
TRIM(PLNUM) PLAN_ORDR,
TRIM(SERNR) BOM_EXPLS_NUM,
TRIM(AESKD) CUST_ENGR_CHG_STS,
cast(TRIM(ABGES) as decimal(18,4)) GUARNTD,
TRIM(AULWE) RTE_SCHED,
cast(null as timestamp) as HAND_OVR_LOC_DTTM,
null as  DELV_DT_RULE,
cast(null as decimal(18,4)) as DELV_QTY_BU,
cast(null as decimal(18,4)) as DELV_QTY_SU,
cast(null as decimal(18,4)) as OPEN_CNFRM_DELV_QTY_BU,
cast(null as decimal(18,4)) as OPEN_CNFRM_DELV_QTY_SU,
cast(null as decimal(18,4)) as OPEN_RQST_DELV_QTY_BU,
cast(null as decimal(18,4)) as OPEN_RQST_DELV_QTY_SU,
cast(null as timestamp) as DELV_CRT_DTTM,
cast(null as timestamp) as  SCHED_LINE_DTTM,
null as  RQR_CLS,
cast(null as timestamp) as DATA_FIL_VAL_DATA_AGE_DTTM,
null as  SD_DOC_CRNCY,
cast(null as decimal(18,4)) as OPEN_DELV_NET_AMT,
null as  GUID_RAW_FMT,
null as  LEGAL_CNTL_STS,
cast(null as decimal(18,4)) as RQST_REQ_QTY_BASE_UNIT,
cast(null as decimal(18,4)) as CNFRM_REQ_QTY_BASE_UNIT,
null as  DUMMY_FUNC_LGTH_1,
cast(null as decimal(18,4)) as ARUN_REQ_ALC_QTY,
null as  ORDR_SCHDLNG_GRP_ID,
null as  REJ_CD_FOR_PRTL_QTY_REJ,
cast(null as timestamp) as MATL_AVLBLTY_3PTY_ORDR_PLNG_DTTM,
VBEP._upt_ as _l0_upt_
from {Config.sourceDatabase}.VBEP VBEP
LEFT JOIN {Config.sourceDatabase}.VBAK VBAK
on VBEP.VBELN = VBAK.VBELN
AND VBAK._deleted_ = 'F' AND VBAK.MANDT = \"800\"
LEFT JOIN {Config.sourceDatabase}.TVLST TVLST
ON VBEP.LIFSP = TVLST.LIFSP
AND TVLST._deleted_ = 'F' AND TVLST.MANDT = \"800\" AND TVLST.SPRAS = 'E'
where VBEP._deleted_ = 'F' AND VBEP.MANDT =\"800\"  
 
"""
    )

    return out0
