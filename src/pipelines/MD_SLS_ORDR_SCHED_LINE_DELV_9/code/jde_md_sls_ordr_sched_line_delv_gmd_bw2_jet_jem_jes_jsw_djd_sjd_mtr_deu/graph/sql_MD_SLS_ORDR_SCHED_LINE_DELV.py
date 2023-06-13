from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_sched_line_delv_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu.config.ConfigStore import *
from jde_md_sls_ordr_sched_line_delv_gmd_bw2_jet_jem_jes_jsw_djd_sjd_mtr_deu.udfs.UDFs import *

def sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    select 
  '{Config.sourceSystem}' AS SRC_SYS_CD,
 F4211.SDOCTO AS SCHED_LINE_ITM_NUM,
 F4211.SDDOCO AS SLS_ORDR_DOC_ID,
 F4211.SDLNID AS SCHED_LINE_NUM,
 F4211.SDKCOO AS CO_CD,
 F4211.SDDCTO AS SLS_ORDR_TYPE_CD,
 CASE WHEN
LOWER(TRIM(SDPDDJ)) = 'null'
OR TRIM(SDPDDJ) = ''
OR TRIM(SDPDDJ) = '0'
THEN null
ELSE
to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(F4211.SDPDDJ) AS INT) + 1900000 AS STRING),1,4),'-01-01'),
CAST(substr(CAST(CAST(TRIM(F4211.SDPDDJ) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS MATL_AVLBLTY_DTTM,
 CAST(TRIM(F4211.SDSOQS) AS DECIMAL(18,4)) AS CNFRM_QTY,
 null AS RQST_DELV_DTTM,
 null AS  GI_DTTM,
 CAST(TRIM(F4211.SDUORG) AS DECIMAL(18,4)) AS SLS_UNIT_ORDR_QTY,
 TRIM(F4211.SDUOM) AS SLS_UOM_CD,
 null AS  DELV_BLK_CD,
 null AS  DELV_BLK_DESC,
cast(null AS timestamp) as TRSPN_PLAN_DTTM,
cast(null AS timestamp) as LD_DTTM,
null as  SCHED_LINE_CAT,
null as  ITM_RLVNT_DELV,
cast(null AS timestamp) as ARR_DTTM,
cast(null AS decimal(18,4)) as REQ_QTY_MGMT_SKU,
null as  BASE_UNIT_OF_MEAS,
cast(null AS timestamp) as REQ_DTTM,
null as  REQ_TYPE,
null as  PLNG_TYPE,
null as  BUSN_DOC_NUM,
null as  BUSN_ITM_NUM,
null as  SCHED_LINE,
cast(null AS timestamp) as EARLY_POSBL_RESV_DTTM,
null as  MAINT_RQST,
null as  PRCH_REQSN_NUM,
null as  ORDR_TYPE,
null as  PRCHSNG_DOC_CAT,
null as  CNFRM_STS_SCHED_LINE,
null as  INVC_RCPT_IN,
cast(null AS timestamp) as RTN_RTRN_PKGNG_DTTM,
null as  DT_TYPE,
cast(null AS decimal(18,4)) as CORR_QTY_SLS_UNIT,
null as  GRP_DEF_STRC_DATA,
null as  RLSE_TYPE,
null as  FCST_DELV_SCHED_NUM,
cast(null AS decimal(18,4)) as CMT_QTY,
cast(null AS decimal(18,4)) as SIZE_2,
cast(null AS decimal(18,4)) as SIZE_3,
null as  UNIT_MEAS_SIZE_1_TO_3,
null as  FRML_KEY_VAR_SIZE_ITM,
null as  NUMRTR_CONV_SLS_QTY_INTO_SKU,
null as  DENOM_CONV_SLS_QTY_INTO_SKU,
null as  AVLBLTY_CNFRM_AUTMT,
null as  MVMT_TYPE,
null as  ITM_NUM_PRCH_REQSN,
null as  SCHED_LINE_TYPE_EDI,
null as  ORDR_NUM,
null as  PLAN_ORDR,
null as  BOM_EXPLS_NUM,
null as  CUST_ENGR_CHG_STS,
cast(null AS decimal(18,4)) as GUARNTD,
null as  RTE_SCHED,
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
F4211._upt_ as _l0_upt_
from {Config.sourceDatabase}.F4211 F4211
where F4211._deleted_ = 'F' 
  
 
"""
    )

    return out0
