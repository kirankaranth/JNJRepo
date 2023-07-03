from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_SPEC_4.config.ConfigStore import *
from PPLN_MES_MD_SPEC_4.udfs.UDFs import *

def sql_MES_MD_SPEC(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT '{Config.sourceSystem}' AS SRC_SYS_CD
, TRIM(SPEC.SPECID) as SPEC_ID
, TRIM(SPEC.OPERATIONID) as OPR_ID
, TRIM(SPEC.SPECBASEID) as SPEC_BASE_ID
, CAST(SPEC.ALLOWOVERRIDES as BOOLEAN ) as ALLW_OVRDS_IND
, TRIM(SPEC.ALLOWREWORKTO) as ALLW_RWRK_TO_TXT
, CAST(SPEC.ALWAYSOVERRIDEEXPIRATIONDATE as BOOLEAN ) as ALWAYS_OVRD_EXPN_DT_IND
, CAST(SPEC.ASSIGNUDI as BOOLEAN ) as ASGN_UDI_IND
, CAST(SPEC.ASSIGNDUI1 as BOOLEAN ) as ASGN_UDI_1_IND
, CAST(NULL AS STRING) as BP_SCALE_GRP_ID
, CAST(NULL AS STRING) as CARR_AUTO_MODE_CD
, INT(SPEC.CHANGECOUNT) as CHG_CNT
, TRIM(SPEC.CHANGEHISTORYID) as CHG_HIST_ID
, CAST(NULL AS STRING) as DFLT_RUN_LVL_ID
, CAST(NULL AS STRING) as DFLT_RUN_MC_BASE_ID
, CAST(NULL AS STRING) as DFLT_RUN_RMC_ID
, TRIM(SPEC.DOCUMENTSETID) as DOC_SET_ID
, TRIM(SPEC.ELECTRONICPROCEDUREID) as ELCTRNC_PCDR_ID
, TRIM(SPEC.ELECTRONICPROCEDUREBASEID) as ELCTRNC_PCDR_BASE_ID
, TRIM(SPEC.ECO) as ENGR_CHG_ORDR_NUM
, CAST(NULL AS STRING) as ESIG_TXN_MAP_GRP_ID
, TRIM(SPEC.EXPIRATIONPERIODUNITS) as EXPN_PER_UOM_CD
, TRIM(SPEC.EXPIRATIONPERIOD) as EXPN_PER_VAL
, Case when SPEC.EXPIRATIONDATETRANSACTION = '0000000000' then CAST(NULL AS TIMESTAMP) else to_timestamp(left(SPEC.EXPIRATIONDATETRANSACTION,8),\"yyyyMMdd\") end AS EXPN_TRX_DTTM
, CAST(NULL AS STRING) as GSL_WRKF_ID
, TRIM(SPEC.ICONID) as ICON_ID
, CAST(SPEC.ISFROZEN as BOOLEAN ) as IS_FRZN_IND
, CAST(NULL AS BOOLEAN) as IS_LD_CARR_IND
, CAST(NULL AS BOOLEAN) as IS_UNLD_CARR_IND
, CAST(NULL AS BOOLEAN) as KEP_RSRS_ASGNMT_IND
, CAST(NULL AS STRING) as LBL_PRT_MSG_1_TXT
, CAST(NULL AS STRING) as LBL_PRT_MSG_2_TXT
, CAST(NULL AS STRING) as LBL_TXN_MAP_GRP_ID
, Case when SPEC.MANUFACTURINGDATETRANSACTION = '0000000000' then CAST(NULL AS TIMESTAMP) else to_timestamp(left(SPEC.MANUFACTURINGDATETRANSACTION,8),\"yyyyMMdd\") end AS MFG_TRX_DTTM
, TRIM(SPEC.ccfMaxProcessTime) as MAX_PRCS_TIME_VAL
, TRIM(SPEC.MAXQUEUETIMEACTION) as MAX_QUE_TIME_ACTN_CD
, TRIM(SPEC.MAXQUEUETIME) as MAX_QUE_TIME_VAL
, TRIM(SPEC.ccfMinProcessTime) as MIN_PRCS_TIME_VAL
, TRIM(SPEC.ccfMinQueueTime) as MIN_QUE_TIME_VAL
, TRIM(SPEC.NOTES) as NOTES_TXT
, TRIM(SPEC.CDOTYPEID) as OBJ_TYPE_ID
, TRIM(SPEC.RECIPEFILEBASEID) as RECIPE_FILE_BASE_ID
, TRIM(SPEC.RECIPEFILEID) as RECIPE_FILE_ID
, TRIM(SPEC.RESOURCEGROUPID) as RSRS_GRP_ID
, TRIM(SPEC.ccfResourceLineGroupAvailabili) as RSRS_LINE_GRP_AVLBLTY_TXT
, CAST(SPEC.ccfResourceLineGroupMaintChk as BOOLEAN ) as RSRS_LINE_GRP_MNT_CHK_IND
, TRIM(SPEC.SCHEDULINGDETAILID) as SCHDLNG_DTL_ID
, TRIM(SPEC.SETUPBASEID) as SETUP_BASE_ID
, TRIM(SPEC.SETUPID) as SETUP_ID
, TRIM(SPEC.DESCRIPTION) as SPEC_DESC
, TRIM(SPEC.SPECREVISION) as SPEC_RVSN_ID
, TRIM(SPEC.STATUS) as STS_CD
, TRIM(SPEC.TRAININGREQGROUPID) as TRNG_REQ_GRP_ID
, CAST(NULL AS STRING) as USER_DEFIN_STRNG_TXT
, CAST(SPEC.VALIDATELOTSAMPLINGCOMPLETE as BOOLEAN ) as VLD_LOT_SMPLG_CMPLT_IND
, CAST(SPEC.VALIDATEMATERIALCONSUMPTION as BOOLEAN ) as VLD_MATL_CNSMPTN_IND
, CAST(NULL AS BOOLEAN) as VLD_PDDC_IND
, CAST(NULL AS BOOLEAN) as VLD_PLC_VERS_IND
, CAST(SPEC.VALIDATEUDI as BOOLEAN ) as VLD_UDI_IND
, CAST(NULL AS STRING) as VLD_PCDR_BASE_ID
, CAST(NULL AS STRING) as VLD_PCDR_ID
, TRIM(SPEC.WARNINGQUEUETIME) as WARN_QUE_TIME_VAL
, TRIM(SPEC.WIPMSGDEFMGRID) as WIP_MSG_DEF_MGR_ID
FROM  {Config.sourceDatabase}.SPEC as SPEC WHERE SPEC._deleted_ =  'F'
  
 
"""
    )

    return out0
