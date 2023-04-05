from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_WRKF_STEP_4.config.ConfigStore import *
from PPLN_MES_MD_WRKF_STEP_4.udfs.UDFs import *

def sql_MES_MD_WRKF_STEP(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
   '{Config.sourceSystem}' AS SRC_SYS_CD
, TRIM(WorkflowStep.WORKFLOWSTEPID) as WRKF_STEP_ID
, TRIM(WorkflowStep.WORKFLOWID) as WRKF_ID
, TRIM(WorkflowStep.gdnAltBOMRouteStepId) as ALT_BOM_RTE_STEP_ID
, INT(WorkflowStep.CHANGECOUNT) as CHG_CNT
, TRIM(WorkflowStep.DEFAULTPATHID) as DFLT_PATH_ID
, TRIM(WorkflowStep.EXPORTIMPORTKEY) as EXPT_IMPT_KEY_VAL
, TRIM(WorkflowStep.ICONID) as ICON_ID
, CAST(WorkflowStep.ISFROZEN AS BOOLEAN) as IS_FRZN_IND
, CAST(WorkflowStep.ISLASTSTEP AS BOOLEAN) as IS_LAST_STEP_IND
, NULL as JDE_INV_STEP_IND
, TRIM(WorkflowStep.NOTES) as NOTES_TXT
, TRIM(WorkflowStep.CDOTYPEID) as OBJ_TYPE_ID
, TRIM(WorkflowStep.ONDEFAULTROUTE) as ON_DFLT_RTE_ID
, TRIM(WorkflowStep.ROUTESTEPID) as RTE_STEP_ID
, TRIM(WorkflowStep.SCHEDULINGDETAILID) as SCHDLNG_DTL_ID
, INT(WorkflowStep.SEQUENCE) as SEQ_NBR
, TRIM(WorkflowStep.SPECBASEID) as SPEC_BASE_ID
, TRIM(WorkflowStep.SPECID) as SPEC_ID
, NULL as STEP_CMPLT_REQ_IND
, NULL as STEP_STRT_REQ_IND
, TRIM(WorkflowStep.STEPTYPE) as STEP_TYPE_CD
, TRIM(WorkflowStep.SUBWORKFLOWBASEID) as SUB_WRKF_BASE_ID
, TRIM(WorkflowStep.SUBWORKFLOWID) as SUB_WRKF_ID
, TRIM(WorkflowStep.WIPMSGLABEL) as WIP_MSG_LBL_ID
, TRIM(WorkflowStep.DESCRIPTION) as WRKF_STEP_DESC
, TRIM(WorkflowStep.WORKFLOWSTEPNAME) as WRKF_STEP_NM
, TRIM(WorkflowStep.XLOCATION) as X_LOC_VAL
, TRIM(WorkflowStep.YLOCATION) as Y_LOC_VAL
FROM  {Config.sourceDatabase}.WorkflowStep as WorkflowStep  WHERE WorkflowStep._deleted_ = 'F'
  
 
"""
    )

    return out0
