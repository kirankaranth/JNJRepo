from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_WRKF_2.config.ConfigStore import *
from PPLN_MES_MD_WRKF_2.udfs.UDFs import *

def sql_MES_MD_WRKF(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
   '{Config.sourceSystem}' AS SRC_SYS_CD
, TRIM(Workflow.WORKFLOWID) as WRKF_ID
, TRIM(Workflow.WORKFLOWBASEID) as WRKF_BASE_ID
, TRIM(Workflow.CARTONLABELCOLLECTIONSTEPID) as CTN_LBL_CLCT_STEP_ID
, TRIM(Workflow.CARTONOPERATIONID) as CTN_OPR_ID
, TRIM(Workflow.CASELABELRECONSTEPID) as CASE_LBL_RECON_STEP_ID
, INT(Workflow.CHANGECOUNT) as CHG_CNT
, TRIM(Workflow.CHANGEHISTORYID) as CHG_HIST_ID
, TRIM(Workflow.CHECKLISTHEATSEALID) as CKLST_HEAT_SEAL_ID
, TRIM(Workflow.CHECKLISTHYDRATIONID) as CKLST_HYDRATION_ID
, TRIM(Workflow.CHECKLISTINJECTIONMOLDINGID) as CKLST_INJECTION_MOLDING_ID
, TRIM(Workflow.CHECKLISTLENSFABRICATIONID) as CKLST_LEN_FABRICATION_ID
, TRIM(Workflow.CHECKLISTPOSTHYDRATIONID) as CKLST_POST_HYDRATION_ID
, TRIM(Workflow.CHECKLISTQAID) as CKLST_QA_ID
, TRIM(Workflow.CHECKLISTSECONDARYPACKINGID) as CKLST_SEC_PACK_ID
, TRIM(Workflow.CHECKLISTSTERILIZATIONID) as CKLST_STERILIZATION_ID
, TRIM(Workflow.DATACOLLECTIONDEFCARTONLABELID) as DATA_CLCT_DEF_CTN_LBL_ID
, TRIM(Workflow.DATACOLLECTIONDEFFOILLABBASEID) as DATA_CLCT_DEF_FOIL_LAB_BASE_ID
, TRIM(Workflow.DATACOLLECTIONDEFFOILLABELID) as DATA_CLCT_DEF_FOIL_LBL_ID
, TRIM(Workflow.DATACOLLECTIONDEFPLCVERID) as DATA_CLCT_DEF_PLC_VER_ID
, TRIM(Workflow.DATACOLLECTIONDEFPLCVERBASEID) as DATA_CLCT_DEF_PLC_VERS_BASE_ID
, TRIM(Workflow.DATACOLLECTIONDEFPLCDHRBASEID) as DATA_CLCT_DEF_PLCDHR_BASE_ID
, TRIM(Workflow.DATACOLLECTIONDEFPLCDHRID) as DATA_CLCT_DEF_PLCDHR_ID
, TRIM(Workflow.DATACOLLECTIONDEFPOSTHYDBASEID) as DATA_CLCT_DEF_POST_HYD_BASE_ID
, TRIM(Workflow.DATACOLLECTIONDEFPOSTHYDID) as DATA_CLCT_DEF_POST_HYD_ID
, TRIM(Workflow.ECO) as ENGR_CHG_ORDR_NUM
, TRIM(Workflow.ERPROUTEBASEID) as ERP_RTE_BASE_ID
, TRIM(Workflow.ERPROUTEID) as ERP_RTE_ID
, TRIM(Workflow.EXCLUSIVEMASTERLOTCHECKLISTID) as EXCLV_MSTR_LOT_CKLST_ID
, TRIM(Workflow.FIRSTSTEPID) as FST_STEP_ID
, TRIM(Workflow.FOILOPERATIONID) as FOIL_OPR_ID
, CAST(NULL AS BOOLEAN) as GSL_WRKF_IND
, TRIM(Workflow.ICONID) as ICON_ID
, TRIM(Workflow.IMAGE) as IMG_TXT
, CAST(TRIM(Workflow.ISEXCLUSIVECHECK) AS BOOLEAN) AS IS_EXCLV_CHK_IND
, CAST(TRIM(Workflow.ISFROZEN) AS BOOLEAN) AS IS_FRZN_IND
, TRIM(Workflow.LASTINPROCESSSTEPCHECKLISTID) as LAST_IN_PRCS_STEP_CKLST_ID
, TRIM(Workflow.LOCATIONRETURNFROMDISTRIBUTION) as LOC_RTN_FROM_DSTN_TXT
, TRIM(Workflow.NOTES) as NOTES_TXT
, TRIM(Workflow.CDOTYPEID) as OBJ_TYPE_ID
, TRIM(Workflow.PRODUCTCONVMULTISTEPNAME) as PROD_CONV_MULTI_STEP_NM
, TRIM(Workflow.WORKFLOWREVISION) as RVSN_ID
, TRIM(Workflow.REWORKSTEPNAME) as RWRK_STEP_NM
, TRIM(Workflow.SPLITLOTINTROHSSTEP) as SPLT_LOT_INTRO_HS_STEP_ID
, TRIM(Workflow.SPLITLOTINTROSTEPNAME) as SPLT_LOT_INTRO_STEP_NM
, NULL as SPLT_MOVE_TO_SPEC_ID
, TRIM(Workflow.STATUS) as STS_CD
, CAST(Workflow.TOTALCYCLETIME AS DECIMAL(18, 4)) as TOT_CYC_TIME_QTY
, CAST(Workflow.TOTALYIELD AS DECIMAL(18, 4)) as TOT_YLD_QTY
, TRIM(Workflow.WIPMSGDEFMGRID) as WIP_MSG_DEF_MGR_ID
, TRIM(Workflow.DESCRIPTION) as WRKF_DESC
FROM  {Config.sourceDatabase}.Workflow as Workflow  WHERE Workflow._deleted_ = 'F'  
 
"""
    )

    return out0
