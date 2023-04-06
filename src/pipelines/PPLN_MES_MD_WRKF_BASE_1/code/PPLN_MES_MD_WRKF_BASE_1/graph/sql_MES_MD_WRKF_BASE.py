from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_WRKF_BASE_1.config.ConfigStore import *
from PPLN_MES_MD_WRKF_BASE_1.udfs.UDFs import *

def sql_MES_MD_WRKF_BASE(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
   '{Config.sourceSystem}' AS SRC_SYS_CD
, TRIM(WorkflowBase.WORKFLOWBASEID) as WRKF_BASE_ID
, CAST(WorkflowBase.CHANGECOUNT as INT) as CHG_CNT
, TRIM(WorkflowBase.ICONID) as ICON_ID
, TRIM(WorkflowBase.CDOTYPEID) as OBJ_TYPE_ID
, TRIM(WorkflowBase.REVOFRCDID) as RVSN_ID
, TRIM(WorkflowBase.WORKFLOWNAME) as WRKF_NM
FROM  {Config.sourceDatabase}.WorkflowBase as WorkflowBase  WHERE WorkflowBase._deleted_ = 'F'   
 
"""
    )

    return out0
