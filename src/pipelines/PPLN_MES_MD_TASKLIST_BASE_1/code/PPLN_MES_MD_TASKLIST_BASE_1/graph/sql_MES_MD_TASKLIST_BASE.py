from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_TASKLIST_BASE_1.config.ConfigStore import *
from PPLN_MES_MD_TASKLIST_BASE_1.udfs.UDFs import *

def sql_MES_MD_TASKLIST_BASE(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
  '{Config.sourceSystem}' AS SRC_SYS_CD,
  TRIM(TasklistBase.TasklistBaseId) as TASKLIST_BASE_ID,
  cast(TRIM(TasklistBase.ChangeCount) as INT) as CHG_CNT,
  TRIM(TasklistBase.ICONID) as ICON_ID,
  TRIM(TasklistBase.CDOTYPEID) as OBJ_TYPE_ID,
  TRIM(TasklistBase.REVOFRCDID) as RVSN_ID,
  TRIM(TasklistBase.TASKLISTNAME) as TASK_LIST_NM
FROM {Config.sourceDatabase}.TasklistBase as TasklistBase
Where TasklistBase._deleted_ = 'F'  
 
"""
    )

    return out0
