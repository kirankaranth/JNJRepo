from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from PPLN_MES_MD_RWRK_RSN_1.config.ConfigStore import *
from PPLN_MES_MD_RWRK_RSN_1.udfs.UDFs import *

def sql_MES_MD_RWRK_RSN(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    
SELECT
  '{Config.sourceSystem}' AS SRC_SYS_CD,
  TRIM(ReworkReason.REWORKREASONID) as RWRK_RSN_ID,
  cast(TRIM(ReworkReason.CHANGECOUNT) as INT) as CHG_CNT,
  TRIM(ReworkReason.CHANGEHISTORYID) as CHG_HIST_ID,
  TRIM(ReworkReason.ICONID) as ICON_ID,
  cast(TRIM(ReworkReason.ISFROZEN) as BOOLEAN) as IS_FRZN_IND,
  TRIM(ReworkReason.NOTES) as NOTES_TXT,
  TRIM(ReworkReason.CDOTYPEID) as OBJ_TYPE_ID,
  TRIM(ReworkReason.REWORKREASONNAME) as RWRK_RSN_NM,
  TRIM(ReworkReason.DESCRIPTION) as RWRK_RSN_DESC,
  TRIM(ReworkReason.WIPMSGDEFMGRID) as WIP_MSG_DEF_MGR_ID
FROM {Config.sourceDatabase}.ReworkReason as ReworkReason
Where ReworkReason._deleted_ = 'F'  
 
"""
    )

    return out0
