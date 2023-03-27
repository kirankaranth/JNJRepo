from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_WORKFLOW.config.ConfigStore import *
from tbl_strct_MES_MD_WORKFLOW.udfs.UDFs import *

def sql_MES_MD_WRKF_STEP(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as WRKF_STEP_ID,\ncast('' as STRING) as WRKF_ID,\ncast('' as STRING) as ALT_BOM_RTE_STEP_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as DFLT_PATH_ID,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as ICON_ID,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_LAST_STEP_IND,\ncast('' as BOOLEAN) as JDE_INV_STEP_IND,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as ON_DFLT_RTE_ID,\ncast('' as STRING) as RTE_STEP_ID,\ncast('' as STRING) as SCHDLNG_DTL_ID,\ncast('' as INT) as SEQ_NBR,\ncast('' as STRING) as SPEC_BASE_ID,\ncast('' as INT) as SPEC_ID,\ncast('' as BOOLEAN) as STEP_CMPLT_REQ_IND,\ncast('' as BOOLEAN) as STEP_STRT_REQ_IND,\ncast('' as STRING) as STEP_TYPE_CD,\ncast('' as STRING) as SUB_WRKF_BASE_ID,\ncast('' as STRING) as SUB_WRKF_ID,\ncast('' as STRING) as WIP_MSG_LBL_ID,\ncast('' as STRING) as WRKF_STEP_DESC,\ncast('' as STRING) as WRKF_STEP_NM,\ncast('' as STRING) as X_LOC_VAL,\ncast('' as STRING) as Y_LOC_VAL,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
