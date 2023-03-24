from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_CNTNR_STS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as CNTNR_STS_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CARR_ID,\ncast('' as INT) as CUR_STEP_PAS_CNT,\ncast('' as STRING) as FCTRY_ID,\ncast('' as STRING) as INIT_RECIPE_LIST_ID,\ncast('' as BOOLEAN) as IN_PRCS_IND,\ncast('' as BOOLEAN) as IN_RWRK_IND,\ncast('' as STRING) as LAST_CMPLT_TASK_ID,\ncast('' as TIMESTAMP) as LAST_MOVE_DTTM,\ncast('' as TIMESTAMP) as LAST_MOVE_GMT_DTTM,\ncast('' as STRING) as LAST_REV_TXN_ID,\ncast('' as STRING) as LOC_ID,\ncast('' as STRING) as RSRS_ID,\ncast('' as INT) as RWRK_LOOP_CNT,\ncast('' as INT) as RWRK_TOT_CNT,\ncast('' as STRING) as SPEC_ID,\ncast('' as STRING) as STEP_ENT_TXN_ID,\ncast('' as STRING) as WRKF_STEP_ID,\ncast('' as STRING) as IS_RTE_STEP_ID,\ncast('' as INT) as TMRS_CNT,\ncast('' as STRING) as WSTN_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
