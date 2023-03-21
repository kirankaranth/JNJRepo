from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_WORKFLOW.config.ConfigStore import *
from tbl_strct_MES_MD_WORKFLOW.udfs.UDFs import *

def sql_MES_MD_RTE_STEP(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as RTE_STEP_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as DUR_PER_UNIT_VAL,\ncast('' as STRING) as ERP_OPR_TXT,\ncast('' as STRING) as ERP_RTE_ID,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as IS_FRZN_IND,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as RTE_STEP_DESC,\ncast('' as STRING) as RTE_STEP_NM,\ncast('' as STRING) as RUN_RT_OPT_CD,\ncast('' as INT) as SEQ_NBR,\ncast('' as STRING) as SETUP_TIME_VAL,\ncast('' as STRING) as TIME_PER_BTCH_VAL,\ncast('' as STRING) as UNIT_PER_HR_VAL,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
