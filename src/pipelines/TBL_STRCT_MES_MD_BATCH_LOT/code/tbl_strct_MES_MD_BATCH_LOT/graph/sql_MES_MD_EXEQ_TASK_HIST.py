from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_EXEQ_TASK_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as EXEQ_TASK_HIST_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as STRING) as CARR_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as COMT_TXT,\ncast('' as BOOLEAN) as CMPLT_TASK_FOR_BTCH_IND,\ncast('' as BOOLEAN) as CORR_APLD_IND,\ncast('' as STRING) as DVC_ID,\ncast('' as STRING) as DOC_SET_ID,\ncast('' as STRING) as ELCTRNC_PCDR_ID,\ncast('' as STRING) as EMPS_PERFM_WRK_TXT,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as INSTR_TXT,\ncast('' as STRING) as ISS_DIFF_RSN_ID,\ncast('' as STRING) as MATL_CNTNR_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as BOOLEAN) as PAS_IND,\ncast('' as STRING) as PC_ID,\ncast('' as INT) as SEQ_NBR,\ncast('' as STRING) as SPEC_ID,\ncast('' as STRING) as TASK_COMT_TXT,\ncast('' as STRING) as TASK_ID,\ncast('' as STRING) as TASK_LIST_ID,\ncast('' as INT) as TASK_LIST_SEQ_NBR,\ncast('' as STRING) as TASK_TYPE_CD,\ncast('' as STRING) as TRAIN_REQ_GRP_ID,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as WRKCELL_ID,\ncast('' as STRING) as WSTN_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
