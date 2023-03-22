from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_WORKFLOW.config.ConfigStore import *
from tbl_strct_MES_MD_WORKFLOW.udfs.UDFs import *

def sql_MES_MD_OUTB_CIO_RQST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as STRING) as MSG_TYPE_TXT,\ncast('' as STRING) as OPR_ID,\ncast('' as STRING) as WRKF_STEP_ID,\ncast('' as BOOLEAN) as CN_RESUBMT_IND,\ncast('' as STRING) as ERR_MSG_TXT,\ncast('' as TIMESTAMP) as LAST_ACTV_DTTM,\ncast('' as STRING) as CNTNR_ID,\ncast('' as STRING) as STS_CD,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
