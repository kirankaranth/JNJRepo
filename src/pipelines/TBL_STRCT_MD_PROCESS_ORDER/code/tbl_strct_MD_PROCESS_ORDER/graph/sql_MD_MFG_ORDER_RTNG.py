from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PROCESS_ORDER.config.ConfigStore import *
from tbl_strct_MD_PROCESS_ORDER.udfs.UDFs import *

def sql_MD_MFG_ORDER_RTNG(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as ORDR_RTNG_CTR_NUM,\ncast('' as String) as ORDR_RTNG_NUM,\ncast('' as String) as WRK_CNTR_ID,\ncast('' as String) as OPER_DESC,\ncast('' as String) as OPR_STS_CD,\ncast('' as String) as OPR_STS_TXT,\ncast('' as String) as OPER_CD,\ncast('' as String) as OPT_OVRLAP_IND,\ncast('' as String) as REQ_OVRLAP_IND,\ncast('' as String) as OPER_NUM,\ncast('' as String) as KEY_PRFL_CD,\ncast('' as String) as UDF1_TXT,\ncast('' as String) as UDF2_TXT,\ncast('' as String) as UDF3_TXT,\ncast('' as String) as UDF4_TXT,\ncast('' as decimal(18,4)) as UDF1_QTY,\ncast('' as String) as UDF1_UOM_CD,\ncast('' as decimal(18,4)) as UDF2_QTY,\ncast('' as String) as UDF2_UOM_CD,\ncast('' as decimal(18,4)) as UDF1_AMT,\ncast('' as String) as UDF1_CURR_CD,\ncast('' as decimal(18,4)) as UDF2_AMT,\ncast('' as String) as UDF2_CURR_CD,\ncast('' as timestamp) as UDF1_DTTM,\ncast('' as timestamp) as UDF2_DTTM,\ncast('' as String) as UDF1_IND,\ncast('' as String) as UDF2_IND,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
