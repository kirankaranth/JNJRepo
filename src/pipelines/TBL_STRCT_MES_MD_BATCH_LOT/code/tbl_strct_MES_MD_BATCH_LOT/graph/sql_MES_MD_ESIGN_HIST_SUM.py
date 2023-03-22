from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_ESIGN_HIST_SUM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as ESIGN_HIST_SUM_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as COSIGN_ROLE_ID,\ncast('' as STRING) as ESIGN_REQ_DTL_ID,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as MEANING_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as ROLE_ID,\ncast('' as INT) as SGNTR_CNT,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as VERIF_METH_CD,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
