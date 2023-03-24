from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_CMPNT_ISS_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as CMPNT_ISS_HIST_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as STRING) as BTCH_ID,\ncast('' as BOOLEAN) as BILL_CNTL_IND,\ncast('' as BOOLEAN) as BYPAS_EXPN_CHK_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as DVC_ID,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as MSTR_RECIPE_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as PC_ID,\ncast('' as STRING) as RECIPE_ISS_ITM_ID,\ncast('' as STRING) as RECIPE_LIST_ID,\ncast('' as STRING) as TXN_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
