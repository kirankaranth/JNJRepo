from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_MATL_QTY_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as MATL_QTY_HIST_ID,\ncast('' as STRING) as BTCH_ID,\ncast('' as STRING) as CARR_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as DECIMAL(18,4)) as CHG_QTY,\ncast('' as DECIMAL(18,4)) as CHG_2_QTY,\ncast('' as BOOLEAN) as CLSE_WHEN_ZERO_IND,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as STRING) as IS_CHRG_TO_RSRS_ID,\ncast('' as STRING) as MSTR_RECIPE_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as RSRS_ID,\ncast('' as STRING) as RLUP_RSN_ID,\ncast('' as STRING) as THRUPUT_RPTG_LVL_ID,\ncast('' as STRING) as THRUPUT_SUM_TXT,\ncast('' as STRING) as TXN_ID,\ncast('' as INT) as UNIT_LOST_NBR,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
