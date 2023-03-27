from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_COLL_SAMP_SPOOL_HIS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as COLL_SAMP_SPOOL_HIST_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as INT) as AMBNT_WRAPS_CNT,\ncast('' as STRING) as ANNEALING_TEMP_VAL,\ncast('' as INT) as ANNEALING_WRAPS_CNT,\ncast('' as INT) as CHG_CNT,\ncast('' as BOOLEAN) as CLEAN_IND,\ncast('' as STRING) as COMT_TXT,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as EXTRUDER_ID,\ncast('' as STRING) as FDR_SPOOL_ID,\ncast('' as STRING) as FPM_AMBNT_SPEED_VAL,\ncast('' as STRING) as FPM_ANNEALING_SPEED_VAL,\ncast('' as STRING) as FPM_ORNTN_SPEED_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as LINE_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as BOOLEAN) as ORNTN_BRKS_IND,\ncast('' as STRING) as ORNTN_LINE_NUM,\ncast('' as STRING) as ORNTN_TEMP_VAL,\ncast('' as INT) as ORNTN_WRAPS_CNT,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as USRID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
