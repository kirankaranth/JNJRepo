from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_REMV_HIST_DTL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as INT) as REMV_HIST_DTL_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CMPNT_REMV_HIST_ID,\ncast('' as STRING) as DEST_CNTNR_ID,\ncast('' as STRING) as DEST_LOC_ID,\ncast('' as STRING) as DEST_LOT_NUM,\ncast('' as STRING) as DEST_STK_PT_VAL,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as ISS_ACTLS_HIST_ID,\ncast('' as STRING) as ISS_CNTL_CD,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as BOOLEAN) as OPEN_CLSE_CNTNR_IND,\ncast('' as STRING) as REF_DSGNR_VAL,\ncast('' as STRING) as RMV_RSN_ID,\ncast('' as DECIMAL(18,4)) as REMV_ALL_QTY,\ncast('' as STRING) as REMV_DIFF_RSN_ID,\ncast('' as DECIMAL(18,4)) as RMVD_QTY,\ncast('' as DECIMAL(18,4)) as RMVD_2_QTY,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UOM_2_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
