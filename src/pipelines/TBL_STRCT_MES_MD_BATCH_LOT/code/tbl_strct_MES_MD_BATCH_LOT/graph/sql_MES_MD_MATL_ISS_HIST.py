from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_MATL_ISS_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as MATL_ISS_HIST_ID,\ncast('' as STRING) as FROM_CNTNR_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as STRING) as RSRS_ID,\ncast('' as STRING) as TO_CNTNR_ID,\ncast('' as STRING) as BIN_NUM,\ncast('' as INT) as CHG_CNT,\ncast('' as TIMESTAMP) as EST_DTTM,\ncast('' as TIMESTAMP) as EXP_DTTM,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as FROM_BTCH_ID,\ncast('' as STRING) as FROM_CARR_ID,\ncast('' as STRING) as FROM_LOC_ID,\ncast('' as STRING) as FROM_LOT_ID,\ncast('' as STRING) as FROM_STK_PT_ID,\ncast('' as STRING) as HIST_ID,\ncast('' as BOOLEAN) as IS_AUTO_WT_ENT_IND,\ncast('' as STRING) as IS_MATL_QUE_DTL_ID,\ncast('' as STRING) as ISS_DIFF_RSN_ID,\ncast('' as DECIMAL(18,4)) as ISS_QTY,\ncast('' as DECIMAL(18,4)) as ISS_2_QTY,\ncast('' as STRING) as ISS_RSN_ID,\ncast('' as BOOLEAN) as MAN_OVRD_IND,\ncast('' as STRING) as MATL_ISS_HIST_DTL_ID,\ncast('' as STRING) as NOTES_TXT,\ncast('' as INT) as OBJ_TYPE_ID,\ncast('' as STRING) as PROD_DESC,\ncast('' as STRING) as REF_DSGNR_VAL,\ncast('' as STRING) as SCALE_ID,\ncast('' as STRING) as STRG_TYPE_CD,\ncast('' as STRING) as SUBST_RSN_ID,\ncast('' as STRING) as TARE_WT_VAL,\ncast('' as STRING) as TO_LOC_ID,\ncast('' as BOOLEAN) as TLRNC_OVRD_IND,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as VEND_ITM_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
