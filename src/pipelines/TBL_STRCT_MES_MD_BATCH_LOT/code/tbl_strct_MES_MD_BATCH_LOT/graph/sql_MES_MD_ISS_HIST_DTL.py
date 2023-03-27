from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_ISS_HIST_DTL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as ISS_HIST_DTL_ID,\ncast('' as STRING) as CMPNT_ISS_HIST_ID,\ncast('' as STRING) as ISS_DIFF_RSN_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as DECIMAL(18,4)) as ACTL_ISS_QTY,\ncast('' as DECIMAL(18,4)) as ACTL_ISS_2_QTY,\ncast('' as BOOLEAN) as ALLW_MAN_WEIGH_OVRD_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as BOOLEAN) as CLSE_WHEN_EMPTY_IND,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as FROM_CNTNR_CONV_FACT_VAL,\ncast('' as STRING) as FROM_CNTNR_CNV_ACTL_VAL,\ncast('' as STRING) as FROM_CNTNR_UOM_ID,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as INTRM_CONV_FACT_VAL,\ncast('' as STRING) as INTRM_ID,\ncast('' as DECIMAL(18,4)) as INTRM_PLAN_QTY,\ncast('' as STRING) as INTRM_PLAN_UOM_ID,\ncast('' as BOOLEAN) as IS_MAN_READ_ONLY_IND,\ncast('' as BOOLEAN) as IS_QTY_ADDTV_IND,\ncast('' as STRING) as ISS_CNTL_CD,\ncast('' as DECIMAL(18,4)) as LWR_LMT_QTY,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as PROD_MATL_LIST_ITM_ID,\ncast('' as STRING) as REF_DSGNR_VAL,\ncast('' as DECIMAL(18,4)) as REQ_QTY,\ncast('' as DECIMAL(18,4)) as REQ_2_QTY,\ncast('' as STRING) as TO_CNTNR_CONV_FACT_VAL,\ncast('' as STRING) as TO_CNTNR_CNV_ACTL_VAL,\ncast('' as STRING) as TO_CNTNR_UOM_ID,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UOM_2_ID,\ncast('' as DECIMAL(18,4)) as UP_LMT_QTY,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
