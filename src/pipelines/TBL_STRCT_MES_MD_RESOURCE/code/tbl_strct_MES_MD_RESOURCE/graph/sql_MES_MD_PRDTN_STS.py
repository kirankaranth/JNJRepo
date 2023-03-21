from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_RESOURCE.config.ConfigStore import *
from tbl_strct_MES_MD_RESOURCE.udfs.UDFs import *

def sql_MES_MD_PRDTN_STS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as PRDTN_STS_ID,\ncast('' as STRING) as MFG_ORDR_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as STRING) as PROD_BASE_ID,\ncast('' as STRING) as RSRS_ID,\ncast('' as BOOLEAN) as AVLBLTY_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as COMT_TXT,\ncast('' as STRING) as CNTNR_ID,\ncast('' as STRING) as DATA_CLCT_DEF_ID,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as BOOLEAN) as IN_PRCS_IND,\ncast('' as STRING) as IS_OEE_LOSS_CAT_CD,\ncast('' as STRING) as IS_TCKG_CNTNR_ID,\ncast('' as TIMESTAMP) as LAST_ACTV_DTTM,\ncast('' as TIMESTAMP) as LAST_ACTV_GMT_DTTM,\ncast('' as STRING) as LAST_REVERSABLE_TXN_ID,\ncast('' as TIMESTAMP) as LAST_STS_CHG_DTTM,\ncast('' as TIMESTAMP) as LAST_STS_CHG_GMT_DTTM,\ncast('' as TIMESTAMP) as LAST_THRUPUT_DTTM,\ncast('' as TIMESTAMP) as LAST_THRUPUT_GMT_DTTM,\ncast('' as STRING) as LOT_ID,\ncast('' as STRING) as MAINT_RQST_ID,\ncast('' as STRING) as MAINT_STS_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as RSN_ID,\ncast('' as STRING) as RSRS_ST_CD,\ncast('' as INT) as RUN_NBR,\ncast('' as STRING) as SETUP_BASE_ID,\ncast('' as STRING) as SETUP_ID,\ncast('' as STRING) as STS_ID,\ncast('' as DECIMAL(18,4)) as TOT_THRUPUT_QTY,\ncast('' as DECIMAL(18,4)) as TOT_THRUPUT_2_QTY,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UOM_2_ID,\ncast('' as STRING) as UPDT_LAST_STS_CHG_DT_CD,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
