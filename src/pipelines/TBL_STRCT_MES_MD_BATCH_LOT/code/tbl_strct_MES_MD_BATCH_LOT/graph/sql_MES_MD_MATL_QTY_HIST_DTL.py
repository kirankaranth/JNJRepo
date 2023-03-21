from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_MATL_QTY_HIST_DTL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as MATL_QTY_HIST_DTL_ID,\ncast('' as STRING) as CNTNR_ID,\ncast('' as STRING) as MATL_QTY_HIST_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as BOOLEAN) as ADJ_THRUPUT_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_QTY_TYPE_CD,\ncast('' as STRING) as CHRG_TO_STEP_ID,\ncast('' as INT) as CHLD_LOST_NBR,\ncast('' as BOOLEAN) as CLSE_WHEN_ZERO_IND,\ncast('' as STRING) as COMT_TXT,\ncast('' as BOOLEAN) as CNTNR_CLSE_IND,\ncast('' as BOOLEAN) as CNTNR_DISASSOCIATED_IND,\ncast('' as BOOLEAN) as CNTS_AGNST_PRDTN_IND,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as BOOLEAN) as IS_RLUP_IND,\ncast('' as DECIMAL(18,4)) as LOST_2_QTY,\ncast('' as DECIMAL(18,4)) as LOST_QTY,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as OPR_ID,\ncast('' as INT) as QTY_MULT_NBR,\ncast('' as STRING) as RSN_CD_ID,\ncast('' as BOOLEAN) as REC_ALL_QTY_IND,\ncast('' as STRING) as TXN_ID,\ncast('' as BOOLEAN) as UNIT_CNTNR_IND,\ncast('' as INT) as UNIT_LOST_NBR,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UOM_2_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
