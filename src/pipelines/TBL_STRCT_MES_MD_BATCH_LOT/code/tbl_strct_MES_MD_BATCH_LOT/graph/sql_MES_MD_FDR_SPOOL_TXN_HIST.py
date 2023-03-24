from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_FDR_SPOOL_TXN_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as FDR_SPOOL_TXN_HIST_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHNL_TEMP_ID,\ncast('' as STRING) as COMT_TXT,\ncast('' as STRING) as DENIER_CD,\ncast('' as STRING) as DENIER_SIZE_VAL,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as FDR_SPOOL_ITM_ID,\ncast('' as STRING) as FDR_SPOOL_TXT,\ncast('' as STRING) as GODET_RPM_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as LUBE_PUMP_RPM_VAL,\ncast('' as INT) as MINS_CNT,\ncast('' as BOOLEAN) as NCR_ON_FDR_SPOOL_IND,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as BOOLEAN) as OP_CMPLT_SENT_IND,\ncast('' as BOOLEAN) as OUTP_GPH_IND,\ncast('' as STRING) as RESERVOIR_TEMP_VAL,\ncast('' as TIMESTAMP) as SPOOL_FIN_DTTM,\ncast('' as TIMESTAMP) as STRT_DTTM,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as WT_VAL,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
