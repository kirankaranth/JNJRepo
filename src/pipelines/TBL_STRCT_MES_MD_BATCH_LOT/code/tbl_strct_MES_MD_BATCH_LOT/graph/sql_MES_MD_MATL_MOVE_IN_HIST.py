from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_MATL_MOVE_IN_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as MATL_MOVE_IN_HIST_ID,\ncast('' as STRING) as HIST_MNLINE_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as INT) as CHILD_CNT,\ncast('' as STRING) as CYC_TIME_VAL,\ncast('' as STRING) as ELAP_TIME_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as LOC_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as OWN_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as DECIMAL(18,4)) as MOVE_QTY,\ncast('' as DECIMAL(18,4)) as MOVE_2_QTY,\ncast('' as STRING) as RUN_IND,\ncast('' as STRING) as SETUP_ID,\ncast('' as STRING) as TXN_ID,\ncast('' as INT) as UNIT_CNT,\ncast('' as STRING) as UOM_2_ID,\ncast('' as STRING) as UOM_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
