from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *

def sql_MD_STRG_BINS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as WHSE_NUM,\ncast('' as string) as STRG_TYPE,\ncast('' as string) as STRG_BIN,\ncast('' as string) as STRG_SECTN,\ncast('' as string) as STRG_BIN_TYPE,\ncast('' as string) as SECTN_OF_STRG_BIN,\ncast('' as string) as BLOK_IN_STK_RMV_USER,\ncast('' as string) as BLOK_IN_PUTAWAYS_USER,\ncast('' as string) as BLOK_IN_CUR_STK_RMV_SYS,\ncast('' as string) as BLOK_IN_CUR_STK_PLCMT_SYS,\ncast('' as string) as BLOK_IN_CUR_INV_SYS,\ncast('' as string) as BLOK_RSN,\ncast('' as decimal(18,4)) as QUANTS_CNT,\ncast('' as decimal(18,4)) as QUANTS_CNT_MAX,\ncast('' as decimal(18,4)) as STRG_UNIT_CNT,\ncast('' as decimal(18,4)) as STRG_UNIT_CNT_MAX,\ncast('' as string) as DYN_IN,\ncast('' as decimal(18,4)) as LD_CAPY,\ncast('' as string) as WT_UNIT,\ncast('' as decimal(18,4)) as WT_OF_MATL_IN_STRG_BIN,\ncast('' as timestamp) as LAST_MVMT_DTTM,\ncast('' as string) as TFR_ORDR_NUM_OF_STK_TFR,\ncast('' as string) as ITM_IN_TFR_ORDR,\ncast('' as timestamp) as LAST_BIN_CLRNG_DTTM,\ncast('' as string) as INV_METH,\ncast('' as timestamp) as LAST_INV_DTTM,\ncast('' as string) as NUM_OF_SYS__REC,\ncast('' as string) as ITM_NUM_IN_INV_DOC,\ncast('' as string) as INV_IN_PREP,\ncast('' as string) as EMPTY_IN,\ncast('' as string) as FULL_IN,\ncast('' as string) as SRT_FLD_CROSS_LINE,\ncast('' as string) as FIRE_CONT_SECTN,\ncast('' as string) as LAST_CHG_BY,\ncast('' as timestamp) as LAST_CHG_DTTM,\ncast('' as decimal(18,4)) as TOT_CAPY_OF_STRG_BIN,\ncast('' as decimal(18,4)) as AVAIL_CAPY_IN_STRG_BIN,\ncast('' as string) as PICK_AREA,\ncast('' as string) as SRT_FLD_FOR_STRG_BIN,\ncast('' as string) as VERIF_FLD_FOR_MOBL_DATA_ENT,\ncast('' as string) as ZN,\ncast('' as decimal(18,4)) as X_COORD,\ncast('' as decimal(18,4)) as Y_COORD,\ncast('' as decimal(18,4)) as Z_COORD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
