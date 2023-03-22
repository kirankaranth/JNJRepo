from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *

def sql_MD_PRCH_INFO_ORG(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PRCH_INFO_NUM,\ncast('' as string) as PRCHSNG_ORG_NUM,\ncast('' as string) as PRCH_INFO_CAT_CD,\ncast('' as string) as PLNT_CD,\ncast('' as string) as DEL_IND,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as CRT_BY_NM,\ncast('' as string) as PRCHSNG_GRP_NUM,\ncast('' as string) as CRNCY_CD,\ncast('' as decimal(18,4)) as MIN_PO_QTY,\ncast('' as decimal(18,4)) as STD_PO_QTY,\ncast('' as string) as VOL_REBT_IND,\ncast('' as string) as QTY_REBT,\ncast('' as decimal(18,4)) as PLAN_DELV_DAYS,\ncast('' as string) as TAX_CD,\ncast('' as string) as CNFRM_CAT_CD,\ncast('' as string) as PRC_CNTL_IND,\ncast('' as string) as INCOTERM_1_CD,\ncast('' as string) as INCOTERM_2_CD,\ncast('' as string) as PRDTN_VERS_NUM,\ncast('' as decimal(18,4)) as MAX_PO_QTY,\ncast('' as string) as RD_PRFL_CD,\ncast('' as string) as BRAZLIAN_NCM_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
