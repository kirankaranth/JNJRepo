from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_LEGAL_ENTITY.config.ConfigStore import *
from tbl_strct_MD_LEGAL_ENTITY.udfs.UDFs import *

def sql_MD_LE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as LE_CD,\ncast('' as string) as LE_TYPE_IND,\ncast('' as string) as LE_NM,\ncast('' as string) as CHRT_OF_ACCT_CD,\ncast('' as string) as LE_CITY_CD,\ncast('' as string) as VAT_REGS_NUM,\ncast('' as string) as MAX_EXCH_RT_DEVT,\ncast('' as string) as ADDR_CD,\ncast('' as string) as JURIS_CD,\ncast('' as string) as FISC_YR_VRNT_CD,\ncast('' as string) as LE_CRNCY_CD,\ncast('' as string) as LE_CTRY_CD,\ncast('' as string) as LANG_CD,\ncast('' as string) as CNTL_AREA_CD,\ncast('' as string) as CO,\ncast('' as string) as INP_TAX_CD,\ncast('' as string) as OUTP_TAX_CD,\ncast('' as string) as CTRY_CHRT_OF_ACCT,\ncast('' as string) as HYPER_INFLT_IND,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
