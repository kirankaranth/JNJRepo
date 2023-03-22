from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_VENDOR_MASTER.config.ConfigStore import *
from tbl_strct_MD_VENDOR_MASTER.udfs.UDFs import *

def sql_MD_SUP_CO(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SUP_NUM,\ncast('' as string) as CO_CD,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as PSTNG_BLK_IND,\ncast('' as string) as DEL_IND,\ncast('' as string) as LDGR_ACCT_CD,\ncast('' as string) as PMT_METH_CD,\ncast('' as string) as PMT_BLK_IND,\ncast('' as string) as PMT_TERM_CD,\ncast('' as string) as BLOK_SUP_IND,\ncast('' as string) as OWN_EXPLN_OF_TERM_OF_PMT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
