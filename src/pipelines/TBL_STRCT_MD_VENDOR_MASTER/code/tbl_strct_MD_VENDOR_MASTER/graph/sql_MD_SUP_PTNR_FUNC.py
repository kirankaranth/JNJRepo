from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_VENDOR_MASTER.config.ConfigStore import *
from tbl_strct_MD_VENDOR_MASTER.udfs.UDFs import *

def sql_MD_SUP_PTNR_FUNC(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SUP_NUM,\ncast('' as string) as PRCHSNG_ORG,\ncast('' as string) as SUP_SUB_RNG,\ncast('' as string) as PLNT_CD,\ncast('' as string) as PTNR_FUNC,\ncast('' as int) as PTNR_CNTR,\ncast('' as string) as PRSN_RESP_FOR_CREAT_OBJ,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as REF_TO_OTH_SUP,\ncast('' as string) as DFLT_PTNR,\ncast('' as int) as PERS_NUM,\ncast('' as int) as NUM_OF_CNTCT_PRSN,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
