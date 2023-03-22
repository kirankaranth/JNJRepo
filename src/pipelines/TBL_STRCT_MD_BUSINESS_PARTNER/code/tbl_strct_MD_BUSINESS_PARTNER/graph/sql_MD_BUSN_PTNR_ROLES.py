from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BUSINESS_PARTNER.config.ConfigStore import *
from tbl_strct_MD_BUSINESS_PARTNER.udfs.UDFs import *

def sql_MD_BUSN_PTNR_ROLES(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as BUSN_PTNR_NUM,\ncast('' as string) as BUSN_PTNR_ROLE,\ncast('' as string) as BUSN_PTNR_DFRNTN_TYPE_VAL,\ncast('' as timestamp) as BUSN_PTNR_ROLE_VLD_FROM_DTTM,\ncast('' as timestamp) as BUSN_PTNR_ROLE_VLD_TO_DTTM,\ncast('' as string) as BUSN_PTNR_ROLE_TYPE,\ncast('' as string) as AUTH_GRP,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
