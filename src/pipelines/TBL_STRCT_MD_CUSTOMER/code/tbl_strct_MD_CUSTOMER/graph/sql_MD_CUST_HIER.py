from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def sql_MD_CUST_HIER(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CUST_HIER_TYPE,\ncast('' as string) as NM,\ncast('' as string) as CUST_HI_HIER,\ncast('' as string) as CUST,\ncast('' as timestamp) as STRT_VLD_DTTM,\ncast('' as timestamp) as END_VLD_DTTM,\ncast('' as string) as SLORG,\ncast('' as string) as DSTR_CHNL,\ncast('' as string) as DIV,\ncast('' as string) as HI_LVL_SLS_ORG,\ncast('' as string) as HI_LVL_DSTN_CHNL,\ncast('' as string) as HI_LVL_DIV,\ncast('' as string) as NUM_RTN_USED_COPY,\ncast('' as string) as IN_CUST_RBT_RLVNT,\ncast('' as string) as RLVNT_PRC_DTRMN_ID,\ncast('' as string) as ASGNMT_HIER,\ncast('' as string) as GUID_CUST_HIER_NODE,\ncast('' as string) as ID_CUST_HIER_NODE,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
