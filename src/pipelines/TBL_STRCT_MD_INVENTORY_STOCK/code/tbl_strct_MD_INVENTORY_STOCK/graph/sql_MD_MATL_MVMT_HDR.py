from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_INVENTORY_STOCK.config.ConfigStore import *
from tbl_strct_MD_INVENTORY_STOCK.udfs.UDFs import *

def sql_MD_MATL_MVMT_HDR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as MATL_MVMT_NUM,\ncast('' as string) as MATL_MVMT_YR,\ncast('' as string) as EV_TYPE_CD,\ncast('' as timestamp) as MATL_MVMT_DTTM,\ncast('' as timestamp) as PSTNG_DTTM,\ncast('' as string) as MATL_MVMT_HDR_TXT,\ncast('' as string) as DOC_TYPE,\ncast('' as string) as DOC_TYPE_DESC,\ncast('' as string) as BILL_OF_LANDG,\ncast('' as timestamp) as LAST_CHG_DTTM,\ncast('' as string) as MATL_MVMT_ITM_NUM,\ncast('' as string) as SPLT_GUID_PART1,\ncast('' as string) as SPLT_GUID_PART2,\ncast('' as string) as SPLT_GUID_PART3,\ncast('' as string) as SPLT_GUID_PART4,\ncast('' as string) as SPLT_GUID_PART5,\ncast('' as string) as SPLT_GUID_PART6,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
