from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *

def sql_MD_PR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PR_NUM,\ncast('' as int) as PR_LINE_NBR,\ncast('' as string) as PR_TYPE_CD,\ncast('' as string) as PR_CAT_CD,\ncast('' as string) as PR_STS_CD,\ncast('' as string) as PRCHSNG_GRP_NUM,\ncast('' as timestamp) as PR_RQST_DTTM,\ncast('' as string) as PO_TYPE_CD,\ncast('' as string) as ERP_VENDOR_NUMBER,\ncast('' as string) as RELEASE_INDICATOR,\ncast('' as string) as RELEASE_STATUS_CODE,\ncast('' as string) as PR_NOT_YET_COMPLETE,\ncast('' as string) as PR_GOODS_RECEIPT_IND,\ncast('' as string) as PR_INVOICE_RECEIPT_IND,\ncast('' as string) as PR_PARTIAL_INVOICE_IND,\ncast('' as decimal(18,4)) as PR_RELEASE_VALUE_AMOUNT,\ncast('' as string) as DOCUMENT_CURRENCY_CODE,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
