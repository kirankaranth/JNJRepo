from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SOURCELIST.config.ConfigStore import *
from tbl_strct_MD_SOURCELIST.udfs.UDFs import *

def sql_MD_MATL_LOC_SRC_LIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as int) as SRC_LIST_REC_NBR,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as CRT_BY_NM,\ncast('' as timestamp) as VLD_FROM_DTTM,\ncast('' as timestamp) as VLD_TO_DTTM,\ncast('' as string) as SUP_PRTY_ID,\ncast('' as string) as PRE_SUP_IND,\ncast('' as string) as PUR_DOC_ID,\ncast('' as int) as PUR_LINE_NBR,\ncast('' as string) as PRE_AGMT_ITM_IND,\ncast('' as string) as PLA_PRTY_ID,\ncast('' as string) as MAN_PART_NUMBER,\ncast('' as string) as BLO_SRC_IND,\ncast('' as string) as PUR_ORG_PRTY_ID,\ncast('' as string) as PO_CAT_TYPE_CD,\ncast('' as string) as SOU_LIST_CAT_CD,\ncast('' as string) as PLA_RLVNT_IND,\ncast('' as string) as GOOD_RCPT_ROUTE_CD,\ncast('' as string) as GOODS_SUP_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
