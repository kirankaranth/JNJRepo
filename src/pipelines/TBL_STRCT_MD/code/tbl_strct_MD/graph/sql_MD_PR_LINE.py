from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *

def sql_MD_PR_LINE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PR_NUM,\ncast('' as int) as PR_LINE_NBR,\ncast('' as string) as DEL_IND,\ncast('' as string) as REC_CRT_IND,\ncast('' as string) as CRT_BY_NM,\ncast('' as timestamp) as CHNG_ON_DTTM,\ncast('' as string) as PRCH_INFO_DESC,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as SLOC_CD,\ncast('' as string) as INTRNL_REF_NUM,\ncast('' as string) as MATL_GRP_CD,\ncast('' as string) as SUPL_PLNT_CD,\ncast('' as decimal(18,4)) as PR_LINE_QTY,\ncast('' as string) as PR_LINE_UOM_CD,\ncast('' as timestamp) as NEED_BY_DTTM,\ncast('' as timestamp) as APPR_BY_DTTM,\ncast('' as decimal(18,4)) as GR_LEAD_TIME_DAYS,\ncast('' as string) as PR_LINE_CAT_CD,\ncast('' as string) as ACCT_ASGNMT_CAT_CD,\ncast('' as string) as FX_VND,\ncast('' as string) as PRCHSNG_ORG_NUM,\ncast('' as string) as ASGN_SUPL_SRC_IND,\ncast('' as string) as PR_MRP_HRZN,\ncast('' as string) as BOM_NUM,\ncast('' as string) as PR_CLSE_IND,\ncast('' as string) as SPL_STK_IND,\ncast('' as string) as FX_IND,\ncast('' as string) as DELV_ADDR_NUM,\ncast('' as string) as SUP_NUM,\ncast('' as string) as MFR_PART_NUM,\ncast('' as string) as LINE_STS_CD,\ncast('' as decimal(18,4)) as PO_LINE_QUANTITY,\ncast('' as decimal(18,4)) as PR_LINE_PRICE,\ncast('' as decimal(18,4)) as PR_LINE_PRICE_UNIT,\ncast('' as string) as BLOK_IND,\ncast('' as string) as PO_UOM,\ncast('' as timestamp) as PO_DTTM,\ncast('' as string) as PO_NUMBER,\ncast('' as int) as PO_LINE_NUMBER,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
