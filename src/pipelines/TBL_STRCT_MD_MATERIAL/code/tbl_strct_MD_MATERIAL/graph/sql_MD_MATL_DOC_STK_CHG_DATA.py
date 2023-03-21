from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *

def sql_MD_MATL_DOC_STK_CHG_DATA(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SPLT_GUID_PART_1,\ncast('' as string) as SPLT_GUID_PART_2,\ncast('' as string) as SPLT_GUID_PART_3,\ncast('' as string) as SPLT_GUID_PART_4,\ncast('' as string) as SPLT_GUID_PART_5,\ncast('' as string) as SPLT_GUID_PART_6,\ncast('' as string) as REC_TYPE,\ncast('' as string) as CO_CD,\ncast('' as string) as MATL_IN_RSPCT_OF_WHICH_STK_IS_MNG,\ncast('' as string) as PLNT,\ncast('' as string) as STRG_LOC_SI,\ncast('' as string) as BTCH_NUM_SI,\ncast('' as string) as SUP_FOR_SPL_STK,\ncast('' as string) as SLS_ORDR_NUM_OF_VALUT_SLS_ORDR_STK,\ncast('' as string) as SLS_ORDR_ITM_OF_VALUT_SLS_ORDR_STK,\ncast('' as string) as VALUT_SLS_ORDR_STK_WBS_ELMNT,\ncast('' as string) as CUST_FOR_SPL_STK,\ncast('' as string) as SPL_STK_IN,\ncast('' as string) as STK_TYPE_OF_GOODS_MVMT_SI,\ncast('' as string) as ADDL_SUP_FOR_SPL_STK,\ncast('' as string) as FISC_YR_VRNT,\ncast('' as string) as PER_1,\ncast('' as string) as PER_2,\ncast('' as string) as STK_QTY_AND_VAL_NOT_RLVNT_FOR_PRECMPCT,\ncast('' as decimal(18,4)) as VAL_AT_SLS_PRC_FOR_CDS_VIEW_ON_BTCH_LVL,\ncast('' as decimal(18,4)) as VAL_AT_SLS_PRC_FOR_CDS_VIEW_ON_STRGLOC_LVL,\ncast('' as decimal(18,4)) as STK_QTY_FOR_CDS_VIEW_ON_BTCH_LVL,\ncast('' as decimal(18,4)) as STK_QTY_FOR_CDS_VIEW_ON_STRG_LOC_LVL,\ncast('' as decimal(18,4)) as STK_QTY_FOR_CDS_VIEW_ON_BTCH_LVL_PUOM,\ncast('' as decimal(18,4)) as STK_QTY_FOR_CDS_VIEW_ON_STRG_LOC_LVL_PUOM,\ncast('' as string) as VALUT_OF_SPL_STK,\ncast('' as string) as VEND_STK_VALUT_IN,\ncast('' as timestamp) as CRT_ON_BTCH_LVL_DTTM,\ncast('' as timestamp) as CRT_ON_STRG_LOC_LVL_DTTM,\ncast('' as string) as BAS_UOM,\ncast('' as string) as CRNCY_KEY,\ncast('' as decimal(31,14)) as CNSMPTN_QTY,\ncast('' as string) as PAREL_UOM,\ncast('' as decimal(31,14)) as CNSMPTN_QTY_IN_PUOM,\ncast('' as string) as PAREL_UOM_SI,\ncast('' as string) as RSRS_NM_SI,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
