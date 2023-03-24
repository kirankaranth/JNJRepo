from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SALES_DOCUMENT_HIERARCHY.config.ConfigStore import *
from tbl_strct_MD_SALES_DOCUMENT_HIERARCHY.udfs.UDFs import *

def sql_MD_SLS_DOC_HIER(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as CO_CD,\ncast('' as string) as PREV_DOC_NUM,\ncast('' as string) as PREV_DOC_LINE_NBR,\ncast('' as string) as SUBSQ_DOC_NUM,\ncast('' as string) as SUBSQ_DOC_LINE_NBR,\ncast('' as string) as SUBSQ_DOC_CAT_CD,\ncast('' as string) as PREV_DOC_TYPE_CD,\ncast('' as string) as PREV_DOC_CAT_CD,\ncast('' as decimal(18,4)) as REF_QTY,\ncast('' as string) as BASE_UOM_CD,\ncast('' as string) as REF_AMT,\ncast('' as string) as CRNCY_CD,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as string) as MATL_NUM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as string) as REQ_TYPE_CD,\ncast('' as string) as PLNG_TYPE_CD,\ncast('' as string) as LVL_CD,\ncast('' as string) as WHSE_CD,\ncast('' as string) as BILL_CAT_CD,\ncast('' as decimal(18,4)) as GRS_WT_MEAS,\ncast('' as string) as WT_UOM_CD,\ncast('' as decimal(18,4)) as VOL_MEAS,\ncast('' as string) as VOL_UOM_CD,\ncast('' as string) as SLS_UOM_CD,\ncast('' as string) as SPL_STK_TYPE_CD,\ncast('' as string) as SPL_STK_NUM,\ncast('' as decimal(18,4)) as NET_WT_MEAS,\ncast('' as string) as GM_STS_CD,\ncast('' as string) as QTY_CONV_CD,\ncast('' as string) as MATL_MVMT_YR,\ncast('' as string) as SD_UNIQ_DOC_RL_ID,\ncast('' as string) as QTY_CALC_POS_NGTV,\ncast('' as string) as ID_MM_WM_TFR_ORDR_CNFRM,\ncast('' as string) as MVMT_TYPE,\ncast('' as string) as BILL_INVC_PLAN_NUM,\ncast('' as string) as ITM_FOR_BILL_INVC_PLAN_PMT_CRD,\ncast('' as decimal(18,4)) as REF_QTY_SLS_UNIT,\ncast('' as decimal(18,4)) as REF_QTY_BASE_UNIT_MEAS,\ncast('' as decimal(18,4)) as GUARNT,\ncast('' as string) as IN_INV_MGMT_ACT,\ncast('' as string) as LOGL_SYS,\ncast('' as timestamp) as DATA_FIL_VAL_DATA_AGE_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
