from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PURCHASE_ORDER.config.ConfigStore import *
from tbl_strct_MD_PURCHASE_ORDER.udfs.UDFs import *

def sql_MD_PRCH_DELV_CNFRMS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PO_NUM,\ncast('' as string) as PO_LINE_NBR,\ncast('' as int) as CNFRM_SEQ_NBR,\ncast('' as string) as CNFRM_CAT_CD,\ncast('' as timestamp) as DELV_DTTM,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as decimal(18,4)) as CNFRM_QTY,\ncast('' as decimal(18,4)) as MRP_ADJ_QTY,\ncast('' as string) as SLS_ORDR_NUM,\ncast('' as string) as SLS_ORDR_LINE_NBR,\ncast('' as string) as VEND_BTCH_NUM,\ncast('' as string) as REF_DOC_NUM,\ncast('' as string) as CO_CD,\ncast('' as string) as ORDER_SUF,\ncast('' as string) as DT_CAT_OF_DELV_DT_SUP_CNFRM,\ncast('' as string) as CRT_IN_SUP_CNFRM,\ncast('' as string) as SUP_CNFRM_DEL_IN,\ncast('' as string) as CNFRM_RLVNT_TO_MATL_PLNG,\ncast('' as string) as MFR_PART_PRFL,\ncast('' as string) as MATL_NUM_CRSPN_TO_MFR_PART_NUM,\ncast('' as decimal(18,4)) as NUM_OF_RMNDR,\ncast('' as string) as HI_LVL_ITM_OF_BTCH_SPLT_ITM,\ncast('' as string) as SEQ_NUM_OF_SUP_CNFRM,\ncast('' as string) as DELV_HAS_STS_IN_PLNT,\ncast('' as string) as DELV,\ncast('' as string) as DELV_ITM,\ncast('' as timestamp) as HANDOVR_DTTM,\ncast('' as string) as STK_SGMNT,\ncast('' as decimal(18,4)) as UTC_TMST,\ncast('' as decimal(18,4)) as QTY_OF_ORDR_CNFRM,\ncast('' as decimal(18,4)) as MRP_REDUC_QTY,\ncast('' as timestamp) as PER_OF_PERF_STRT_DTTM,\ncast('' as timestamp) as PER_OF_PERF_END_DTTM,\ncast('' as string) as SRVC_PERFMR,\ncast('' as decimal(18,4)) as EXPTD_VAL_OF_OVRL_LMT,\ncast('' as string) as SUP_CNFRM_NUM,\ncast('' as string) as SUP_CNFRM_ITM,\ncast('' as decimal(18,4)) as ALC_STK_QTY,\ncast('' as decimal(18,4)) as ORIG_QTY_OF_SHIPPING_NTF,\ncast('' as string) as REF_UUID_OF_TRSPN_MGMT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
