from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_TRACKING_DATA.config.ConfigStore import *
from tbl_strct_MD_TRACKING_DATA.udfs.UDFs import *

def sql_MD_TCKG_DATA_HDR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as SD_DOC_CAT,\ncast('' as String) as SLS_DSTN_DOC_NUM,\ncast('' as String) as EXP_DELV_CO,\ncast('' as String) as DOC_CAT_HI_LVL_DOC,\ncast('' as String) as HI_LVL_DOC,\ncast('' as String) as TCKG_NUM,\ncast('' as String) as SNDR_WTH_EXP_DELV_CO,\ncast('' as String) as RTG_INFO_XSI_CARR,\ncast('' as String) as EXP_DELV_CO_RTE_NUM,\ncast('' as String) as FRWDR_TFR_PT_ARR,\ncast('' as String) as EXP_DELV_CO_CENT_HUB,\ncast('' as String) as FRWDR_OUTB_DELV_PT,\ncast('' as timestamp) as LAST_UPDT_RTG_INFO_DTTM,\ncast('' as String) as EXP_DELV_CO_SRVC_CD,\ncast('' as String) as EXP_DELV_CO_PROD_CD,\ncast('' as String) as VLBL_DATA_EXISTS,\ncast('' as String) as ERR_IN_EXP_DELV_CO,\ncast('' as String) as PREV_TCKG_STS,\ncast('' as String) as TCKG_STS,\ncast('' as String) as TCKG_EV_TO_BE_PRCS,\ncast('' as String) as TCKG_STS_DTL,\ncast('' as timestamp) as TCKG_IN_UTC_FMT_DTTM,\ncast('' as String) as NM_PRSN_ACPT_PARCEL,\ncast('' as String) as LOC_PARCEL_DELV_ACPT,\ncast('' as String) as URL_WTH_SGNTR_SCRN,\ncast('' as timestamp) as LAST_RQST_TCKG_DTTM,\ncast('' as String) as TIME_ZN,\ncast('' as String) as TCKG_NUM2,\ncast('' as timestamp) as FIL_VAL_DATA_AGE_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
