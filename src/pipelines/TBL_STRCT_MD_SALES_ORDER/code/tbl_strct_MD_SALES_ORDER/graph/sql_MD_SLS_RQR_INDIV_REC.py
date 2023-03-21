from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_SALES_ORDER.config.ConfigStore import *
from tbl_strct_MD_SALES_ORDER.udfs.UDFs import *

def sql_MD_SLS_RQR_INDIV_REC(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SLS_DOC,\ncast('' as string) as SLS_DOC_ITM,\ncast('' as string) as SCHED_LINE_NUM,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT,\ncast('' as string) as MRP_AREA,\ncast('' as timestamp) as MATL_STGNG_AVLBLTY_DTTM,\ncast('' as string) as STRG_LOC,\ncast('' as string) as BTCH_NUM,\ncast('' as string) as SD_DOC_CAT,\ncast('' as string) as REQ_TYPE,\ncast('' as string) as PLNG_TYPE,\ncast('' as decimal(18,4)) as OPEN_QTY_SKU_TFR_RQR_MRP,\ncast('' as decimal(18,4)) as CNFRM_QTY_AVLBLTY_CHK_SKU,\ncast('' as string) as BASE_UNIT_MEAS,\ncast('' as string) as BUSN_DOC_NUM,\ncast('' as string) as BUSN_ITM_NUM,\ncast('' as string) as SCHED_LINE,\ncast('' as string) as ORDR_PRBLTY_ITM,\ncast('' as string) as SLS_DOC_TYPE,\ncast('' as string) as OLD_PROJ_NUM_NO_LONG_USED_PS_POSNR,\ncast('' as string) as SOLD_TO_PRTY,\ncast('' as string) as RQR_REC_NOT_RLVNT_MRP,\ncast('' as string) as ALLC_IN,\ncast('' as string) as PLNG_MATL,\ncast('' as string) as PLNG_PLNT,\ncast('' as string) as BASE_UNIT_MEAS_PROD_GRP,\ncast('' as decimal(18,4)) as CONV_FACT_QTY,\ncast('' as decimal(18,4)) as PLAN_ALLC_QTY_INDP_RQR,\ncast('' as string) as ACCT_ASGNMT_CAT,\ncast('' as string) as SPL_STK_IN,\ncast('' as string) as CNSMPTN_PSTNG,\ncast('' as string) as BOM_EXPLS_NUM,\ncast('' as string) as WBS_ELMNT,\ncast('' as string) as PLNG_IN,\ncast('' as string) as CNFG,\ncast('' as string) as IN_ASBL_ORDR_PCDR,\ncast('' as string) as VALUT_SPL_STK,\ncast('' as string) as PARM_VRNT_STD_VRNT,\ncast('' as string) as REQ_SGMNT,\ncast('' as decimal(18,4)) as ARUN_REQ_ALC_QTY,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
