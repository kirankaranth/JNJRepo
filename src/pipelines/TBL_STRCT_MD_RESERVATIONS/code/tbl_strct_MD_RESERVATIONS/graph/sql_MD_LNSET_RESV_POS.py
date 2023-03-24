from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_RESERVATIONS.config.ConfigStore import *
from tbl_strct_MD_RESERVATIONS.udfs.UDFs import *

def sql_MD_LNSET_RESV_POS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as RESV_CD,\ncast('' as string) as RESV_POS,\ncast('' as string) as MATL_NUM,\ncast('' as decimal(18,4)) as GOODS_ISS_QTY,\ncast('' as decimal(18,4)) as GOODS_RECV_QTY,\ncast('' as decimal(18,4)) as BILL_TO_QTY,\ncast('' as string) as BASE_UOM,\ncast('' as string) as BTCH_NUM,\ncast('' as decimal(18,4)) as FRWD_QTY,\ncast('' as decimal(18,4)) as INTERM_DOC_QTY,\ncast('' as string) as SER_NUM,\ncast('' as string) as ADD_FL,\ncast('' as decimal(18,4)) as MSNG_QTY,\ncast('' as decimal(18,4)) as DMGD_QTY,\ncast('' as string) as RESV_REF,\ncast('' as string) as POS_RESV_REF,\ncast('' as string) as TRAY_CD,\ncast('' as string) as GOODS_ISS_SLOC,\ncast('' as string) as OTGO_GOODS_WRK,\ncast('' as timestamp) as ACTL_RTN_DTTM,\ncast('' as string) as BILL_ORDR,\ncast('' as string) as USG_CD,\ncast('' as string) as MAINT_PLNT_CD,\ncast('' as string) as TECH_OBJ_TYPE,\ncast('' as string) as DMGD_CD,\ncast('' as string) as ITM_XFER,\ncast('' as decimal(18,4)) as TFR_QTY,\ncast('' as string) as SUP_MATL_NUM,\ncast('' as string) as CUST_DATA_FLD_01,\ncast('' as string) as SPL_STK_IN,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
