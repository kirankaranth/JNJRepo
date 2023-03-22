from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_HANDLING_UNIT.config.ConfigStore import *
from tbl_strct_MD_HANDLING_UNIT.udfs.UDFs import *

def sql_MD_HU_LINE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as HU_NUM,\ncast('' as String) as HU_LINE_NBR,\ncast('' as String) as DELV_NUM,\ncast('' as String) as DELV_LINE_NBR,\ncast('' as String) as DELV_TYP_CD,\ncast('' as decimal(18,4)) as HU_PACK_QTY,\ncast('' as String) as MATL_NUM,\ncast('' as String) as BTCH_NUM,\ncast('' as String) as PLNT_CD,\ncast('' as String) as SLOC_CD,\ncast('' as timestamp) as EXP_DTTM,\ncast('' as timestamp) as GR_DTTM,\ncast('' as String) as BASE_UOM_CD,\ncast('' as String) as SPL_STK_TYPE_CD,\ncast('' as int) as SER_NUM_CNT,\ncast('' as String) as SER_TYPE_CD,\ncast('' as String) as SUB_HU_NUM,\ncast('' as String) as CO_CD,\ncast('' as String) as ALT_UM_STK_UM,\ncast('' as String) as INSP_LOT_NUM,\ncast('' as String) as SLS_DOC_ITM_CAT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
