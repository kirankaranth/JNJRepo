from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_DELIVERIES.config.ConfigStore import *
from tbl_strct_MD_DELIVERIES.udfs.UDFs import *

def sql_MD_SHIP_LINE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SHIP_LINE_NBR,\ncast('' as string) as SHIP_NUM,\ncast('' as string) as DELV_TYP_CD,\ncast('' as string) as DOC_REF_NUM,\ncast('' as string) as CO_CD,\ncast('' as string) as DELV_LINE_NBR,\ncast('' as string) as DELV_NUM,\ncast('' as string) as ITNRY_OF_SHIP_ITM,\ncast('' as string) as NM_PRSN_RESP_CREAT_OBJ,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as string) as PACK_DELV_DEPN_SHIPPING,\ncast('' as string) as IN_DELV_ITM_HUS_GEN,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
