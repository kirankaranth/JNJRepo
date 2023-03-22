from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BILL_OF_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_BILL_OF_MATERIAL.udfs.UDFs import *

def sql_MD_BOM_ITM_NODE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as BOM_CAT_CD,\ncast('' as string) as BOM_NUM,\ncast('' as string) as ALT_BOM_NUM,\ncast('' as string) as BOM_ITM_NODE_NUM,\ncast('' as string) as BOM_ITM_NODE_CNTR_NBR,\ncast('' as timestamp) as BOM_ITM_VLD_FROM_DTTM,\ncast('' as string) as CHG_NUM,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as string) as DEL_IND,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
