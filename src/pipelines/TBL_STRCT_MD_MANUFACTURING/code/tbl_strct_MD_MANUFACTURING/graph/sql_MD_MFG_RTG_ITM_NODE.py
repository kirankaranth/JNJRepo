from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MANUFACTURING.config.ConfigStore import *
from tbl_strct_MD_MANUFACTURING.udfs.UDFs import *

def sql_MD_MFG_RTG_ITM_NODE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as RTG_TYPE_CD,\ncast('' as String) as RTG_GRP_CD,\ncast('' as String) as RTG_GRP_CNTR_NBR,\ncast('' as String) as RTG_SEQ_NUM,\ncast('' as String) as RTG_NODE_NUM,\ncast('' as String) as RTG_NODE_VERS_CNTR_NBR,\ncast('' as timestamp) as VLD_FROM_DTTM,\ncast('' as String) as CHGNUM,\ncast('' as String) as DEL_IND,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1