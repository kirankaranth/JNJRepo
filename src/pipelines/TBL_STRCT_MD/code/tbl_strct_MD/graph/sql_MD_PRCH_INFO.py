from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD.config.ConfigStore import *
from tbl_strct_MD.udfs.UDFs import *

def sql_MD_PRCH_INFO(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PRCH_INFO_NUM,\ncast('' as string) as MATL_NUM,\ncast('' as string) as MATL_GRP_CD,\ncast('' as string) as SUP_NUM,\ncast('' as string) as DEL_IND,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as CRT_BY_NM,\ncast('' as string) as PRCH_INFO_DESC,\ncast('' as string) as PO_UOM_CD,\ncast('' as string) as MFR_PART_NUM,\ncast('' as string) as MFR_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
