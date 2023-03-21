from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *

def sql_MD_MATL_LOC_QUAL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as INSPECTION_TYPE_CD,\ncast('' as decimal(18,4)) as AVG_INSP_DUR_DAYS_QTY,\ncast('' as string) as INSP_STK_PSTD_IND,\ncast('' as string) as AUTO_USG_DCSN_IND,\ncast('' as string) as HU_INSP_IND,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
