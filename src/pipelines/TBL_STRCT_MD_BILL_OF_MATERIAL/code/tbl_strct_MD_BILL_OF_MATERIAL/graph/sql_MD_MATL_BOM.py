from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_BILL_OF_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_BILL_OF_MATERIAL.udfs.UDFs import *

def sql_MD_MATL_BOM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as BOM_USG_CD,\ncast('' as string) as BOM_NUM,\ncast('' as string) as ALT_BOM_NUM,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as decimal (18,4)) as FROM_LOT_SIZE_QTY,\ncast('' as decimal (18,4)) as TO_LOT_SIZE_QTY,\ncast('' as string) as USER_WHO_CRT_REC,\ncast('' as string) as NM_OF_PRSN_WHO_CHG_OBJ,\ncast('' as string) as CNFG_MATL_IN,\ncast('' as string) as MATL_BOM_CONCAT_KEY,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
