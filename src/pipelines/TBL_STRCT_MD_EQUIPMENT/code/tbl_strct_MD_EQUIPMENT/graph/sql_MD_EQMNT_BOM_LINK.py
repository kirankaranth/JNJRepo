from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_EQUIPMENT.config.ConfigStore import *
from tbl_strct_MD_EQUIPMENT.udfs.UDFs import *

def sql_MD_EQMNT_BOM_LINK(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as EQMNT_NUM,\ncast('' as String) as PLNT,\ncast('' as String) as BOM_USG,\ncast('' as String) as BOM,\ncast('' as String) as ALT_BOM,\ncast('' as timestamp) as REC_CRT_ON_DTTM,\ncast('' as String) as USER_CRT_REC,\ncast('' as timestamp) as LAST_CHG_ON_DTTM,\ncast('' as String) as NM_PRSN_CHG_OBJ,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
