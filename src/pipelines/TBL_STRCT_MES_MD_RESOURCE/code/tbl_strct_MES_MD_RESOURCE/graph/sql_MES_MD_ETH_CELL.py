from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_RESOURCE.config.ConfigStore import *
from tbl_strct_MES_MD_RESOURCE.udfs.UDFs import *

def sql_MES_MD_ETH_CELL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as ETH_CELL_ID,\ncast('' as STRING) as ASSCTD_PKGS_TXT,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_HIST_ID,\ncast('' as STRING) as ETH_CELL_NM,\ncast('' as STRING) as ETH_CELL_DESC,\ncast('' as STRING) as ICON_ID,\ncast('' as BOOLEAN) as INST_LOCK_IND,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
