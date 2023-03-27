from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_SITE_PLANT.config.ConfigStore import *
from tbl_strct_MES_MD_SITE_PLANT.udfs.UDFs import *

def sql_MES_MD_ENTRP(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as ENTRP_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_STS_ID,\ncast('' as STRING) as ENTRP_NM,\ncast('' as STRING) as ENTRP_DESC,\ncast('' as STRING) as ICON_ID,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as STRING) as LIVE_CONN_PROXY_SITE_ID,\ncast('' as STRING) as NOTES_TXT,\ncast('' as INT) as OBJ_TYPE_ID,\ncast('' as STRING) as RPT_HD_TXT,\ncast('' as STRING) as TRAIN_REQ_GRP_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
