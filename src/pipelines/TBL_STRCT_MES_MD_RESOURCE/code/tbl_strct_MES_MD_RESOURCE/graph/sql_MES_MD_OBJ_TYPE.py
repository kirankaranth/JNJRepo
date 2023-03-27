from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_RESOURCE.config.ConfigStore import *
from tbl_strct_MES_MD_RESOURCE.udfs.UDFs import *

def sql_MES_MD_OBJ_TYPE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as DFLT_TBL_ID,\ncast('' as STRING) as DSPLY_NM_LBL_ID,\ncast('' as BOOLEAN) as ENFRC_INTEG_IND,\ncast('' as STRING) as FEAT_ID,\ncast('' as STRING) as INHERIT_MASK_CD,\ncast('' as BOOLEAN) as IS_ABSTR_IND,\ncast('' as BOOLEAN) as IS_EXPO_TO_CLNT_UI_IND,\ncast('' as BOOLEAN) as IS_WS_EXPO_STD_IND,\ncast('' as STRING) as MAINT_TYPE_ID,\ncast('' as STRING) as OBJ_DESC,\ncast('' as STRING) as OBJ_NM,\ncast('' as STRING) as OBJ_USG_MASK_CD,\ncast('' as STRING) as OBJS_TO_CACHE_TXT,\ncast('' as STRING) as PARNT_OBJ_TYPE_ID,\ncast('' as BOOLEAN) as READ_OVRD_IND,\ncast('' as STRING) as RVSN_TYPE_ID,\ncast('' as STRING) as SCTY_TYPE_ID,\ncast('' as STRING) as SEL_VAL_QRY_DEF_ID,\ncast('' as STRING) as STRG_CAT_ID,\ncast('' as STRING) as UI_DTLS_ID,\ncast('' as BOOLEAN) as USE_INST_SCTY_IND,\ncast('' as STRING) as VIS_CD,\ncast('' as STRING) as WRKSPC_CD,\ncast('' as BOOLEAN) as WRITE_OVRD_IND,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
