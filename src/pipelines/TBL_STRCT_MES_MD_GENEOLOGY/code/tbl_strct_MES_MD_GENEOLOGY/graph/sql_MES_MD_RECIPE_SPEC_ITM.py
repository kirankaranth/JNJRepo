from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_GENEOLOGY.config.ConfigStore import *
from tbl_strct_MES_MD_GENEOLOGY.udfs.UDFs import *

def sql_MES_MD_RECIPE_SPEC_ITM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as RECIPE_SPEC_ITM_ID,\ncast('' as STRING) as PROD_RVSN_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as DATA_PT_NM,\ncast('' as STRING) as DATA_TYPE_CD,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as STRING) as ITM_NM,\ncast('' as STRING) as ITM_RVSN_ID,\ncast('' as STRING) as LWR_LMT_VAL,\ncast('' as STRING) as LWR_WARN_LMT_VAL,\ncast('' as STRING) as NOM_VAL,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as RECIPE_CAT_ID,\ncast('' as STRING) as RESL_LWR_LMT_VAL,\ncast('' as STRING) as RESL_LWR_WARN_VAL,\ncast('' as STRING) as RESL_UP_LMT_VAL,\ncast('' as STRING) as RESL_UP_WARN_VAL,\ncast('' as STRING) as UP_LMT_VAL,\ncast('' as STRING) as UP_WARN_LMT_VAL,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
