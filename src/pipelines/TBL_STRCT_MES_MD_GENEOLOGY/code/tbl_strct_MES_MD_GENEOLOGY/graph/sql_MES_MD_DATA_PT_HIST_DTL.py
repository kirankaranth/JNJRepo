from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_GENEOLOGY.config.ConfigStore import *
from tbl_strct_MES_MD_GENEOLOGY.udfs.UDFs import *

def sql_MES_MD_DATA_PT_HIST_DTL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as DATA_PT_HIST_DTL_ID,\ncast('' as STRING) as DATA_CLCT_DEF_ID,\ncast('' as STRING) as DATA_PT_ID,\ncast('' as STRING) as DATA_PT_HIST_ID,\ncast('' as STRING) as ATTR_NM,\ncast('' as STRING) as BOOL_VAL,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as COMT_TXT,\ncast('' as STRING) as DATA_NM,\ncast('' as STRING) as DATA_TYPE_CD,\ncast('' as STRING) as DATA_VAL,\ncast('' as STRING) as DEC_VAL,\ncast('' as STRING) as ENUM_VAL,\ncast('' as STRING) as ETH_QRY_NM,\ncast('' as STRING) as ETH_QRY_TYPE_CD,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as FX_VAL,\ncast('' as STRING) as FLOAT_VAL,\ncast('' as STRING) as HIST_ID,\ncast('' as STRING) as INTGR_VAL,\ncast('' as BOOLEAN) as IS_LMT_OVRD_IND,\ncast('' as BOOLEAN) as IS_LMT_OVRD_ALLW_IND,\ncast('' as BOOLEAN) as IS_LMT_VIOLA_IND,\ncast('' as BOOLEAN) as IS_REQ_IND,\ncast('' as BOOLEAN) as IS_WARN_VIOLA_IND,\ncast('' as STRING) as LWR_LMT_VAL,\ncast('' as STRING) as LWR_WARN_LMT_VAL,\ncast('' as BOOLEAN) as MAP_TO_USER_ATTR_IND,\ncast('' as STRING) as NDO_VAL_ID,\ncast('' as STRING) as NOM_VAL,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as OBJ_TYPE_NM,\ncast('' as STRING) as QRY_NM,\ncast('' as STRING) as QRY_TYPE_CD,\ncast('' as STRING) as RDO_VAL_ID,\ncast('' as TIMESTAMP) as TMST_DTTM,\ncast('' as STRING) as TXN_ID,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UP_LMT_VAL,\ncast('' as STRING) as UP_WARN_LMT_VAL,\ncast('' as BOOLEAN) as VLD_LMT_NA_IND,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
