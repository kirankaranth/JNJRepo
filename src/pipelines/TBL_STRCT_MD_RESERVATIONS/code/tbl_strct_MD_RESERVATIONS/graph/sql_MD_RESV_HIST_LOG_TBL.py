from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_RESERVATIONS.config.ConfigStore import *
from tbl_strct_MD_RESERVATIONS.udfs.UDFs import *

def sql_MD_RESV_HIST_LOG_TBL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as EQMNT_NUM,\ncast('' as string) as SLS_ORDR_DOC_ID,\ncast('' as string) as MATL_DOC_NUM,\ncast('' as int) as MATL_DOC_YR,\ncast('' as int) as MATL_LDGR_DOC,\ncast('' as string) as MATL_LDGR_DOC_NUM,\ncast('' as string) as REF_PCDR,\ncast('' as string) as REF_DOC_NUM,\ncast('' as string) as REF_ORG_UNIT,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as string) as CRT_BY,\ncast('' as string) as GOODS_MVMT_STS,\ncast('' as string) as LNSET_ACTV,\ncast('' as string) as SLS_ORDR_TYPE_CD,\ncast('' as string) as SLS_ORDR_TYPE_DESC,\ncast('' as string) as RESV_CD,\ncast('' as decimal(18,4)) as REF_VAL,\ncast('' as string) as CRNCY_CD,\ncast('' as string) as SLORG_NUM,\ncast('' as string) as DELV_DOC_NUM,\ncast('' as string) as CRNCY_KEY_CD,\ncast('' as decimal(18,4)) as PSTNG_AMT_LCL_CRNCY,\ncast('' as string) as ORDR_NUM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
