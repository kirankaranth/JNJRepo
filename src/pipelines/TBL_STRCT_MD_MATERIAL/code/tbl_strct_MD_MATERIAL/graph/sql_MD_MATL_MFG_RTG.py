from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *

def sql_MD_MATL_MFG_RTG(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as MATL_NUM,\ncast('' as String) as PLNT_CD,\ncast('' as String) as RTG_TYPE_CD,\ncast('' as String) as RTG_GRP_CD,\ncast('' as String) as RTG_GRP_CNTR_NBR,\ncast('' as String) as RTG_ADDL_CNTR_NBR,\ncast('' as String) as MATL_RTG_VERS_CNTR_NBR,\ncast('' as timestamp) as VLD_FROM_DTTM,\ncast('' as String) as CHG_NUM,\ncast('' as String) as DEL_IND,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as String) as TECH_STS_FROM,\ncast('' as String) as IN_INACT_CHG,\ncast('' as String) as USER_CRT_REC,\ncast('' as String) as NM_OF_PRSN_CHG_OBJ,\ncast('' as String) as VEND_ACCT_NUM,\ncast('' as String) as ACCT_NUM_CUST,\ncast('' as String) as SRCH_FLD_CUST_SPEC_TASK_LIST_SLCT,\ncast('' as String) as SLS_DOC,\ncast('' as String) as SLS_DOC_ITM,\ncast('' as String) as WRK_BRKDWN_STRC_ELMNT,\ncast('' as timestamp) as VLD_TO_DTTM,\ncast('' as String) as DEL_IN,\ncast('' as String) as RTG_VERS,\ncast('' as String) as SRC_RTG_VERS,\ncast('' as String) as TASK_LIST_VERS,\ncast('' as String) as OBJ_MLT_SPEC,\ncast('' as String) as TYPE_OBJ_MLT_SPEC,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
