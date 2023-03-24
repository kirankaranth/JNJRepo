from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_VENDOR_MASTER.config.ConfigStore import *
from tbl_strct_MD_VENDOR_MASTER.udfs.UDFs import *

def sql_MD_SUP_PRCHSNG_ORG(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SUP_NUM,\ncast('' as string) as PRCHSNG_ORG_NUM,\ncast('' as timestamp) as CRT_ON_DTTM,\ncast('' as string) as PRCH_BLK_IND,\ncast('' as string) as DEL_IND,\ncast('' as string) as CRNCY_CD,\ncast('' as string) as PMT_TERM_CD,\ncast('' as string) as INCOTERM1_CD,\ncast('' as string) as INCOTERM2_CD,\ncast('' as string) as PRC_PCDR_CD,\ncast('' as string) as PRC_CNTL_CD,\ncast('' as string) as EVAL_RCPT_SETLM_CD,\ncast('' as string) as RTRN_VEND_IND,\ncast('' as string) as CNFRM_CD,\ncast('' as string) as NM_OF_PRSN_RESP_CREAT_OBJ,\ncast('' as string) as GR_BAS_INVC_VERIF,\ncast('' as string) as AUTO_GNR_OF_PO_ALLW,\ncast('' as string) as AUTO_EVAL_RCPT_SETLM,\ncast('' as string) as OWN_EXPLN_OF_TERM_OF_PMT,\ncast('' as string) as SHIPPING_COND_CD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
