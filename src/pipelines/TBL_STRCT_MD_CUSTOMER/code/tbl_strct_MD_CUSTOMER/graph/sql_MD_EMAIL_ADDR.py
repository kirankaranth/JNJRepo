from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_CUSTOMER.config.ConfigStore import *
from tbl_strct_MD_CUSTOMER.udfs.UDFs import *

def sql_MD_EMAIL_ADDR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as ADDR_NUM,\ncast('' as String) as PRSN_NUM,\ncast('' as timestamp) as VLD_FROM_DTTM,\ncast('' as String) as SEQ_NUM,\ncast('' as String) as FL_DFLT_ADDR,\ncast('' as String) as FL_COMM_NUM_NOT_USED,\ncast('' as String) as RCPNT_ADDR_COMM_TYPE,\ncast('' as String) as EMAIL_ADDR,\ncast('' as String) as EMAIL_ADDR_SRCH_FLD,\ncast('' as String) as FL_RCPNT_STD_RCPNT_ADDR,\ncast('' as String) as FL_CNCT_SAP_SYS,\ncast('' as String) as REQ_DATA_ENCD,\ncast('' as String) as FL_RCVR_RECV_TNEF_ENCD_SMTP,\ncast('' as timestamp) as COMM_DATA_VLD_FROM_DTTM,\ncast('' as timestamp) as COMM_DATA_VLD_TO_DTTM,\ncast('' as timestamp) as DATA_FIL_VAL_DATA_AGE_DTTM,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
