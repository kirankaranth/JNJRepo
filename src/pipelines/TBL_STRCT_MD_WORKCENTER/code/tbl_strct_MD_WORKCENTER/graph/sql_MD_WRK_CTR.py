from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_WORKCENTER.config.ConfigStore import *
from tbl_strct_MD_WORKCENTER.udfs.UDFs import *

def sql_MD_WRK_CTR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as WRK_CTR_TYPE_CD,\ncast('' as string) as WRK_CTR_NUM,\ncast('' as timestamp) as STRT_DTTM,\ncast('' as timestamp) as END_DTTM,\ncast('' as string) as WRK_CTR_CD,\ncast('' as string) as PLNT_CD,\ncast('' as string) as WRK_CTR_CAT_CD,\ncast('' as string) as DEL_IND,\ncast('' as string) as WRK_CTR_USG_CD,\ncast('' as string) as WRK_CTR_LOC_CD,\ncast('' as string) as RESP_PRSN_NUM,\ncast('' as string) as WRK_CTR_ACTV_CD,\ncast('' as string) as LOCK_IND,\ncast('' as string) as SCHDLNG_IND,\ncast('' as string) as SETUP_TYPE_CD,\ncast('' as string) as OPR_CD,\ncast('' as string) as SETUP_FRML_CD,\ncast('' as string) as RUN_FRML_CD,\ncast('' as string) as TEARDOWN_FRML_CD,\ncast('' as string) as CAPY_NUM,\ncast('' as string) as LOC_GRP_CD,\ncast('' as string) as MACH_TYPE_CD,\ncast('' as string) as PLNR_GRP_CD,\ncast('' as string) as OTH_FRML_CD,\ncast('' as string) as SUPL_AREA_CD,\ncast('' as string) as SLOC_CD,\ncast('' as string) as MIXING_IND,\ncast('' as string) as WRK_CTR_DESC,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
