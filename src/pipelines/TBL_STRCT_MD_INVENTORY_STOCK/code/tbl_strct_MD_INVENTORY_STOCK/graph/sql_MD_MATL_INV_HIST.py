from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_INVENTORY_STOCK.config.ConfigStore import *
from tbl_strct_MD_INVENTORY_STOCK.udfs.UDFs import *

def sql_MD_MATL_INV_HIST(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as SRC_TBL_NM,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as SLOC_CD,\ncast('' as string) as BTCH_NUM,\ncast('' as string) as SPCL_STCK_IND,\ncast('' as string) as PRTY_NUM,\ncast('' as int) as PSTNG_YR,\ncast('' as int) as PSTNG_PER,\ncast('' as decimal(18,4)) as UNRSTRCTD_STCK,\ncast('' as decimal(18,4)) as IN_TRNSFR_STCK,\ncast('' as decimal(18,4)) as QLTY_INSP_STCK,\ncast('' as decimal(18,4)) as RSTRCTD_STCK,\ncast('' as decimal(18,4)) as BLCKD_STCK,\ncast('' as decimal(18,4)) as RTRNS,\ncast('' as string) as BASE_UOM_CD,\ncast('' as decimal(18,4)) as STO_IN_TRNST_QTY,\ncast('' as decimal(18,4)) as PLNT_IN_TRNST_QTY,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
