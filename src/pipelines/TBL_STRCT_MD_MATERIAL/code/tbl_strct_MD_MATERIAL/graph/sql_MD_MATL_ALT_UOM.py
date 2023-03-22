from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *

def sql_MD_MATL_ALT_UOM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as MATL_NUM,\ncast('' as string) as ALT_UOM_CD,\ncast('' as string) as BASE_UOM_CD,\ncast('' as string) as HARMO_UOM_CD,\ncast('' as string) as STD_UOM_CD,\ncast('' as string) as ENTRP_UOM_CD,\ncast('' as decimal(18,4)) as LCL_FACT_NUMRTR_MEAS,\ncast('' as decimal(18,4)) as LCL_FACT_DENOM_MEAS,\ncast('' as string) as BASE_UOM_IND,\ncast('' as decimal(18,4)) as BASE_UOM_MULT_QTY,\ncast('' as decimal(18,4)) as FACT_NUMRTR_MEAS,\ncast('' as decimal(18,4)) as FACT_DENOM_MEAS,\ncast('' as string) as GTIN_NUM,\ncast('' as string) as GTIN_CAT_CD,\ncast('' as decimal(18,4)) as WDTH_MEAS,\ncast('' as string) as DIM_UOM_CD,\ncast('' as decimal(18,4)) as GRS_WT_MEAS,\ncast('' as string) as WT_UOM_CD,\ncast('' as decimal(18,4)) as LGTH_MEAS,\ncast('' as decimal(18,4)) as HGHT_MEAS,\ncast('' as decimal(18,4)) as VOL_MEAS,\ncast('' as string) as VOL_UOM_CD,\ncast('' as decimal(18,4)) as NET_WT_MEAS,\ncast('' as string) as GTIN_NUM_HRMZD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
