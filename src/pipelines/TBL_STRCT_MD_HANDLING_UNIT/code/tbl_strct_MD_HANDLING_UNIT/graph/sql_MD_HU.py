from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_HANDLING_UNIT.config.ConfigStore import *
from tbl_strct_MD_HANDLING_UNIT.udfs.UDFs import *

def sql_MD_HU(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as String) as SRC_SYS_CD,\ncast('' as String) as HU_NUM,\ncast('' as String) as EXTRNL_HU_NUM,\ncast('' as String) as REF_DOC_NUM,\ncast('' as String) as REF_DOC_TYPE_CD,\ncast('' as String) as SHIPPING_PT_NUM,\ncast('' as decimal(18,4)) as GR_WT_MEAS,\ncast('' as decimal(18,4)) as LD_WT_MEAS,\ncast('' as decimal(18,4)) as VOL_MEAS,\ncast('' as String) as WT_UOM_CD,\ncast('' as timestamp) as CRT_DTTM,\ncast('' as String) as HU_STS_CD,\ncast('' as timestamp) as CHG_DTTM,\ncast('' as String) as BASE_UOM_CD,\ncast('' as String) as PKGNG_MATL_GRP_CD,\ncast('' as String) as PLNT_CD,\ncast('' as String) as LINE_ITEM_CAT_CD,\ncast('' as String) as SLORG_NUM,\ncast('' as String) as SLOC_CD,\ncast('' as String) as EXTRNL_HU2_NUM,\ncast('' as String) as VOL_UNIT,\ncast('' as String) as NM_RESP_CREAT_OBJ,\ncast('' as decimal(18,4)) as LGTH,\ncast('' as decimal(18,4)) as WDTH,\ncast('' as decimal(18,4)) as HGHT,\ncast('' as String) as PKGNG_MATL_TYPE,\ncast('' as String) as OUTP_TYPE,\ncast('' as String) as REF_DSTR_CHNL_MSTR,\ncast('' as String) as WHSE_NUM_WHSE_CMPLX,\ncast('' as String) as LD_PT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
