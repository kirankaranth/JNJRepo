from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_MATERIAL.config.ConfigStore import *
from tbl_strct_MD_MATERIAL.udfs.UDFs import *

def sql_MD_MATL_UOM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as UOM_CD,\ncast('' as string) as EXTRNL_UOM_IN,\ncast('' as string) as EXTRNL_UOM_ID,\ncast('' as string) as DEC_PLACE_FOR_RD,\ncast('' as string) as COMML_MSR_UNIT_ID,\ncast('' as string) as VAL_BAS_CMMT_IN,\ncast('' as string) as UNIT_IN_1,\ncast('' as string) as UNIT_IN_2,\ncast('' as string) as DIM_KEY,\ncast('' as string) as NUMRTR_FOR_CONV_UNIT,\ncast('' as string) as DENOM_FOR_CONV_UNIT,\ncast('' as string) as BASE_TEN_EXP_FOR_CONV,\ncast('' as string) as ADDTV_CNST_FOR_CONV,\ncast('' as string) as EXP_OF_TEN_FOR_FLT_PT_FMT,\ncast('' as string) as NUM_OF_DEC_PLACE_FOR_NUM_DSPLY,\ncast('' as string) as ISO_CD_FOR_UOM,\ncast('' as string) as SLCT_FLD_FOR_CONV_FROM_ISO_CD_TO_INT_CD,\ncast('' as string) as TEMP,\ncast('' as string) as TEMP_UNIT,\ncast('' as string) as UOM_FMLY,\ncast('' as string) as PRESR_VAL,\ncast('' as string) as UNIT_OF_PRESR,\ncast('' as string) as EXTRNL_UOM_COMML_FMT,\ncast('' as string) as EXTRNL_UOM_TECH_FMT,\ncast('' as string) as UOM_SHRT_TEXT,\ncast('' as string) as UOM_LONG_TEXT,\ncast('' as string) as DIM_TEXT,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
