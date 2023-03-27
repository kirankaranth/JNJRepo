from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_GENEOLOGY.config.ConfigStore import *
from tbl_strct_MES_MD_GENEOLOGY.udfs.UDFs import *

def sql_MES_MD_DATA_CLCT_DEF(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as DATA_CLCT_DEF_ID,\ncast('' as STRING) as DATA_CLCT_DEF_BASE_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_HIST_ID,\ncast('' as STRING) as DVC_TYPE_ID,\ncast('' as BOOLEAN) as DISA_FOR_USER_ENT_IND,\ncast('' as STRING) as DATA_CLCT_DEF_DESC,\ncast('' as STRING) as DATA_CLCT_DEF_RVSN_ID,\ncast('' as STRING) as DATA_PT_LYOUT_CD,\ncast('' as STRING) as DSPLY_LMT_TXT,\ncast('' as STRING) as ENG_CHG_ORDR_NUM,\ncast('' as STRING) as FAIL_MODE_ID,\ncast('' as STRING) as ICON_ID,\ncast('' as STRING) as INSTR_TXT,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_RSRS_DC_IND,\ncast('' as STRING) as MACH_IO_RSRS_GRP_ID,\ncast('' as STRING) as MACH_IO_TYPE_CD,\ncast('' as STRING) as MACH_READ_TRIGR_TAG_CD,\ncast('' as STRING) as MACH_READ_TRIGR_VAL,\ncast('' as STRING) as MACH_READ_WAIT_DELAY_VAL,\ncast('' as STRING) as NCR_FAIL_CD,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as PARAMETRIC_DATA_DEF_TYPE_CD,\ncast('' as STRING) as PARM_IC_DATA_DEF_TYPE_CD,\ncast('' as STRING) as RSRS_TYPE_ID,\ncast('' as STRING) as SC_MACH_READ_TRIGR_TAG_NM,\ncast('' as STRING) as SPC_CHRT_DEF_GRP_ID,\ncast('' as STRING) as SPC_CHRT_GRP_ID,\ncast('' as STRING) as STS_CD,\ncast('' as STRING) as WEB_PART_ID,\ncast('' as STRING) as WIP_MSG_DEF_MGR_ID,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
