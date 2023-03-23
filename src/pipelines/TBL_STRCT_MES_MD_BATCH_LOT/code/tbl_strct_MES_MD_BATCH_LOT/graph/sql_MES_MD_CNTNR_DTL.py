from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_BATCH_LOT.config.ConfigStore import *
from tbl_strct_MES_MD_BATCH_LOT.udfs.UDFs import *

def sql_MES_MD_CNTNR_DTL(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as CNTNR_DTL_ID,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CNTNR_ID,\ncast('' as BOOLEAN) as AMBNT_EXP_NCR_CRT_IND,\ncast('' as BOOLEAN) as DRY_EXP_NCR_CRT_IND,\ncast('' as DECIMAL(18,4)) as ELAP_AMBNT_EXP_TM,\ncast('' as DECIMAL(18,4)) as ELAP_DRY_EXP_TM,\ncast('' as DECIMAL(18,4)) as ELAP_LGT_EXP_TM,\ncast('' as DECIMAL(18,4)) as ELAP_NITROGEN_EXP_TM,\ncast('' as DECIMAL(18,4)) as ELAP_POST_EXP_TM,\ncast('' as DECIMAL(18,4)) as ELAP_PRE_EXP_TM,\ncast('' as STRING) as EXTRUSION_LOT_ID,\ncast('' as STRING) as FDR_SPOOL_CD,\ncast('' as TIMESTAMP) as JLN_DTTM,\ncast('' as STRING) as KNOTS_VAL,\ncast('' as TIMESTAMP) as LAST_AMBNT_EXP_STRT_DTTM,\ncast('' as TIMESTAMP) as LAST_DRY_EXP_STRT_DTTM,\ncast('' as TIMESTAMP) as LAST_LGT_EXP_STRT_DTTM,\ncast('' as TIMESTAMP) as LAST_NITROGEN_EXP_STRT_DTTM,\ncast('' as TIMESTAMP) as LAST_OP_CMPLT_DTTM,\ncast('' as TIMESTAMP) as LAST_POST_EXP_STRT_DTTM,\ncast('' as TIMESTAMP) as LAST_PRE_EXP_STRT_DTTM,\ncast('' as BOOLEAN) as LGT_EXPN_CR_CRT_IND,\ncast('' as BOOLEAN) as LIMS_SAMP_PASED_IND,\ncast('' as BOOLEAN) as LIMS_TEST_PASED_IND,\ncast('' as STRING) as LOT_TYPE_CD,\ncast('' as BOOLEAN) as NITROGEN_EXPN_CR_CRT_IND,\ncast('' as INT) as SPOOLS_VRFY_CNT,\ncast('' as DECIMAL(18,4)) as OPR_STRT_QTY,\ncast('' as INT) as ORNTN_LINE_NBR,\ncast('' as BOOLEAN) as POST_EXPN_CR_CRT_IND,\ncast('' as BOOLEAN) as PRE_EXPN_CR_CRT_IND,\ncast('' as INT) as RUN_SEQ_NBR,\ncast('' as STRING) as SAMP_SPOOL_NUM,\ncast('' as TIMESTAMP) as SEAL_DTTM,\ncast('' as STRING) as STATIMAT_SPEC_STEP_ID,\ncast('' as STRING) as STATIMAT_TEST_RSLTS_ID,\ncast('' as STRING) as STATIMAT_WRKF_ID,\ncast('' as DECIMAL(18,4)) as TOT_SCANNED_QTY,\ncast('' as INT) as YD_CNT,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as STRING) as LAST_REVERSABLE_TXN_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
