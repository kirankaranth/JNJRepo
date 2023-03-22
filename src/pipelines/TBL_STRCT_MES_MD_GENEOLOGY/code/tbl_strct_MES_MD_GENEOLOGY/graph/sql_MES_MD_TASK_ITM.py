from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_GENEOLOGY.config.ConfigStore import *
from tbl_strct_MES_MD_GENEOLOGY.udfs.UDFs import *

def sql_MES_MD_TASK_ITM(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as TAKS_ITM_ID,\ncast('' as STRING) as TASKLIST_ID,\ncast('' as BOOLEAN) as ADVN_TO_NEXT_TASK_IND,\ncast('' as BOOLEAN) as ALLW_MAN_WT_ENT_OVRD_IND,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CMPUT_ID,\ncast('' as STRING) as DATA_CLCT_DEF_BASE_ID,\ncast('' as STRING) as DATA_CLCT_DEF_ID,\ncast('' as STRING) as DOC_SET_ID,\ncast('' as STRING) as ESIG_REQ_ID,\ncast('' as STRING) as EXPT_IMPT_KEY_VAL,\ncast('' as BOOLEAN) as EXPRSSN_DPNDS_ON_FROM_CNTNR_IN,\ncast('' as STRING) as FAIL_MODE_ID,\ncast('' as STRING) as INSTR_TXT,\ncast('' as STRING) as INSTR_TYPE_CD,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_TLRNC_OVRD_ALLW_IND,\ncast('' as BOOLEAN) as MAN_WT_ENT_IND,\ncast('' as STRING) as MANUL_WT_OVRD_ESIGN_ID,\ncast('' as BOOLEAN) as MATCH_BY_PROD_NM_IND,\ncast('' as BOOLEAN) as MATL_FROM_SAME_CNTNR_IND,\ncast('' as INT) as MAX_ITERS_CNT,\ncast('' as STRING) as MAX_QTY_ALLW_EXPRSSN_TXT,\ncast('' as INT) as MIN_ITERS_CNT,\ncast('' as STRING) as MIN_QTY_ALLW_EXPRSSN_TXT,\ncast('' as STRING) as NCR_FAIL_CD,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as PROD_BASE_ID,\ncast('' as STRING) as PROD_ID,\ncast('' as BOOLEAN) as QTY_ADDTV_IND,\ncast('' as STRING) as RPT_INSTR_TXT,\ncast('' as STRING) as REQ_QTY_EXPRSSN_TXT,\ncast('' as STRING) as SCALE_GRP_ID,\ncast('' as INT) as SEQ_NBR,\ncast('' as STRING) as SPL_PRCSG_CD,\ncast('' as STRING) as TASK_ITM_NM,\ncast('' as STRING) as TASK_TYPE_CD,\ncast('' as STRING) as TASK_USG_TXT,\ncast('' as STRING) as TLRNC_OVRD_ESIGN_ID,\ncast('' as STRING) as TRAIN_REQ_GRP_ID,\ncast('' as STRING) as TRX_PG_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
