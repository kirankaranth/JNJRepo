from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_RESOURCE.config.ConfigStore import *
from tbl_strct_MES_MD_RESOURCE.udfs.UDFs import *

def sql_MES_MD_RSRS(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as RSRS_ID,\ncast('' as STRING) as FCTRY_ID,\ncast('' as STRING) as LOC_ID,\ncast('' as STRING) as SPEC_ID,\ncast('' as STRING) as ALT_ASSET_ID,\ncast('' as STRING) as ASSET_SITE_NM,\ncast('' as STRING) as ASYNC_TMOUT_TXT,\ncast('' as BOOLEAN) as CALIBRATION_ASSET_IND,\ncast('' as STRING) as CIO_CHNL_ADPT_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_STS_ID,\ncast('' as STRING) as CLRNC_LVL_CD,\ncast('' as INT) as CNTNR_CAPY_CNT,\ncast('' as STRING) as CNTNR_STS_CD,\ncast('' as STRING) as DEC_PRCN_VAL,\ncast('' as STRING) as DEC_SCALE_VAL,\ncast('' as STRING) as DFLT_EXPRSSN_TXT,\ncast('' as STRING) as DFLT_EXPRSSN_TYPE_CD,\ncast('' as BOOLEAN) as EXTRNL_STS_IND,\ncast('' as STRING) as FULL_QUAL_NM,\ncast('' as STRING) as ICON_ID,\ncast('' as BOOLEAN) as INCL_IN_OEE_IND,\ncast('' as BOOLEAN) as INCL_RAW_DATA_IND,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_RESL_DNT_QRY_IND,\ncast('' as BOOLEAN) as IS_VLD_QUE_IND,\ncast('' as STRING) as LINE_ID,\ncast('' as BOOLEAN) as LOGGING_IND,\ncast('' as STRING) as MAINT_CLS_ID,\ncast('' as STRING) as MATL_QUE_BOM_ID,\ncast('' as STRING) as MAXIMO_DSTN_ID,\ncast('' as STRING) as MAXIMO_GL_ACCT_CD,\ncast('' as STRING) as MAXIMO_RSRS_NM,\ncast('' as STRING) as MAX_CAPY_VAL,\ncast('' as STRING) as MIN_CAPY_VAL,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as OEE_SETS_ID,\ncast('' as STRING) as OPC_CHNL_ID,\ncast('' as STRING) as OPC_MACH_ID,\ncast('' as STRING) as OPC_SERVR_ID,\ncast('' as STRING) as PARNT_RSRS_ID,\ncast('' as STRING) as PC_ID,\ncast('' as STRING) as PORT_ID,\ncast('' as STRING) as PRT_QUE_ID,\ncast('' as STRING) as PRDTN_STS_ID,\ncast('' as STRING) as PRMT_TXT,\ncast('' as STRING) as READABL_VAL,\ncast('' as DECIMAL(18,4)) as RSRS_CAPY_QTY,\ncast('' as STRING) as RSRS_DESC,\ncast('' as STRING) as RSRS_FMLY_ID,\ncast('' as STRING) as RSRS_NM,\ncast('' as STRING) as RSRS_NUM,\ncast('' as STRING) as RSRS_PART_ID,\ncast('' as STRING) as RSRS_STS_MDL_ID,\ncast('' as STRING) as RSRS_TYPE_ID,\ncast('' as STRING) as SGNTR_REQ_ID,\ncast('' as INT) as TIME_TO_LIVE_SECS_CNT,\ncast('' as INT) as TRNG_RCNT_EXP_DAYS_CNT,\ncast('' as STRING) as TRAIN_RCNT_ID,\ncast('' as STRING) as TRAIN_REQ_GRP_ID,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as VEND_ID,\ncast('' as STRING) as VEND_MDL_NUM,\ncast('' as STRING) as VEND_SER_NUM,\ncast('' as STRING) as WEB_SRVC_TRNSP_ID,\ncast('' as STRING) as WIP_MSG_DEF_MGR_ID,\ncast('' as STRING) as WRK_RNG_MAX_VAL,\ncast('' as STRING) as WRK_RNG_MIN_VAL,\ncast('' as STRING) as ETH_PRT_QUE_ID,\ncast('' as STRING) as ETH_RSRS_TYPE_ID,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as LAST_REVERSABLE_TXN_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
