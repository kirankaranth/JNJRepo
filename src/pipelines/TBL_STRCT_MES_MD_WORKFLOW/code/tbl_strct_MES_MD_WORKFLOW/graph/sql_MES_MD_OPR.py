from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_WORKFLOW.config.ConfigStore import *
from tbl_strct_MES_MD_WORKFLOW.udfs.UDFs import *

def sql_MES_MD_OPR(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as OPR_ID,\ncast('' as STRING) as ACTNS_MENU_ID,\ncast('' as BOOLEAN) as ALLW_OPR_ADJ_IND,\ncast('' as BOOLEAN) as ALLW_RWRK_IND,\ncast('' as STRING) as ALLW_GRADE_CD,\ncast('' as STRING) as AUTO_ADJ_LMT_VAL,\ncast('' as STRING) as AUTO_ADJ_RSN_ID,\ncast('' as STRING) as BTCH_PRCSG_TYPE_ID,\ncast('' as STRING) as BTCH_TYPE_RLSE_VERIF_ID,\ncast('' as STRING) as BNS_RSN_ID,\ncast('' as STRING) as BUY_RSN_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as STRING) as CHG_STS_ID,\ncast('' as STRING) as CKLST_GRP_ID,\ncast('' as INT) as CMPLT_CNT,\ncast('' as STRING) as CMPNT_DEFCT_RSN_ID,\ncast('' as STRING) as CNSMPTN_GRP_ID,\ncast('' as STRING) as CNTNR_DEFCT_RSN_ID,\ncast('' as STRING) as DFLT_RLUP_RSN_ID,\ncast('' as INT) as DFLT_UNIT_PER_BOX_NBR,\ncast('' as STRING) as DSPCH_RULE_ID,\ncast('' as STRING) as EES_OPR_TYPE_CD,\ncast('' as STRING) as ETH_SUBST_RSN_ID,\ncast('' as STRING) as ICON_ID,\ncast('' as STRING) as IMG_TXT,\ncast('' as STRING) as IN_PRCS_CKLST_RST_GRP_ID,\ncast('' as STRING) as IN_PRCS_WRKF_BASE_ID,\ncast('' as STRING) as IN_PRCS_WRKF_ID,\ncast('' as BOOLEAN) as IN_TRNST_IND,\ncast('' as STRING) as INV_PT_CD,\ncast('' as BOOLEAN) as IS_BK_END_IND,\ncast('' as BOOLEAN) as IS_BULK_PACK_IND,\ncast('' as BOOLEAN) as IS_FNL_INV_PT_IND,\ncast('' as BOOLEAN) as IS_FRN_END_IND,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_HEAT_SEAL_IND,\ncast('' as BOOLEAN) as IS_IN_PRCS_SPLT_OPR_IND,\ncast('' as BOOLEAN) as IS_MSTR_LOT_STGNG_OPR_IND,\ncast('' as BOOLEAN) as IS_PRE_QC_IND,\ncast('' as BOOLEAN) as IS_PROD_CONV_MULTI_STAGE_IND,\ncast('' as BOOLEAN) as IS_PROD_CONV_IND,\ncast('' as BOOLEAN) as IS_PROD_CONV_STGNG_IND,\ncast('' as BOOLEAN) as IS_QA_IND,\ncast('' as BOOLEAN) as IS_QC_IND,\ncast('' as BOOLEAN) as IS_SEC_PACK_IND,\ncast('' as BOOLEAN) as IS_SEC_PKG_STGNG_IND,\ncast('' as BOOLEAN) as IS_SPLT_INTRO_IND,\ncast('' as BOOLEAN) as IS_STGNG_IND,\ncast('' as BOOLEAN) as IS_STERI_STGNG_IND,\ncast('' as BOOLEAN) as IS_STERILIZATION_IND,\ncast('' as BOOLEAN) as IS_STERILIZER_IND,\ncast('' as STRING) as ISS_RSN_GRP_ID,\ncast('' as BOOLEAN) as LIMS_RLSE_REQ_IND,\ncast('' as STRING) as LCL_RWRK_RSN_ID,\ncast('' as STRING) as LOSS_RSN_ID,\ncast('' as STRING) as LOT_RSN_GRP_ID,\ncast('' as INT) as MAX_ALLW_GRADE_NBR,\ncast('' as STRING) as MIC_CD,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as OPR_DESC,\ncast('' as STRING) as OPR_NM,\ncast('' as STRING) as OUTSD_SRVC_PT_CD,\ncast('' as STRING) as PRT_QUE_1_ID,\ncast('' as STRING) as PRT_QUE_2_ID,\ncast('' as STRING) as PRT_QUE_3_ID,\ncast('' as STRING) as PRT_QUE_ID,\ncast('' as STRING) as QTY_ADJ_RSN_ID,\ncast('' as BOOLEAN) as REJ_INCM_NC_CNTNR_IND,\ncast('' as STRING) as RMV_RSN_GRP_ID,\ncast('' as STRING) as RWRK_RSN_ID,\ncast('' as STRING) as SAMP_TYPE_REQ_ID,\ncast('' as STRING) as SAMP_TYPE_VERIF_ID,\ncast('' as STRING) as SCHDLNG_DTL_ID,\ncast('' as STRING) as SEALING_OPR_CD,\ncast('' as STRING) as SEL_RSN_ID,\ncast('' as BOOLEAN) as SEND_OP_CMPLT_MSG_IND,\ncast('' as STRING) as SHIP_DEST_ID,\ncast('' as STRING) as SUBST_RSN_ID,\ncast('' as STRING) as SUM_THRUPUT_VAL,\ncast('' as STRING) as THRUPUT_RPTG_LVL_ID,\ncast('' as BOOLEAN) as TRK_AMBNT_EXP_TIME_IND,\ncast('' as BOOLEAN) as TRK_DRY_EXP_TIME_IND,\ncast('' as BOOLEAN) as TRK_LGT_EXP_TIME_IND,\ncast('' as BOOLEAN) as TRK_NITROGEN_EXP_TIME_IND,\ncast('' as STRING) as TRK_POST_ANNEALING_EXP_TIME_IN,\ncast('' as BOOLEAN) as TRK_PRE_ANNEALING_EXP_TIME_IND,\ncast('' as STRING) as TRAIN_REQ_GRP_ID,\ncast('' as INT) as TRNG_RCNT_EXP_DAYS_CNT,\ncast('' as STRING) as TRNG_RCNT_ID,\ncast('' as BOOLEAN) as USE_QUE_IND,\ncast('' as BOOLEAN) as USE_RACK_IND,\ncast('' as BOOLEAN) as VLD_SNGL_SPOOLS_IND,\ncast('' as BOOLEAN) as VLD_SPOOLS_IND,\ncast('' as STRING) as WIP_MSG_DEF_MGR_ID,\ncast('' as STRING) as WRK_CTR_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1
