from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MES_MD_SITE_PLANT.config.ConfigStore import *
from tbl_strct_MES_MD_SITE_PLANT.udfs.UDFs import *

def sql_MES_MD_FCTRY(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as STRING) as SRC_SYS_CD,\ncast('' as STRING) as FCTRY_ID,\ncast('' as STRING) as ENTRP_ID,\ncast('' as STRING) as ORG_ID,\ncast('' as STRING) as ADJ_RSN_MAT_DFLT_ID,\ncast('' as STRING) as APPL_ACCT_NM_ID,\ncast('' as STRING) as APPL_ACCT_PSWRD_TXT,\ncast('' as STRING) as ARCH_IN_TRNST_TXT,\ncast('' as STRING) as AUTO_SIGN_USRID,\ncast('' as STRING) as AUTO_STRT_SETS_ID,\ncast('' as STRING) as BRAIDING_INV_OPR_ID,\ncast('' as STRING) as BULK_INV_OPR_ID,\ncast('' as STRING) as CARR_NUMING_RULE_ID,\ncast('' as STRING) as CARR_WRKF_ID,\ncast('' as STRING) as CASE_ADJ_LOSS_RSN_GRP_ID,\ncast('' as STRING) as CASE_CD,\ncast('' as STRING) as CASE_LBL_TEMPL_GRP_ID,\ncast('' as INT) as CHG_CNT,\ncast('' as BOOLEAN) as CHG_JOB_STS_AUTMT_IND,\ncast('' as STRING) as CHG_MGT_APPL_ID,\ncast('' as STRING) as CHG_STS_ID,\ncast('' as STRING) as CKLST_HEAT_SEAL_ID,\ncast('' as STRING) as CKLST_HYDRATION_ID,\ncast('' as STRING) as CKLST_INJECTION_MOLDING_ID,\ncast('' as STRING) as CKLST_LEN_FABRICATION_ID,\ncast('' as STRING) as CKLST_POST_HYDRATION_ID,\ncast('' as STRING) as CKLST_QA_ID,\ncast('' as STRING) as CKLST_SEC_PACK_ID,\ncast('' as STRING) as CKLST_STERILIZATION_ID,\ncast('' as STRING) as CHEM_TIP_INV_OPR_ID,\ncast('' as STRING) as CLSN_MSMCHED_UPC_ID,\ncast('' as STRING) as CMMS_DATA_TRNSP_ID,\ncast('' as STRING) as CMMS_DFLT_DSTN_ID,\ncast('' as STRING) as CMMS_MSG_DFLT_SNDR_TXT,\ncast('' as STRING) as COATING_INV_OPR_ID,\ncast('' as STRING) as CMPLT_ORDR_STS_ID,\ncast('' as STRING) as CNTNR_LVL_ID,\ncast('' as STRING) as CNTNR_NUMING_RULE_ID,\ncast('' as STRING) as DATA_CLCT_DEF_FOIL_LAB_BASE_ID,\ncast('' as STRING) as DATA_CLCT_DEF_FOIL_LBL_ID,\ncast('' as STRING) as DATA_CLCT_DEF_PLCDHR_BASE_ID,\ncast('' as STRING) as DATA_CLCT_DEF_PLCDHR_ID,\ncast('' as STRING) as DATA_CLCT_DEF_POST_HYD_BASE_ID,\ncast('' as STRING) as DATA_CLCT_DEF_POST_HYD_ID,\ncast('' as STRING) as DATA_TRNSP_DCS_ID,\ncast('' as STRING) as DFLT_AUTO_PE_DESC,\ncast('' as STRING) as DFLT_DATA_PT_NM,\ncast('' as STRING) as DFLT_INV_LOC_ID,\ncast('' as STRING) as DFLT_ISS_DIFF_RSN_ID,\ncast('' as STRING) as DFLT_ISS_RSN_ID,\ncast('' as STRING) as DFLT_LOT_RSN_ID,\ncast('' as STRING) as DFLT_LOT_STRT_RSN_ID,\ncast('' as STRING) as DFLT_OWN_CD,\ncast('' as STRING) as DFLT_RECON_ADJ_RSN_ID,\ncast('' as STRING) as DFLT_RMV_RSN_ID,\ncast('' as STRING) as DFLT_REPL_RSN_ID,\ncast('' as STRING) as DFLT_RUN_STRT_RSN_ID,\ncast('' as STRING) as DSTRY_RSN_DFLT_ID,\ncast('' as STRING) as DETAIAN_RSN_SET_PT_MSMCH_ID,\ncast('' as STRING) as DETAIN_RSN_CTN_VFY_FAIL_ID,\ncast('' as STRING) as DETAIN_RSN_EXPN_MSMCH_ID,\ncast('' as STRING) as DETAIN_RSN_FOIL_VFY_FAIL_ID,\ncast('' as STRING) as DETAIN_RSN_FOR_CE_MOVE_STERI_I,\ncast('' as STRING) as DETAIN_RSN_FOR_RAW_MATL_ID,\ncast('' as STRING) as DETAIN_RSN_HIGH_SPEC_ID,\ncast('' as STRING) as DETAIN_RSN_IN_PRCS_SPLT_ID,\ncast('' as STRING) as DETAIN_RSN_INTRO_MSTR_AT_QC_ID,\ncast('' as STRING) as DETAIN_RSN_LOT_INTRO_FE_ID,\ncast('' as STRING) as DETAIN_RSN_LOT_INTRO_ID,\ncast('' as STRING) as DETAIN_RSN_LOT_NOT_IN_HS_ID,\ncast('' as STRING) as DETAIN_RSN_LOW_SPEC_ID,\ncast('' as STRING) as DETAIN_RSN_MAN_ST_CKLST_ID,\ncast('' as STRING) as DETAIN_RSN_NEW_MSTR_ID,\ncast('' as STRING) as DETAIN_RSN_NO_CTN_ID,\ncast('' as STRING) as DETAIN_RSN_NO_DATA_ID,\ncast('' as STRING) as DETAIN_RSN_NO_DHR_OR_FOIL_DATA,\ncast('' as STRING) as DETAIN_RSN_NO_PDDC_DATA_ID,\ncast('' as STRING) as DETAIN_RSN_NO_PLC_VERS_DATA_ID,\ncast('' as STRING) as DETAIN_RSN_NON_STD_MOVE_ID,\ncast('' as STRING) as DETAIN_RSN_NOT_READY_FOR_STERI,\ncast('' as STRING) as DETAIN_RSN_PLC_VERS_MSMCH_ID,\ncast('' as STRING) as DETAIN_RSN_PROD_NOT_VLDD_ID,\ncast('' as STRING) as DETAIN_RSN_RECON_VAR_ABV_TOL_I,\ncast('' as STRING) as DETAIN_RSN_RECON_VAR_BLW_TOL_I,\ncast('' as STRING) as DETAIN_RSN_RECON_VAR_ID,\ncast('' as STRING) as DETAIN_RSN_STRT_QTY_CHG_ID,\ncast('' as STRING) as DETAIN_RSN_UPC_MSMCH_ID,\ncast('' as STRING) as DSPCH_RULE_ID,\ncast('' as BOOLEAN) as DSPLY_GENL_MSG_IND,\ncast('' as BOOLEAN) as ENABL_CARR_TCKG_IND,\ncast('' as STRING) as EXCLV_MSTR_LOT_CKLST_ID,\ncast('' as STRING) as EXTRUSION_INV_OPR_ID,\ncast('' as STRING) as FCTRY_DESC,\ncast('' as STRING) as FCTRY_NM,\ncast('' as STRING) as GENL_MSG_TXT,\ncast('' as STRING) as GERMAN_EXTRUSION_INV_OPR_ID,\ncast('' as STRING) as HACOBA_INV_OPR_ID,\ncast('' as STRING) as HEAT_TIP_INV_OPR_ID,\ncast('' as STRING) as ICON_ID,\ncast('' as BOOLEAN) as IS_FRZN_IND,\ncast('' as BOOLEAN) as IS_PREACTOR_ENABL_IND,\ncast('' as BOOLEAN) as IS_VLD_MATL_QUE_QTY_IND,\ncast('' as STRING) as ISS_DIFF_RSN_DFLT_ID,\ncast('' as STRING) as JDE_DATA_TRNSP_ID,\ncast('' as STRING) as JOB_SCHED_STS_MDL_ID,\ncast('' as STRING) as LIMERICK_LOT_AUTO_SIGN_ID,\ncast('' as DECIMAL(18,4)) as LIMERICK_ORDR_QTY,\ncast('' as STRING) as LIMERICK_RWRK_LINE_NM,\ncast('' as BOOLEAN) as LCL_SCHDLNG_ENABL_IND,\ncast('' as STRING) as LOC_PROD_CONV_REQ_FILLED_TXT,\ncast('' as STRING) as LOC_PROD_CONV_REQ_TXT,\ncast('' as STRING) as LOC_RTN_FROM_DSTN_TXT,\ncast('' as STRING) as LOC_RWRK_TXT,\ncast('' as STRING) as LOSS_RSN_QTY_OVRD_ID,\ncast('' as STRING) as LOSS_RSN_RJCT_ID,\ncast('' as STRING) as LOSS_RSN_SAMPS_ID,\ncast('' as STRING) as LOSS_RSN_VAR_ID,\ncast('' as STRING) as LOT_DETAIN_RSN_IN_PRCS_ID,\ncast('' as STRING) as LOT_DETAIN_STERILIZATION_VERIF,\ncast('' as STRING) as LOT_END_RSN_INTRO_ID,\ncast('' as STRING) as LOT_RACK_DETAIN_RSN_ID,\ncast('' as STRING) as LOT_RACK_QTY_ADJ_RSN_ID,\ncast('' as STRING) as LOT_RSN_MOVE_INTRO_ID,\ncast('' as STRING) as LOT_RSN_MOVE_OUT_OF_STERILIZAT,\ncast('' as STRING) as MSTR_LOT_OWN_ID,\ncast('' as STRING) as MSTR_LOT_STRT_RSN_ID,\ncast('' as BOOLEAN) as MATL_MNG_ENABL_IND,\ncast('' as INT) as MAX_CASE_SEQ_NBR,\ncast('' as INT) as MAX_LBR_EDIT_DAYS_CNT,\ncast('' as INT) as MAX_PLLT_SEQ_NBR,\ncast('' as STRING) as MAX_TIMES_IN_CE_MSGS_RTRV_VAL,\ncast('' as STRING) as MFG_CAL_ID,\ncast('' as STRING) as MFG_PLNT_LOC_ID,\ncast('' as STRING) as MFG_SRC_NM,\ncast('' as TIMESTAMP) as MONOMER_DSPLY_DTTM,\ncast('' as INT) as MONOMER_SPLT_CNT,\ncast('' as STRING) as MULTI_CNTNR_UTIL_SPEC_BASE_ID,\ncast('' as STRING) as MULTI_CNTNR_UTIL_SPEC_ID,\ncast('' as STRING) as NCR_FC_AMBNT_ID,\ncast('' as STRING) as NCR_FC_DRY_ID,\ncast('' as STRING) as NCR_FC_LGT_ID,\ncast('' as STRING) as NCR_FC_NITROGEN_ID,\ncast('' as STRING) as NCR_FC_POST_ANNEALING_ID,\ncast('' as STRING) as NCR_FC_PRE_ANNEALING_ID,\ncast('' as STRING) as NOTES_TXT,\ncast('' as STRING) as OBJ_TYPE_ID,\ncast('' as STRING) as OEE_SETS_ID,\ncast('' as STRING) as OPR__CMPLT_DFLT_QTY_ADJ_RSN_ID,\ncast('' as STRING) as PLLT_CD,\ncast('' as STRING) as PLLT_LBL_TEMPL_GRP_ID,\ncast('' as STRING) as PE_FM_AMBNT_ID,\ncast('' as STRING) as PE_FM_DRY_ID,\ncast('' as STRING) as PE_FM_LGT_ID,\ncast('' as STRING) as PE_FM_NITROGEN_ID,\ncast('' as STRING) as PE_FM_POST_ANNEALING_ID,\ncast('' as STRING) as PE_FM_PRE_ANNEALING_ID,\ncast('' as STRING) as PRT_QUE_ID,\ncast('' as BOOLEAN) as PROD_CONV_FROM_DSTN_ALLW_IND,\ncast('' as STRING) as PROD_CONV_LOC_FILLED_ID,\ncast('' as STRING) as PROD_CONV_LOC_REQ_TXT,\ncast('' as STRING) as PROD_CONV_NOTFICATION_TRGT_ID,\ncast('' as STRING) as PROD_CONV_REQ_EML_NTF_ID,\ncast('' as STRING) as PROD_GRP_CTN_ID,\ncast('' as STRING) as PROD_GRP_CASE_ID,\ncast('' as STRING) as PROD_GRP_FOIL_ID,\ncast('' as STRING) as PROD_GRP_POLYPROPYLENE_ID,\ncast('' as STRING) as PROD_GRP_POLYSTYRENE_ID,\ncast('' as STRING) as PROD_GRP_RMM_ID,\ncast('' as STRING) as PROD_GRP_SALINE_ID,\ncast('' as STRING) as PROD_GRP_TWEEN_ID,\ncast('' as STRING) as RLSE_RSN_NM_CLNC_ID,\ncast('' as STRING) as RPT_HD_TXT,\ncast('' as BOOLEAN) as REQ_LOC_IND,\ncast('' as STRING) as RST_CASE_VFY_TIME_VAL,\ncast('' as BOOLEAN) as RSDN_TIME_CHK_ENABL_IND,\ncast('' as INT) as RETN_DAYS_CNT,\ncast('' as BOOLEAN) as RTN_CASE_VFY_CNV_LOTS_IND,\ncast('' as STRING) as RTN_FROM_DIST_QTY_ADJ_RSN_ID,\ncast('' as STRING) as RTN_FROM_DSTN_RWRK_RSN_ID,\ncast('' as STRING) as RWRK_STEP_NM,\ncast('' as STRING) as SAP_RSTK_CD,\ncast('' as STRING) as SAP_UNRSTK_CD,\ncast('' as STRING) as SMTP_TRNSP_ID,\ncast('' as STRING) as SPLT_LOT_STRT_RSN_ID,\ncast('' as STRING) as SPOOL_QTY_ADJ_RSN_ID,\ncast('' as STRING) as STERILIZATION_RUN_DCD_ID,\ncast('' as STRING) as TRAIN_REQ_GRP_ID,\ncast('' as STRING) as UNKWN_LOT_RSN_ID,\ncast('' as STRING) as UOM_ID,\ncast('' as STRING) as UPDT_STRT_QTY_ADJ_RSN_ID,\ncast('' as STRING) as VAR_TLRNC_MINUS_VAL,\ncast('' as STRING) as VAR_TLRNC_PLUS_VAL,\ncast('' as STRING) as WRKF_BASE_ID,\ncast('' as STRING) as WRKF_ID,\ncast('' as STRING) as WIP_MSG_DEF_MGR_ID,\ncast('' as STRING) as ETH_SMTP_TRNSP_ID,\ncast('' as INT) as DAI_ETL_ID,\ncast('' as TIMESTAMP) as DAI_CRT_DTTM,\ncast('' as TIMESTAMP) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1