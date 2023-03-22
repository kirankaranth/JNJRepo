from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from tbl_strct_MD_PURCHASE_ORDER.config.ConfigStore import *
from tbl_strct_MD_PURCHASE_ORDER.udfs.UDFs import *

def sql_MD_PO_LINE(spark: SparkSession, ) -> (DataFrame):
    df1 = spark.sql(
        "select \ncast('' as string) as SRC_SYS_CD,\ncast('' as string) as PO_NUM,\ncast('' as string) as PO_LINE_NBR,\ncast('' as string) as CO_CD,\ncast('' as string) as PO_TYPE_CD,\ncast('' as string) as DEL_IND,\ncast('' as string) as MATL_NUM,\ncast('' as string) as PLNT_CD,\ncast('' as string) as SLOC_CD,\ncast('' as string) as INTRNL_REF_NUM,\ncast('' as decimal(18,4)) as PO_LINE_QTY,\ncast('' as string) as PO_UOM_CD,\ncast('' as string) as STK_TYPE_CD,\ncast('' as string) as DELV_CMPLT_IND,\ncast('' as string) as LINE_ITEM_TYPE_CD,\ncast('' as string) as ACCT_ASGNMT_CAT_CD,\ncast('' as string) as PRIN_PRCH_AGMT_NUM,\ncast('' as string) as PRIN_PRCH_AGMT_LINE_NUM,\ncast('' as string) as DELV_ADDR_NUM,\ncast('' as decimal(18,4)) as GR_LEAD_TIME_DAYS_QTY,\ncast('' as string) as SPL_STK_IND,\ncast('' as string) as CNFRM_CD,\ncast('' as string) as SUBCNTRC_IND,\ncast('' as string) as PR_NUM,\ncast('' as string) as PR_LINE_NBR,\ncast('' as string) as BRAZILIAN_NCM_CD,\ncast('' as string) as OUTB_DELV_CMPLT_IND,\ncast('' as string) as ISU_SLOC_CD,\ncast('' as string) as CUST_NUM,\ncast('' as string) as SHIPPING_PT_NUM,\ncast('' as string) as EM_MATERIAL_CODE,\ncast('' as string) as BASE_UOM,\ncast('' as string) as PO_PRICE_UOM,\ncast('' as decimal(18,2)) as PO_LINE_PRICE,\ncast('' as decimal(18,2)) as DOCUMENT_CURRENCY_GROSS_AMOUNT,\ncast('' as decimal(18,2)) as DOCUMENT_CURRENCY_NET_AMOUNT,\ncast('' as decimal(18,4)) as NUMRTR_FOR_CONV_ORDR_UNIT,\ncast('' as decimal(18,4)) as DENOM_FOR_CONV_ORDR_UNIT,\ncast('' as decimal(18,4)) as PRICE_UNIT,\ncast('' as string) as ORDER_SUF,\ncast('' as timestamp) as PO_LINE_DTTM,\ncast('' as string) as PRMRY_QTY_ORD,\ncast('' as string) as G_L_ACCT_NUM,\ncast('' as timestamp) as PRCHSNG_DOC_CRT_DTTM,\ncast('' as string) as TAX_RPTG_CTRY_RGN,\ncast('' as string) as TAX_ON_SLS,\ncast('' as decimal(18,4)) as STK_TFR_RECV_QTY,\ncast('' as string) as CONCAT_OF_EBELN_AND_EBELP,\ncast('' as string) as ORIG_OF_PRCHSNG_DOC_ITM,\ncast('' as timestamp) as PRCHSNG_DOC_ITM_CHG_DTTM,\ncast('' as string) as SHRT_TEXT,\ncast('' as string) as MATL_NUM1,\ncast('' as string) as MATL_GRP,\ncast('' as string) as NUM_OF_PRCHSNG_INFO_REC,\ncast('' as decimal(18,4)) as TRGT_QTY,\ncast('' as decimal(18,4)) as NUMRTR_CONV_ORDR_PRC_UNIT,\ncast('' as decimal(18,4)) as DENOM_CONV_ORDR_PRC_UNIT,\ncast('' as timestamp) as DEAD_FOR_SBMN_OF_BID,\ncast('' as decimal(18,4)) as GOODS_RCPT_PRCSG_TIME_IN_DAYS,\ncast('' as timestamp) as TAX_RT_VLD_FROM_DTTM,\ncast('' as timestamp) as DETRMNG_TAX_RT_DTTM,\ncast('' as string) as SETLM_GRP_1,\ncast('' as string) as UPDT_INFO_REC_IN,\ncast('' as string) as PRC_PRINTOUT,\ncast('' as string) as EST_PRC_IN,\ncast('' as decimal(18,4)) as NUM_OF_REMNDR_EXPDTR,\ncast('' as decimal(18,4)) as NUM_OF_DAYS_FST_REMNDR,\ncast('' as decimal(18,4)) as NUM_OF_DAYS_SEC_REMNDR,\ncast('' as decimal(18,4)) as NUM_OF_DAYS_THIRD_RMNDR,\ncast('' as decimal(18,4)) as OVRDELV_TLRNC,\ncast('' as string) as UNLMTD_OVRDELV_ALLW,\ncast('' as decimal(18,4)) as UNDRDELV_TLRNC,\ncast('' as string) as VALUT_TYPE,\ncast('' as string) as VALUT_CAT,\ncast('' as string) as REJ_IN,\ncast('' as string) as INTRNL_COMT_ON_QUTN,\ncast('' as string) as FNL_INVC_IN,\ncast('' as string) as CNSMPTN_PSTNG,\ncast('' as string) as DSTN_IN_FOR_MLT_ACCT_ASGNMT,\ncast('' as string) as PRTL_INVC_IN,\ncast('' as string) as GOODS_RCPT_IN,\ncast('' as string) as GOODS_RCPT_NVALUATED,\ncast('' as string) as INVC_RCPT_IN,\ncast('' as string) as GR_BAS_INVC_VERIF_IN,\ncast('' as string) as ORDR_ACK_REQ,\ncast('' as string) as ORDR_ACK_NUM,\ncast('' as timestamp) as AGR_CUM_QTY_RCNL_DTTM,\ncast('' as decimal(18,4)) as AGR_CUM_QTY,\ncast('' as decimal(18,4)) as FIRM_ZN,\ncast('' as decimal(18,4)) as TRADE_OFF_ZN,\ncast('' as string) as FIRM_TRADE_OFF_ZN,\ncast('' as string) as EXCLN_IN_OUTLINE_AGMT_ITM,\ncast('' as string) as SHIPPING_INSTR,\ncast('' as decimal(18,4)) as TRGT_VAL_FOR_OUTLINE_AGMT,\ncast('' as decimal(18,4)) as NDEDT_INP_TAX,\ncast('' as decimal(18,4)) as STD_RLSE_ORDR_QTY,\ncast('' as timestamp) as PRC_DTRMN_DTTM,\ncast('' as string) as PRCHSNG_DOC_CAT,\ncast('' as string) as EFF_VAL_OF_ITM,\ncast('' as string) as ITM_AFFCT_CMMT,\ncast('' as string) as COND_GRP_WTH_SUP,\ncast('' as string) as ITM_DONT_QUAL_FOR_CASH_DISC,\ncast('' as string) as UPDT_GRP_FOR_STATS_UPDT,\ncast('' as decimal(18,4)) as NET_WT,\ncast('' as string) as UNIT_OF_WT,\ncast('' as string) as TAX_JURIS,\ncast('' as string) as PRT_RLVNT_SCHED_LIN_EXIST,\ncast('' as string) as SETLM_RESV_NUM,\ncast('' as string) as ITM_NUM_OF_THE_SETLM_RESV,\ncast('' as string) as QUAL_INSP_IN_CNNOT_BE_CHG,\ncast('' as string) as CNTL_KEY_FOR_QUAL_MGMT,\ncast('' as string) as INTNL_ARTCL_NUM,\ncast('' as string) as PRFT_CTR,\ncast('' as decimal(18,4)) as GRS_WT,\ncast('' as decimal(18,4)) as VOL,\ncast('' as string) as VOL_UNIT,\ncast('' as string) as PKG_NUM,\ncast('' as decimal(18,4)) as CUR_NOT_USED,\ncast('' as string) as HI_LVL_ITM_IN_PRCHSNG_DOC,\ncast('' as timestamp) as LTST_POSBL_GOODS_RCPT,\ncast('' as string) as CNFG,\ncast('' as string) as EVAL_RCPT_SETLM,\ncast('' as timestamp) as GR_BAS_SETLM_STRT_DTTM,\ncast('' as timestamp) as LAST_XMSN,\ncast('' as string) as SEQ_NUM,\ncast('' as string) as ALLC_TBL_ITM,\ncast('' as decimal(18,4)) as NUM_OF_PT,\ncast('' as string) as MATL_LDGR_ACT_AT_MATL_LVL,\ncast('' as decimal(18,4)) as MIN_RMN_SHLF_LIF,\ncast('' as string) as ITM_NUM_OF_RFQ,\ncast('' as string) as MATL_TYPE,\ncast('' as decimal(18,4)) as SBTOT_1_FROM_PRC_PCDR,\ncast('' as decimal(18,4)) as SBTOT_2_FROM_PRC_PCDR,\ncast('' as decimal(18,4)) as SBTOT_3_FROM_PRC_PCDR,\ncast('' as decimal(18,4)) as SBTOT_4_FROM_PRC_PCDR,\ncast('' as decimal(18,4)) as SBTOT_5_FROM_PRC_PCDR,\ncast('' as decimal(18,4)) as SBTOT_6_FROM_PRC_PCDR,\ncast('' as string) as PRCSG_KEY_FOR_SUB_ITM,\ncast('' as decimal(18,4)) as MAX_CUM_MATL_GO_AHEAD_QTY,\ncast('' as decimal(18,4)) as MAX_CUM_PRDTN_GO_AHEAD_QTY,\ncast('' as string) as RTRN_ITM,\ncast('' as string) as DELV_TYPE_FOR_RTRN_TO_SUP,\ncast('' as timestamp) as NEXT_FCST_DELV_SCHED_XMSN,\ncast('' as timestamp) as NEXT_JIT_DELV_SCHED_XMSN,\ncast('' as string) as VALUT_OF_SPL_STK,\ncast('' as decimal(18,4)) as REBT_BASIS_1,\ncast('' as timestamp) as INFLT_INDX_DTTM,\ncast('' as string) as NM_OF_REQSNR1,\ncast('' as string) as TIME_ZN_OF_RCPNT_LOC1,\ncast('' as string) as MRP_AREA,\ncast('' as string) as DOC_ITM,\ncast('' as string) as WRK_BRKDWN_STRC_ELMNT1,\ncast('' as string) as REQ_URGNCY,\ncast('' as string) as REQ_PRIR,\ncast('' as string) as CRM_SLS_ORDR_ITM_NUM,\ncast('' as decimal(18,4)) as CUM_GOODS_RECIPT,\ncast('' as int) as NUM_OF_SER_NUM,\ncast('' as string) as EXTRNL_SRT_NUM,\ncast('' as decimal(18,4)) as RETN_IN_PCT,\ncast('' as decimal(18,4)) as DOWN_PMT_PCT,\ncast('' as decimal(18,4)) as DOWN_PMT_AMT_IN_DOC_CRNCY,\ncast('' as timestamp) as DOWN_PMT_DUE_DTTM,\ncast('' as string) as CENT_CNTRC_ITM_NUM,\ncast('' as string) as INCO_LOC_1_PLACE_OF_DEST,\ncast('' as string) as INCO_LOC_2_PLACE_OF_DELV,\ncast('' as string) as INCO_DEVT_PLACE_OF_DEST,\ncast('' as decimal(18,4)) as STAT_VAL_FOR_FRGN_TRD,\ncast('' as string) as PROD_TYPE_GRP,\ncast('' as string) as ITM_NUM_FOR_RQST_FOR_QUTN,\ncast('' as decimal(18,4)) as TRGT_VAL_AT_ITM_LVL_IN_PRCM,\ncast('' as decimal(18,4)) as EXPTD_VAL_OF_OVRL_LMT,\ncast('' as decimal(18,4)) as OVRL_LMT,\ncast('' as timestamp) as WKA_WRK_PER_STRT_DTTM,\ncast('' as timestamp) as WKA_WRK_PER_END_DTTM,\ncast('' as decimal(18,4)) as WKA_PCT_OF_WG,\ncast('' as string) as WRK_TIME_IN_HRS,\ncast('' as timestamp) as DATA_FIL_VAL_FOR_DATA_AGE,\ncast('' as decimal(18,4)) as CMT_QTY,\ncast('' as string) as HI_LVL_ITM_IN_PRTL_QTY_REJ,\ncast('' as string) as ANNXNG_PKG_KEY,\ncast('' as string) as EXTD_KEY_FOR_ANNXNG_PKG,\ncast('' as timestamp) as BASE_DTTM,\ncast('' as string) as ANNXNG_DT_TYPE,\ncast('' as timestamp) as ANNXNG_STRT_DTTM,\ncast('' as decimal(18,4)) as DEVT_PCT,\ncast('' as string) as QUTN_ITM_NUM,\ncast('' as string) as DELV_PRIR,\ncast('' as string) as SLS_DOC_ITM,\ncast('' as string) as VEND_CNFRM_TYPE,\ncast('' as timestamp) as DOC_DTTM,\ncast('' as int) as ARUN_ORDR_PRIR,\ncast('' as string) as WRK_BRKDWN_STRC_ELMNT2,\ncast('' as string) as RSN_FOR_ORD,\ncast('' as int) as DAI_ETL_ID,\ncast('' as timestamp) as DAI_CRT_DTTM,\ncast('' as timestamp) as DAI_UPDT_DTTM,\ncast('' as timestamp) as _l0_upt_,\n cast('' as timestamp) as _l1_upt_,\n cast('' as string) as _pk_md5_,\n cast('' as string) as _pk_ \n limit 0"
    )

    return df1