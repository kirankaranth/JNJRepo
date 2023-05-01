from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_doc_itm_incm_invc_hm2.config.ConfigStore import *
from sap_md_doc_itm_incm_invc_hm2.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, MANDT_FILTER: DataFrame) -> DataFrame:
    return MANDT_FILTER\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("ACTG_DOC_NUM", col("BELNR"))\
        .withColumn("FISC_YR", col("GJAHR"))\
        .withColumn("DOC_ITM_IN_INVC_DOC", col("BUZEI"))\
        .withColumn("PRCHSNG_DOC_NUM", trim(col("EBELN")))\
        .withColumn("ITM_NUM_OF_PRCHSNG_DOC", trim(col("EBELP")))\
        .withColumn("SEQ_NUM_OF_ACCT_ASGNMT", trim(col("ZEKKN")))\
        .withColumn("MATL_NUM", trim(col("MATNR")))\
        .withColumn("VALUT_AREA", trim(col("BWKEY")))\
        .withColumn("VALUT_TYPE", trim(col("BWTAR")))\
        .withColumn("CO_CD", trim(col("BUKRS")))\
        .withColumn("PLNT", trim(col("WERKS")))\
        .withColumn("AMT_IN_DOC_CRNCY", trim(col("WRBTR")).cast(DecimalType(18, 4)))\
        .withColumn("DR_CR_IN", trim(col("SHKZG")))\
        .withColumn("TAX_ON_SLS_PRCH_CD", trim(col("MWSKZ")))\
        .withColumn("TAX_JURIS", trim(col("TXJCD")))\
        .withColumn("QTY", trim(col("MENGE")).cast(DecimalType(18, 4)))\
        .withColumn("PRCH_ORDR_UNIT_OF_MEAS", trim(col("BSTME")))\
        .withColumn("QTY_PRCH_ORDR_PRC_UNIT", trim(col("BPMNG")).cast(DecimalType(18, 4)))\
        .withColumn("ORDR_PRC_UNIT_PRCHSNG", trim(col("BPRME")))\
        .withColumn("TOT_VALUT_STK", trim(col("LBKUM")).cast(DecimalType(18, 4)))\
        .withColumn("TOT_VALUT_STK_PREV_PER", trim(col("VRKUM")).cast(DecimalType(18, 4)))\
        .withColumn("BASE_UNIT_OF_MEAS", trim(col("MEINS")))\
        .withColumn("ITM_CAT_IN_PRCHSNG_DOC", trim(col("PSTYP")))\
        .withColumn("ACCT_ASGNMT_CAT", trim(col("KNTTP")))\
        .withColumn("VALUT_CLS", trim(col("BKLAS")))\
        .withColumn("FNL_INVC_IN", trim(col("EREKZ")))\
        .withColumn("IN_UPDT_PRCH_ORDR_HIST", trim(col("EXKBE")))\
        .withColumn("UPDT_PRCH_ORDR_DELV_COST", trim(col("XEKBZ")))\
        .withColumn("IN_SUBSQ_DR_CR", trim(col("TBTKZ")))\
        .withColumn("BLOK_RSN_PRC", trim(col("SPGRP")))\
        .withColumn("BLOK_RSN_QTY", trim(col("SPGRM")))\
        .withColumn("BLOK_RSN_DT", trim(col("SPGRT")))\
        .withColumn("BLOK_RSN_ORDR_PRC_QTY", trim(col("SPGRG")))\
        .withColumn("BLOK_RSN_PROJ_BGT", trim(col("SPGRV")))\
        .withColumn("MAN_BLOK_RSN", trim(col("SPGRQ")))\
        .withColumn("BLOK_RSN_ITM_AMT", trim(col("SPGRS")))\
        .withColumn("BLOK_RSN_QUAL", trim(col("SPGRC")))\
        .withColumn("BLOK_RSN_ENHNC_FLD", trim(col("SPGREXT")))\
        .withColumn("PSTNG_STRNG_FOR_VAL", trim(col("BUSTW")))\
        .withColumn("REF_DOC_NUM", trim(col("XBLNR")))\
        .withColumn("IN_DOC_PSTD_PREV_PER", expr(Config.IN_DOC_PSTD_PREV_PER).cast(StringType()))\
        .withColumn("DELV_COST_SHR_ITM_VAL", trim(col("BNKAN")).cast(DecimalType(18, 4)))\
        .withColumn("COND_TYPE", trim(col("KSCHL")))\
        .withColumn("VAL_TOT_VALUT_STK", trim(col("SALK3")).cast(DecimalType(18, 4)))\
        .withColumn("VAL_TOT_STK_PREV_PER", trim(col("VMSAL")).cast(DecimalType(18, 4)))\
        .withColumn("LIFO_FIFO_RLVNT", expr(Config.LIFO_FIFO_RLVNT).cast(StringType()))\
        .withColumn("DOC_NUM_REF_DOC", trim(col("LFBNR")))\
        .withColumn("FISC_YR_OF_CUR_PER", trim(col("LFGJA")))\
        .withColumn("ITM_OF_REF_DOC", trim(col("LFPOS")))\
        .withColumn("MATL_RSPCT_OF_STK_IS_MNG", trim(col("MATBF")))\
        .withColumn("QTY_INVC_IN_INVC_PO_ORDR", trim(col("RBMNG")).cast(DecimalType(18, 4)))\
        .withColumn("QTY_INVC_IN_INVC_PO_PRC", trim(col("BPRBM")).cast(DecimalType(18, 4)))\
        .withColumn("INVC_AMT_DOC_CRNCY_INVC", trim(col("RBWWR")).cast(DecimalType(18, 4)))\
        .withColumn("TYPE_OF_VEND_ERR", trim(col("LFEHL")))\
        .withColumn("ACTV_CD_FOR_GRS_INCM_TAX", trim(col("GRICD")))\
        .withColumn("RGN", trim(col("GRIRG")))\
        .withColumn("DSTN_TYPE_FOR_EMP_TAX", trim(col("GITYP")))\
        .withColumn("PKG_NUM_OF_SRVC", trim(col("PACKNO")))\
        .withColumn("LINE_NUM_OF_SRVC", trim(col("INTROW")))\
        .withColumn("ITM_TEXT", trim(col("SGTXT")))\
        .withColumn("LINE_ITM_NOT_LIAB_DISC", trim(col("XSKRL")))\
        .withColumn("CORR_IN", trim(col("KZMEK")))\
        .withColumn("IN_INVC_ITM_PRCS", trim(col("MRMOK")))\
        .withColumn("STEP_NUM", trim(col("STUNR")))\
        .withColumn("COND_CNTR", trim(col("ZAEHK")))\
        .withColumn("STK_PSTNG_LINE_INCM_INVC", trim(col("STOCK_POSTING")).cast(DecimalType(18, 4)))\
        .withColumn("STK_PSTNG_INCM_INVC_PREV", trim(col("STOCK_POSTING_PP")).cast(DecimalType(18, 4)))\
        .withColumn("STK_PSTNG_INCM_PREV_YR", trim(col("STOCK_POSTING_PY")).cast(DecimalType(18, 4)))\
        .withColumn("CLRNG_GR_IR_PSTNG_EXTRNL", trim(col("WEREC")))\
        .withColumn("ACCT_NUM_OF_VEND_OR_CR", trim(col("LIFNR")))\
        .withColumn("NUM_BILL_LDNG_GOODS_RCPT", trim(col("FRBNR")))\
        .withColumn("UPDT_MLT_ACCT_ASGNMT", trim(col("XHISTMA")))\
        .withColumn("CMPNT_RSN_IN_INVC", trim(col("COMPLAINT_REASON")))\
        .withColumn("RETN_AMT_IN_DOC_CRNCY", trim(col("RETAMT_FC")).cast(DecimalType(18, 4)))\
        .withColumn("RETN_IN_PCT", trim(col("RETPC")).cast(DecimalType(18, 4)))\
        .withColumn(
          "RETN_DUE_DTTM",
          when((col("RETDUEDT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("RETDUEDT"), "yyyyMMdd"))
        )\
        .withColumn("TAX_RDCTN_FOR_RETN", expr(Config.TAX_RDCTN_FOR_RETN).cast(StringType()))\
        .withColumn("CASH_LDGR_EXP_REVN_ACCT", trim(col("RE_ACCOUNT")))\
        .withColumn("NUM_OF_PRIN_PRCH_AGMT", trim(col("ERP_CONTRACT_ID")))\
        .withColumn("ITM_NUM_PRIN_PRCH_AGMT", trim(col("ERP_CONTRACT_ITM")))\
        .withColumn("CENT_CNTRC", trim(col("SRM_CONTRACT_ID")))\
        .withColumn("CENT_CNTRC_ITM_NUM", trim(col("SRM_CONTRACT_ITM")))\
        .withColumn("CONT_ITM_CAT_PRCHSNG_DOC", trim(col("CONT_PSTYP")))\
        .withColumn("ITM_KEY_FOR_ESOA_MSG", trim(col("SRVMAPKEY")))\
        .withColumn("BTCH_NUM", trim(col("CHARG")))\
        .withColumn("ORIG_INVC_ITM", trim(col("INV_ITM_ORIGIN")))\
        .withColumn("GRP_CHAR_FOR_INVC_VERIF", trim(col("INVREL")))\
        .withColumn("IN_FOR_DIFF_INVC", trim(col("XDINV")))\
        .withColumn("DIFF_AMT", trim(col("DIFF_AMOUNT")).cast(DecimalType(18, 4)))\
        .withColumn("CMMDTY_REPRC_INVC_VERIF", trim(col("XCPRF")))\
        .withColumn("INTRNL_LIC_NUM", trim(col("LICNO")))\
        .withColumn("ITM_NUM", trim(col("ZEILE")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'ACTG_DOC_NUM', ACTG_DOC_NUM, 'FISC_YR', FISC_YR, 'DOC_ITM_IN_INVC_DOC', DOC_ITM_IN_INVC_DOC)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'ACTG_DOC_NUM', ACTG_DOC_NUM, 'FISC_YR', FISC_YR, 'DOC_ITM_IN_INVC_DOC', DOC_ITM_IN_INVC_DOC)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))
