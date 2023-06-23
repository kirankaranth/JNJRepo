from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, MANDT_FILTER: DataFrame) -> DataFrame:
    return MANDT_FILTER\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("ACTG_DOC_NUM", col("OLDOCO"))\
        .withColumn(
          "FISC_YR",
          lit(
            "#"
          )
        )\
        .withColumn("DOC_ITM_IN_INVC_DOC", col("OLITM"))\
        .withColumn("PRCHSNG_DOC_NUM", trim(col("oldoco")))\
        .withColumn("ITM_NUM_OF_PRCHSNG_DOC", trim(col("ollnid")))\
        .withColumn("SEQ_NUM_OF_ACCT_ASGNMT", lit(None).cast(StringType()))\
        .withColumn("MATL_NUM", trim(col("ollitm")))\
        .withColumn("VALUT_AREA", lit(None).cast(StringType()))\
        .withColumn("VALUT_TYPE", lit(None).cast(StringType()))\
        .withColumn("CO_CD", lit(None).cast(StringType()))\
        .withColumn("PLNT", trim(col("OLMCU")))\
        .withColumn("AMT_IN_DOC_CRNCY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("DR_CR_IN", lit(None).cast(StringType()))\
        .withColumn("TAX_ON_SLS_PRCH_CD", lit(None).cast(StringType()))\
        .withColumn("TAX_JURIS", lit(None).cast(StringType()))\
        .withColumn("QTY", trim(col("OLUREC")).cast(DecimalType(18, 4)))\
        .withColumn("PRCH_ORDR_UNIT_OF_MEAS", lit(None).cast(StringType()))\
        .withColumn("QTY_PRCH_ORDR_PRC_UNIT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("ORDR_PRC_UNIT_PRCHSNG", lit(None).cast(StringType()))\
        .withColumn("TOT_VALUT_STK", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("TOT_VALUT_STK_PREV_PER", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("BASE_UNIT_OF_MEAS", lit(None).cast(StringType()))\
        .withColumn("ITM_CAT_IN_PRCHSNG_DOC", lit(None).cast(StringType()))\
        .withColumn("ACCT_ASGNMT_CAT", lit(None).cast(StringType()))\
        .withColumn("VALUT_CLS", lit(None).cast(StringType()))\
        .withColumn("FNL_INVC_IN", lit(None).cast(StringType()))\
        .withColumn("IN_UPDT_PRCH_ORDR_HIST", lit(None).cast(StringType()))\
        .withColumn("UPDT_PRCH_ORDR_DELV_COST", lit(None).cast(StringType()))\
        .withColumn("IN_SUBSQ_DR_CR", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_PRC", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_QTY", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_DT", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_ORDR_PRC_QTY", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_PROJ_BGT", lit(None).cast(StringType()))\
        .withColumn("MAN_BLOK_RSN", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_ITM_AMT", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_QUAL", lit(None).cast(StringType()))\
        .withColumn("BLOK_RSN_ENHNC_FLD", lit(None).cast(StringType()))\
        .withColumn("PSTNG_STRNG_FOR_VAL", lit(None).cast(StringType()))\
        .withColumn("REF_DOC_NUM", trim(col("OLOORN")))\
        .withColumn("IN_DOC_PSTD_PREV_PER", lit(None).cast(StringType()))\
        .withColumn("DELV_COST_SHR_ITM_VAL", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("COND_TYPE", lit(None).cast(StringType()))\
        .withColumn("VAL_TOT_VALUT_STK", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("VAL_TOT_STK_PREV_PER", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("LIFO_FIFO_RLVNT", lit(None).cast(StringType()))\
        .withColumn("DOC_NUM_REF_DOC", lit(None).cast(StringType()))\
        .withColumn("FISC_YR_OF_CUR_PER", lit(None).cast(StringType()))\
        .withColumn("ITM_OF_REF_DOC", lit(None).cast(StringType()))\
        .withColumn("MATL_RSPCT_OF_STK_IS_MNG", lit(None).cast(StringType()))\
        .withColumn("QTY_INVC_IN_INVC_PO_ORDR", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("QTY_INVC_IN_INVC_PO_PRC", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("INVC_AMT_DOC_CRNCY_INVC", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("TYPE_OF_VEND_ERR", lit(None).cast(StringType()))\
        .withColumn("ACTV_CD_FOR_GRS_INCM_TAX", lit(None).cast(StringType()))\
        .withColumn("RGN", lit(None).cast(StringType()))\
        .withColumn("DSTN_TYPE_FOR_EMP_TAX", lit(None).cast(StringType()))\
        .withColumn("PKG_NUM_OF_SRVC", lit(None).cast(StringType()))\
        .withColumn("LINE_NUM_OF_SRVC", lit(None).cast(StringType()))\
        .withColumn("ITM_TEXT", lit(None).cast(StringType()))\
        .withColumn("LINE_ITM_NOT_LIAB_DISC", lit(None).cast(StringType()))\
        .withColumn("CORR_IN", lit(None).cast(StringType()))\
        .withColumn("IN_INVC_ITM_PRCS", lit(None).cast(StringType()))\
        .withColumn("STEP_NUM", lit(None).cast(StringType()))\
        .withColumn("COND_CNTR", lit(None).cast(StringType()))\
        .withColumn("STK_PSTNG_LINE_INCM_INVC", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("STK_PSTNG_INCM_INVC_PREV", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("STK_PSTNG_INCM_PREV_YR", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("CLRNG_GR_IR_PSTNG_EXTRNL", lit(None).cast(StringType()))\
        .withColumn("ACCT_NUM_OF_VEND_OR_CR", lit(None).cast(StringType()))\
        .withColumn("NUM_BILL_LDNG_GOODS_RCPT", lit(None).cast(StringType()))\
        .withColumn("UPDT_MLT_ACCT_ASGNMT", lit(None).cast(StringType()))\
        .withColumn("CMPNT_RSN_IN_INVC", lit(None).cast(StringType()))\
        .withColumn("RETN_AMT_IN_DOC_CRNCY", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("RETN_IN_PCT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("RETN_DUE_DTTM", to_timestamp(lit(None)))\
        .withColumn("TAX_RDCTN_FOR_RETN", lit(None).cast(StringType()))\
        .withColumn("CASH_LDGR_EXP_REVN_ACCT", lit(None).cast(StringType()))\
        .withColumn("NUM_OF_PRIN_PRCH_AGMT", lit(None).cast(StringType()))\
        .withColumn("ITM_NUM_PRIN_PRCH_AGMT", lit(None).cast(StringType()))\
        .withColumn("CENT_CNTRC", lit(None).cast(StringType()))\
        .withColumn("CENT_CNTRC_ITM_NUM", lit(None).cast(StringType()))\
        .withColumn("CONT_ITM_CAT_PRCHSNG_DOC", lit(None).cast(StringType()))\
        .withColumn("ITM_KEY_FOR_ESOA_MSG", lit(None).cast(StringType()))\
        .withColumn("BTCH_NUM", lit(None).cast(StringType()))\
        .withColumn("ORIG_INVC_ITM", lit(None).cast(StringType()))\
        .withColumn("GRP_CHAR_FOR_INVC_VERIF", lit(None).cast(StringType()))\
        .withColumn("IN_FOR_DIFF_INVC", lit(None).cast(StringType()))\
        .withColumn("DIFF_AMT", lit(None).cast(DecimalType(18, 4)))\
        .withColumn("CMMDTY_REPRC_INVC_VERIF", lit(None).cast(StringType()))\
        .withColumn("INTRNL_LIC_NUM", lit(None).cast(StringType()))\
        .withColumn("ITM_NUM", lit(None).cast(StringType()))\
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
