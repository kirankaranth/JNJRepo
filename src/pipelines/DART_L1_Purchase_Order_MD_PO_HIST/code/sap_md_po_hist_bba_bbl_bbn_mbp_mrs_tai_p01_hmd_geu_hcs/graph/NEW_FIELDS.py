from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.config.ConfigStore import *
from sap_md_po_hist_bba_bbl_bbn_mbp_mrs_tai_p01_hmd_geu_hcs.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PO_NUM", col("EBELN"))\
        .withColumn("PO_LINE_NBR", col("EBELP"))\
        .withColumn("PO_SEQ_NBR", col("ZEKKN"))\
        .withColumn("EV_TYPE_CO", col("VGABE"))\
        .withColumn("MATL_MVMT_YR", trim(col("GJAHR")).cast(IntegerType()))\
        .withColumn("MATL_MVMT_NUM", col("BELNR"))\
        .withColumn("MATL_MVMT_SEQ_NBR", col("BUZEI"))\
        .withColumn("PO_HIST_CAT_CD", trim(col("BEWTP")))\
        .withColumn("MVMT_TYPE_CD", trim(col("BWART")))\
        .withColumn(
          "PSTNG_DTTM",
          when((col("budat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("budat"), "yyyyMMdd"))
        )\
        .withColumn("RECV_EA_QTY", trim(col("MENGE")).cast(DecimalType(18, 4)))\
        .withColumn("ENT_MATL_NUM", trim(col("MATNR")))\
        .withColumn("PLNT_CD", trim(col("WERKS")))\
        .withColumn(
          "UNIQ_KEY_ID",
          lit(
            "#"
          )
        )\
        .withColumn("MVMT_TYPE", trim(col("BWART")))\
        .withColumn("QTY_IN_PRCH_ORDR_PRC_UNIT", trim(col("BPMNG")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_IN_LCL_CRNCY1", trim(col("DMBTR")).cast(DecimalType(18, 4)))\
        .withColumn("CRNCY_KEY", trim(col("WAERS")))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_IN_LCL_CRNCY", trim(col("AREWR")).cast(DecimalType(18, 4)))\
        .withColumn("GOODS_RCPT_BLOK_STK_IN_OU", trim(col("WESBS")).cast(DecimalType(18, 4)))\
        .withColumn("QTY_IN_GR_BLOK_STK", trim(col("BPWES")).cast(DecimalType(18, 4)))\
        .withColumn("DEBIT_OR_CREDIT_IN", trim(col("SHKZG")))\
        .withColumn("VALUT_TYPE", trim(col("BWTAR")))\
        .withColumn("DELV_CMPLT_IN", trim(col("ELIKZ")))\
        .withColumn("REF_DOC_NUM", trim(col("XBLNR")))\
        .withColumn("FISC_YR_OF_A_REF_DOC", trim(col("LFGJA")))\
        .withColumn("DOC_NUM_OF_A_REF_DOC", trim(col("LFBNR")))\
        .withColumn("ITM_OF_A_REF_DOC", trim(col("LFPOS")))\
        .withColumn("RSN_FOR_MVMT", trim(col("GRUND")))\
        .withColumn(
          "ACTG_DOC_ENT_DTTM",
          when((col("cpudt") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("cpudt"), col("cputm")), "yyyyMMddHHmmss"))
        )\
        .withColumn("INVC_VAL_ENT", trim(col("REEWR")).cast(DecimalType(18, 4)))\
        .withColumn("CMPLI_WTH_SHIPPING_INSTR", trim(col("EVERE")))\
        .withColumn("INVC_VAL_IN_FRGN_CRNCY", trim(col("REFWR")).cast(DecimalType(18, 4)))\
        .withColumn("RVSL_OF_GR_ALLW", trim(col("XWSBR")))\
        .withColumn("SEQ_NUM_OF_SUP_CNFRM", trim(col("ETENS")))\
        .withColumn("NUM_OF_THE_DOC_COND", trim(col("KNUMV")))\
        .withColumn("TAX_ON_SLS_PRCH_CD", trim(col("MWSKZ")))\
        .withColumn("TAX_RPTG_CTRY_REGN", expr(Config.TAX_RPTG_CTRY_REGN))\
        .withColumn("QTY_IN_UOM_FROM_DELV_NOTE", trim(col("LSMNG")).cast(DecimalType(18, 4)))\
        .withColumn("UOM_FROM_DELV_NOTE", trim(col("LSMEH")))\
        .withColumn("MATL_NUM", trim(col("EMATN")))\
        .withColumn("CLRNG_VAL_ON_GR_IR_CLRNG_ACCT", trim(col("AREWW")).cast(DecimalType(18, 4)))\
        .withColumn("LCL_CRNCY_KEY", trim(col("HSWAE")))\
        .withColumn("QTY1", trim(col("BAMNG")).cast(DecimalType(18, 4)))\
        .withColumn("BTCH_NUM", trim(col("CHARG")))\
        .withColumn(
          "DOC_DTTM",
          when((col("bldat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("bldat"), "yyyyMMdd"))
        )\
        .withColumn("CALC_OF_VAL_OPEN", trim(col("XWOFF")))\
        .withColumn("UNPLAN_ACCT_ASGNMT", trim(col("XUNPL")))\
        .withColumn("NM_OF_PRSN_RESP_FOR_CREAT_OBJ", trim(col("ERNAM")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'PO_SEQ_NBR', PO_SEQ_NBR, 'EV_TYPE_CO', EV_TYPE_CO, 'MATL_MVMT_YR', MATL_MVMT_YR, 'MATL_MVMT_NUM', MATL_MVMT_NUM, 'MATL_MVMT_SEQ_NBR', MATL_MVMT_SEQ_NBR, 'UNIQ_KEY_ID', UNIQ_KEY_ID)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'PO_NUM', PO_NUM, 'PO_LINE_NBR', PO_LINE_NBR, 'PO_SEQ_NBR', PO_SEQ_NBR, 'EV_TYPE_CO', EV_TYPE_CO, 'MATL_MVMT_YR', MATL_MVMT_YR, 'MATL_MVMT_NUM', MATL_MVMT_NUM, 'MATL_MVMT_SEQ_NBR', MATL_MVMT_SEQ_NBR, 'UNIQ_KEY_ID', UNIQ_KEY_ID)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("SRVC_NUM", trim(col("srvpos")))\
        .withColumn("PKG_NUM_OF_SRVC", trim(col("packno")))\
        .withColumn("LINE_NUM_OF_SRVC", trim(col("introw")))\
        .withColumn("NUM_OF_PO_ACCT_ASGNMT", trim(col("bekkn")))\
        .withColumn("RTRN_IN", trim(col("lemin")))\
        .withColumn("CLRNG_VAL_ON_GR_IR_ACCT", trim(col("arewb")).cast(DecimalType(18, 4)))\
        .withColumn("INVC_AMT_IN_PO_CRNCY", trim(col("rewrb")).cast(DecimalType(18, 4)))\
        .withColumn("SAP_RLSE", trim(col("saprl")))\
        .withColumn("QTY2", expr(Config.QTY2))\
        .withColumn("QTY_IN_PO_PRC_UNIT", expr(Config.QTY_IN_PO_PRC_UNIT))\
        .withColumn("AMT_IN_LCL_CRNCY", expr(Config.AMT_IN_LCL_CRNCY))\
        .withColumn("AMT_IN_DOC_CRNCY", expr(Config.AMT_IN_DOC_CRNCY))\
        .withColumn("VALUT_GOODS_RCPT_BLOK_STK", expr(Config.VALUT_GOODS_RCPT_BLOK_STK))\
        .withColumn("QTY_IN_VALUT_GR_BLOK_STK", expr(Config.QTY_IN_VALUT_GR_BLOK_STK))\
        .withColumn("ACC_AT_ORIG", expr(Config.ACC_AT_ORIG))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY", expr(Config.GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY))\
        .withColumn("EXCH_RT_DIFF_AMT", expr(Config.EXCH_RT_DIFF_AMT))\
        .withColumn("RETN_AMT_IN_DOC_CRNCY", expr(Config.RETN_AMT_IN_DOC_CRNCY))\
        .withColumn("RETN_AMT_IN_CO_CD_CRNCY", expr(Config.RETN_AMT_IN_CO_CD_CRNCY))\
        .withColumn("PSTD_RETN_AMT_IN_DOC_CRNCY", expr(Config.PSTD_RETN_AMT_IN_DOC_CRNCY))\
        .withColumn("PSTD_SCTY_RETN_AMT", expr(Config.PSTD_SCTY_RETN_AMT))\
        .withColumn("MLT_ACCT_ASGNMT", expr(Config.MLT_ACCT_ASGNMT))\
        .withColumn("EXCH_RT", expr(Config.EXCH_RT))\
        .withColumn("ORIG_OF_AN_INVC_ITM", expr(Config.ORIG_OF_AN_INVC_ITM))\
        .withColumn("DELV", expr(Config.DELV))\
        .withColumn("DELV_ITM", expr(Config.DELV_ITM))\
        .withColumn("STK_SGMNT", expr(Config.STK_SGMNT))\
        .withColumn("UOM_FROM_SRVC_ENT_SHT", expr(Config.UOM_FROM_SRVC_ENT_SHT))\
        .withColumn("LOGL_SYS", expr(Config.LOGL_SYS))\
        .withColumn("PCDR_FOR_UPDT_SCHED_LINE_QTY", expr(Config.PCDR_FOR_UPDT_SCHED_LINE_QTY))\
        .withColumn("QTY_IN_PAREL_UNIT_OF_MEAS", expr(Config.QTY_IN_PAREL_UNIT_OF_MEAS))\
        .withColumn("GOODS_RCPT_BLOK_STK", expr(Config.GOODS_RCPT_BLOK_STK))\
        .withColumn("TYPE_OF_PAREL_UNIT_OF_MEAS", expr(Config.TYPE_OF_PAREL_UNIT_OF_MEAS))\
        .withColumn("VAL_GOODS_RCPT_BLOK_STK", expr(Config.VAL_GOODS_RCPT_BLOK_STK))\
        .withColumn("DPRE_CMPLT_FL", trim(col("j_sc_die_comp_f")))\
        .withColumn("SEASN_YR", expr(Config.SEASN_YR))\
        .withColumn("SEASN", expr(Config.SEASN))\
        .withColumn("FSHN_CLCT", expr(Config.FSHN_CLCT))\
        .withColumn("FSHN_Theme", expr(Config.FSHN_Theme))\
        .withColumn("QTY3", expr(Config.QTY3))\
        .withColumn("CHAR_VAL_1", expr(Config.CHAR_VAL_1))\
        .withColumn("CHAR_VAL_2", expr(Config.CHAR_VAL_2))\
        .withColumn("CHAR_VAL_3", expr(Config.CHAR_VAL_3))\
        .withColumn("AMT_IN_DOC_CRNCY1", expr(Config.AMT_IN_DOC_CRNCY))
