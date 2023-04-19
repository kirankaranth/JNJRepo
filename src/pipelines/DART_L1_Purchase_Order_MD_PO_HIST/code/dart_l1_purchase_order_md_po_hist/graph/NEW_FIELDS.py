from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *
from dart_l1_purchase_order_md_po_hist.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PO_NUM", col("EBELN"))\
        .withColumn("PO_LINE_NBR", col("EBELP"))\
        .withColumn("PO_SEQ_NBR", col("ZEKKN"))\
        .withColumn("EV_TYPE_CO", col("VGABE"))\
        .withColumn("MATL_MVMT_YR", col("GJAHR").cast(IntegerType()))\
        .withColumn("MATL_MVMT_NUM", col("BELNR"))\
        .withColumn("MATL_MVMT_SEQ_NBR", col("BUZEI"))\
        .withColumn("PO_HIST_CAT_CD", col("BEWTP"))\
        .withColumn("MVMT_TYPE_CD", col("BWART"))\
        .withColumn(
          "PSTNG_DTTM",
          when((col("budat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("budat"), "yyyyMMdd"))
        )\
        .withColumn("RECV_EA_QTY", col("MENGE").cast(DecimalType(18, 4)))\
        .withColumn("ENT_MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn(
          "UNIQ_KEY_ID",
          lit(
            "#"
          )
        )\
        .withColumn("MVMT_TYPE", col("BWART"))\
        .withColumn("QTY_IN_PRCH_ORDR_PRC_UNIT", col("BPMNG").cast(DecimalType(18, 4)))\
        .withColumn("AMT_IN_LCL_CRNCY1", col("DMBTR").cast(DecimalType(18, 4)))\
        .withColumn("CRNCY_KEY", col("WAERS"))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_IN_LCL_CRNCY", col("AREWR").cast(DecimalType(18, 4)))\
        .withColumn("GOODS_RCPT_BLOK_STK_IN_OU", col("WESBS").cast(DecimalType(18, 4)))\
        .withColumn("QTY_IN_GR_BLOK_STK", col("BPWES").cast(DecimalType(18, 4)))\
        .withColumn("DEBIT_OR_CREDIT_IN", col("SHKZG"))\
        .withColumn("VALUT_TYPE", col("BWTAR"))\
        .withColumn("DELV_CMPLT_IN", col("ELIKZ"))\
        .withColumn("REF_DOC_NUM", col("XBLNR"))\
        .withColumn("FISC_YR_OF_A_REF_DOC", col("LFGJA"))\
        .withColumn("DOC_NUM_OF_A_REF_DOC", col("LFBNR"))\
        .withColumn("ITM_OF_A_REF_DOC", col("LFPOS"))\
        .withColumn("RSN_FOR_MVMT", col("GRUND"))\
        .withColumn(
          "ACTG_DOC_ENT_DTTM",
          when((col("cpudt") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("cpudt"), col("cputm")), "yyyyMMddHHmmss"))
        )\
        .withColumn("INVC_VAL_ENT", col("REEWR").cast(DecimalType(18, 4)))\
        .withColumn("CMPLI_WTH_SHIPPING_INSTR", col("EVERE"))\
        .withColumn("INVC_VAL_IN_FRGN_CRNCY", col("REFWR").cast(DecimalType(18, 4)))\
        .withColumn("RVSL_OF_GR_ALLW", col("XWSBR"))\
        .withColumn("SEQ_NUM_OF_SUP_CNFRM", col("ETENS"))\
        .withColumn("NUM_OF_THE_DOC_COND", col("KNUMV"))\
        .withColumn("TAX_ON_SLS_PRCH_CD", col("MWSKZ"))\
        .withColumn("TAX_RPTG_CTRY_REGN", lit(None))\
        .withColumn("QTY_IN_UOM_FROM_DELV_NOTE", col("LSMNG").cast(DecimalType(18, 4)))\
        .withColumn("UOM_FROM_DELV_NOTE", col("LSMEH"))\
        .withColumn("MATL_NUM", col("EMATN"))\
        .withColumn("CLRNG_VAL_ON_GR_IR_CLRNG_ACCT", col("AREWW").cast(DecimalType(18, 4)))\
        .withColumn("LCL_CRNCY_KEY", col("HSWAE"))\
        .withColumn("QTY1", col("BAMNG").cast(DecimalType(18, 4)))\
        .withColumn("BTCH_NUM", col("CHARG"))\
        .withColumn(
          "DOC_DTTM",
          when((col("bldat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("bldat"), "yyyyMMdd"))
        )\
        .withColumn("CALC_OF_VAL_OPEN", col("XWOFF"))\
        .withColumn("UNPLAN_ACCT_ASGNMT", col("XUNPL"))\
        .withColumn("NM_OF_PRSN_RESP_FOR_CREAT_OBJ", col("ERNAM"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID).cast(IntegerType()))\
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
        .withColumn("QTY2", trim(col("menge_pop")).cast(DecimalType(18, 4)))\
        .withColumn("QTY_IN_PO_PRC_UNIT", trim(col("bpmng_pop")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_IN_LCL_CRNCY", trim(col("dmbtr_pop")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_IN_DOC_CRNCY", trim(col("wrbtr_pop")).cast(DecimalType(18, 4)))\
        .withColumn("VALUT_GOODS_RCPT_BLOK_STK", trim(col("wesbb")).cast(DecimalType(18, 4)))\
        .withColumn("QTY_IN_VALUT_GR_BLOK_STK", trim(col("bpweb")).cast(DecimalType(18, 4)))\
        .withColumn("ACC_AT_ORIG", trim(col("weora")))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY", trim(col("arewr_pop")).cast(DecimalType(18, 4)))\
        .withColumn("EXCH_RT_DIFF_AMT", trim(col("kudif")).cast(DecimalType(18, 4)))\
        .withColumn("RETN_AMT_IN_DOC_CRNCY", lit(None))\
        .withColumn("RETN_AMT_IN_CO_CD_CRNCY", lit(None))\
        .withColumn("PSTD_RETN_AMT_IN_DOC_CRNCY", lit(None))\
        .withColumn("PSTD_SCTY_RETN_AMT", lit(None))\
        .withColumn("MLT_ACCT_ASGNMT", lit(None))\
        .withColumn("EXCH_RT", lit(None))\
        .withColumn("ORIG_OF_AN_INVC_ITM", lit(None))\
        .withColumn("DELV", lit(None))\
        .withColumn("DELV_ITM", lit(None))\
        .withColumn("STK_SGMNT", lit(None))\
        .withColumn("UOM_FROM_SRVC_ENT_SHT", lit(None))\
        .withColumn("LOGL_SYS", lit(None))\
        .withColumn("PCDR_FOR_UPDT_SCHED_LINE_QTY", trim(col("et_upd")))\
        .withColumn("QTY_IN_PAREL_UNIT_OF_MEAS", lit(None))\
        .withColumn("GOODS_RCPT_BLOK_STK", lit(None))\
        .withColumn("TYPE_OF_PAREL_UNIT_OF_MEAS", lit(None))\
        .withColumn("VAL_GOODS_RCPT_BLOK_STK", lit(None))\
        .withColumn("DPRE_CMPLT_FL", trim(col("j_sc_die_comp_f")))\
        .withColumn("SEASN_YR", lit(None))\
        .withColumn("SEASN", lit(None))\
        .withColumn("FSHN_CLCT", lit(None))\
        .withColumn("FSHN_Theme", lit(None))\
        .withColumn("QTY3", lit(None))\
        .withColumn("CHAR_VAL_1", lit(None))\
        .withColumn("CHAR_VAL_2", lit(None))\
        .withColumn("CHAR_VAL_3", lit(None))\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("AMT_IN_DOC_CRNCY1", trim(col("WRBTR")).cast(DecimalType(18, 4)))
