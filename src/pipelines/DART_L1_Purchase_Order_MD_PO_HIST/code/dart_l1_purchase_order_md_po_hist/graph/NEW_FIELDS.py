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
        .withColumn("MATL_MVMT_YR", col("GJAHR"))\
        .withColumn("MATL_MVMT_NUM", col("BELNR"))\
        .withColumn("MATL_MVMT_SEQ_NBR", col("BUZEI"))\
        .withColumn("PO_HIST_CAT_CD", col("BEWTP"))\
        .withColumn("MVMT_TYPE_CD", col("BWART"))\
        .withColumn(
          "PSTNG_DTTM",
          when((col("budat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("budat"), "yyyyMMdd"))
        )\
        .withColumn("RECV_EA_QTY", col("MENGE"))\
        .withColumn("ENT_MATL_NUM", col("MATNR"))\
        .withColumn("PLNT_CD", col("WERKS"))\
        .withColumn(
          "UNIQ_KEY_ID",
          lit(
            "#"
          )
        )\
        .withColumn("MVMT_TYPE", col("BWART"))\
        .withColumn("QTY_IN_PRCH_ORDR_PRC_UNIT", col("BPMNG"))\
        .withColumn("AMT_IN_LCL_CRNCY1", col("DMBTR"))\
        .withColumn("AMT_IN_DOC_CRNCY1", col("WRBTR"))\
        .withColumn("CRNCY_KEY", col("WAERS"))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_IN_LCL_CRNCY", col("AREWR"))\
        .withColumn("GOODS_RCPT_BLOK_STK_IN_OU", col("WESBS"))\
        .withColumn("QTY_IN_GR_BLOK_STK", col("BPWES"))\
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
          when((col("cpudt") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(concat(col("cpudt"), col("cputm")), "yyyyMMddHHmmss"))
        )\
        .withColumn("INVC_VAL_ENT", col("REEWR"))\
        .withColumn("CMPLI_WTH_SHIPPING_INSTR", col("EVERE"))\
        .withColumn("INVC_VAL_IN_FRGN_CRNCY", col("REFWR"))\
        .withColumn("RVSL_OF_GR_ALLW", col("XWSBR"))\
        .withColumn("SEQ_NUM_OF_SUP_CNFRM", col("ETENS"))\
        .withColumn("NUM_OF_DOC_COND", col("KNUMV"))\
        .withColumn("TAX_ON_SLS_PRCH_CD", col("MWSKZ"))\
        .withColumn(
          "TAX_RPTG_CTRY_REGN",
          lit(
            "#"
          )
        )\
        .withColumn("QTY_IN_UOM_FROM_DELV_NOTE", col("LSMNG"))\
        .withColumn("UOM_FROM_DELV_NOTE", col("LSMEH"))\
        .withColumn("MATL_NUM", col("EMATN"))\
        .withColumn("CLRNG_VAL_ON_GR_IR_CLRNG_ACCT", col("AREWW"))\
        .withColumn("LCL_CRNCY_KEY", col("HSWAE"))\
        .withColumn("QTY1", col("BAMNG"))\
        .withColumn("BTCH_NUM", col("CHARG"))\
        .withColumn(
          "DOC_DTTM",
          when((col("bldat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("bldat"), "yyyyMMdd"))
        )\
        .withColumn("CALC_OF_VAL_OPEN", col("XWOFF"))\
        .withColumn("UNPLAN_ACCT_ASGNMT", col("XUNPL"))\
        .withColumn("NM_OF_PRSN_RESP_FOR_CREAT_OBJ", col("ERNAM"))
