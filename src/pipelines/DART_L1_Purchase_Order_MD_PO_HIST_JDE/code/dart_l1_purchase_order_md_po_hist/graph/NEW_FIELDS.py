from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from dart_l1_purchase_order_md_po_hist.config.ConfigStore import *
from dart_l1_purchase_order_md_po_hist.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("PO_NUM", col("oldoco"))\
        .withColumn("PO_LINE_NBR", col("ollnid"))\
        .withColumn(
          "PO_SEQ_NBR",
          lit(
            "#"
          )
        )\
        .withColumn(
          "EV_TYPE_CO",
          lit(
            "#"
          )
        )\
        .withColumn(
          "MATL_MVMT_YR",
          when(
              (
                ((lower(trim(col("olupmj"))) == lit("null")) | (trim(col("olupmj")) == lit("")))
                | (trim(col("olupmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(struct(
            concat(
                substring(
                  date_add(
                      concat(
                        substring((trim(col("olupmj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                        lit("-01-01")
                      ), 
                      (
                        substring((trim(col("olupmj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
                          .cast(IntegerType())
                        - 1
                      )
                    )\
                    .cast(StringType()), 
                  1, 
                  10
                ), 
                lit(""), 
                lpad(trim(col("oltday")), 6, "0")
              )\
              .alias("col1"), 
            lit("yyyy-MM-dd HHmmss").alias("col2")
          ))
        )\
        .withColumn(
          "MATL_MVMT_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "MATL_MVMT_SEQ_NBR",
          lit(
            "#"
          )
        )\
        .withColumn("PO_HIST_CAT_CD", trim(col("ollt")))\
        .withColumn("MVMT_TYPE_CD", trim(col("olnxtr")))\
        .withColumn(
          "PSTNG_DTTM",
          when(
              (
                ((lower(trim(col("olupmj"))) == lit("null")) | (trim(col("olupmj")) == lit("")))
                | (trim(col("olupmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(struct(
            concat(
                substring(
                  date_add(
                      concat(
                        substring((trim(col("olupmj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                        lit("-01-01")
                      ), 
                      (
                        substring((trim(col("olupmj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
                          .cast(IntegerType())
                        - 1
                      )
                    )\
                    .cast(StringType()), 
                  1, 
                  10
                ), 
                lit(" "), 
                lpad(trim(col("oltday")), 6, "0")
              )\
              .alias("col1"), 
            lit("yyyy-MM-dd HHmmss").alias("col2")
          ))
        )\
        .withColumn("RECV_EA_QTY", trim(col("olurec")))\
        .withColumn("ENT_MATL_NUM", trim(col("ollitm")))\
        .withColumn("PLNT_CD", trim(col("olmcu")))\
        .withColumn(
          "UNIQ_KEY_ID",
          concat_ws(
            ";", 
            col("olcord"), 
            col("oldcto"), 
            col("olkcoo"), 
            col("ollt"), 
            col("olsfxo"), 
            col("olupmj"), 
            col("oltday")
          )
        )\
        .withColumn("MVMT_TYPE", lit(None))\
        .withColumn("QTY_IN_PRCH_ORDR_PRC_UNIT", lit(None))\
        .withColumn("AMT_IN_LCL_CRNCY1", lit(None))\
        .withColumn("AMT_IN_DOC_CRNCY1", lit(None))\
        .withColumn("CRNCY_KEY", trim(col("OLCRCD")))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_IN_LCL_CRNCY", lit(None))\
        .withColumn("GOODS_RCPT_BLOK_STK_IN_OU", lit(None))\
        .withColumn("QTY_IN_GR_BLOK_STK", lit(None))\
        .withColumn("DEBIT_OR_CREDIT_IN", lit(None))\
        .withColumn("VALUT_TYPE", lit(None))\
        .withColumn("DELV_CMPLT_IN", lit(None))\
        .withColumn("REF_DOC_NUM", trim(col("OLOORN")))\
        .withColumn("FISC_YR_OF_A_REF_DOC", lit(None))\
        .withColumn("DOC_NUM_OF_A_REF_DOC", lit(None))\
        .withColumn("ITM_OF_A_REF_DOC", lit(None))\
        .withColumn("RSN_FOR_MVMT", lit(None))\
        .withColumn("ACTG_DOC_ENT_DTTM", lit(None))\
        .withColumn("INVC_VAL_ENT", lit(None))\
        .withColumn("CMPLI_WTH_SHIPPING_INSTR", lit(None))\
        .withColumn("INVC_VAL_IN_FRGN_CRNCY", lit(None))\
        .withColumn("RVSL_OF_GR_ALLW", lit(None))\
        .withColumn("SEQ_NUM_OF_SUP_CNFRM", lit(None))\
        .withColumn("NUM_OF_DOC_COND", lit(None))\
        .withColumn("TAX_ON_SLS_PRCH_CD", lit(None))\
        .withColumn("TAX_RPTG_CTRY_REGN", lit(None))\
        .withColumn("QTY_IN_UOM_FROM_DELV_NOTE", lit(None))\
        .withColumn("UOM_FROM_DELV_NOTE", trim(col("OLUOM")))\
        .withColumn("MATL_NUM", lit(None))\
        .withColumn("CLRNG_VAL_ON_GR_IR_CLRNG_ACCT", lit(None))\
        .withColumn("LCL_CRNCY_KEY", lit(None))\
        .withColumn("QTY1", lit(None))\
        .withColumn("BTCH_NUM", lit(None))\
        .withColumn("DOC_DTTM", lit(None))\
        .withColumn("CALC_OF_VAL_OPEN", lit(None))\
        .withColumn("UNPLAN_ACCT_ASGNMT", lit(None))\
        .withColumn("NM_OF_PRSN_RESP_FOR_CREAT_OBJ", lit(None))\
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
        .withColumn("DAI_UPT_DTTM", current_timestamp())\
        .withColumn("SRVC_NUM", lit(None))\
        .withColumn("PKG_NUM_OF_SRVC", lit(None))\
        .withColumn("LINE_NUM_OF_SRVC", lit(None))\
        .withColumn("NUM_OF_PO_ACCT_ASGNMT", lit(None))\
        .withColumn("RTRN_IN", lit(None))\
        .withColumn("CLRNG_VAL_ON_GR_IR_ACCT", lit(None))\
        .withColumn("INVC_AMT_IN_PO_CRNCY", lit(None))\
        .withColumn("SAP_RLSE", lit(None))\
        .withColumn("QTY2", lit(None))\
        .withColumn("QTY_IN_PO_PRC_UNIT", lit(None))\
        .withColumn("AMT_IN_LCL_CRNCY", lit(None))\
        .withColumn("AMT_IN_DOC_CRNCY", lit(None))\
        .withColumn("VALUT_GOODS_RCPT_BLOK_STK", lit(None))\
        .withColumn("QTY_IN_VALUT_GR_BLOK_STK", lit(None))\
        .withColumn("ACC_AT_ORIG", lit(None))\
        .withColumn("GR_IR_ACCT_CLRNG_VAL_LCL_CRNCY", lit(None))\
        .withColumn("EXCH_RT_DIFF_AMT", lit(None))\
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
        .withColumn("PCDR_FOR_UPDT_SCHED_LINE_QTY", lit(None))\
        .withColumn("QTY_IN_PAREL_UNIT_OF_MEAS", lit(None))\
        .withColumn("GOODS_RCPT_BLOK_STK", lit(None))\
        .withColumn("TYPE_OF_PAREL_UNIT_OF_MEAS", lit(None))\
        .withColumn("VAL_GOODS_RCPT_BLOK_STK", lit(None))\
        .withColumn("DPRE_CMPLT_FL", lit(None))\
        .withColumn("SEASN_YR", lit(None))\
        .withColumn("SEASN", lit(None))\
        .withColumn("FSHN_CLCT", lit(None))\
        .withColumn("FSHN_Theme", lit(None))\
        .withColumn("QTY3", lit(None))\
        .withColumn("CHAR_VAL_1", lit(None))\
        .withColumn("CHAR_VAL_2", lit(None))\
        .withColumn("CHAR_VAL_3", lit(None))
