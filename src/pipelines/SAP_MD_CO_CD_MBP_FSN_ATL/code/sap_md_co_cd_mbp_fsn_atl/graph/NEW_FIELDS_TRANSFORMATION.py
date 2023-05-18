from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_mbp_fsn_atl.config.ConfigStore import *
from sap_md_co_cd_mbp_fsn_atl.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CO_CD", col("BUKRS"))\
        .withColumn("CUST_NUM", col("KUNNR"))\
        .withColumn("BUY_GRP_CD", trim(col("EKVBD")))\
        .withColumn("PLNG_CUST_IND", trim(col("FDGRV")))\
        .withColumn("TERM_PMT_KEY", trim(col("ZTERM")))\
        .withColumn("PERS_NUM", trim(col("PERNR")))\
        .withColumn(
          "REC_CRT_DTTM",
          when(
              (
                ((col("ERDAT") == lit("00000000")) | (length(regexp_replace(col("ERDAT"), "(\\d+)", "")) > lit(0)))
                | (length(col("ERDAT")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("NM_PRSNCRT_OBJ", trim(col("ERNAM")))\
        .withColumn("PSTNG_BLK_CO_CD", trim(col("SPERR")))\
        .withColumn("DEL_FL_MSTR_REC", trim(col("LOEVM")))\
        .withColumn("KEY_SRT_ACCORD_ASGNMT_NUM", trim(col("ZUAWA")))\
        .withColumn("ACTG_CLERK_ABBR", trim(col("BUSAB")))\
        .withColumn("RCNL_ACCT_GENL_LDGR", trim(col("AKONT")))\
        .withColumn("AUTH_GRP", trim(col("BEGRU")))\
        .withColumn("HD_OFF_ACCT_NUM", trim(col("KNRZE")))\
        .withColumn("ACCT_NUM_ALT_PYR", trim(col("KNRZB")))\
        .withColumn("IN_PMT_NOTC_CUST_WTH_CLR_ITM", trim(col("ZAMIM")))\
        .withColumn("IN_PMT_NOTC_SLS_DEPT", trim(col("ZAMIV")))\
        .withColumn("IN_PMT_NOTC_LEGAL_DEPT", trim(col("ZAMIR")))\
        .withColumn("IN_PMT_NOTC_ACTG_DEPT", trim(col("ZAMIB")))\
        .withColumn("IN_PMT_NOTC_CUST_WOUT_CLR_ITM", trim(col("ZAMIO")))\
        .withColumn("LIST_RSPCT_PMT_METHDS", trim(col("ZWELS")))\
        .withColumn("IN_CLRNG_BTWN_CUST_VEND", trim(col("XVERR")))\
        .withColumn("BLK_KEY_PMT", trim(col("ZAHLS")))\
        .withColumn("TERM_PMT_KEY_BILL_EXCH_CHRG", trim(col("WAKON")))\
        .withColumn("INT_IN", trim(col("VZSKZ")))\
        .withColumn(
          "KEY_LAST_INT_CALC_DTTM",
          when(
              (
                ((col("ZINDT") == lit("00000000")) | (length(regexp_replace(col("ZINDT"), "(\\d+)", "")) > lit(0)))
                | (length(col("ZINDT")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(col("ZINDT"), "yyyyMMdd"))
        )\
        .withColumn("INT_CALC_FREQ_MO", trim(col("ZINRT")))\
        .withColumn("ACCT_NUM_CUST", trim(col("EIKTO")))\
        .withColumn("USER_AT_CUST", trim(col("ZSABE")))\
        .withColumn("MEMO", trim(col("KVERM")))\
        .withColumn("EXPT_CR_INSR_INST_NUM", trim(col("VRBKZ")))\
        .withColumn("AMT_INS", col("VLIBB").cast(DecimalType(18, 4)))\
        .withColumn("INSR_LEAD_MO", trim(col("VRSZL")))\
        .withColumn("DEDCT_PCT_RT", trim(col("VRSPR")))\
        .withColumn("INSR_NUM", trim(col("VRSNR")))\
        .withColumn(
          "INSR_VLD_DTTM",
          when(
              (
                ((col("VERDT") == lit("00000000")) | (length(regexp_replace(col("VERDT"), "(\\d+)", "")) > lit(0)))
                | (length(col("VERDT")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(col("VERDT"), "yyyyMMdd"))
        )\
        .withColumn("COLL_INVC_VRNT", trim(col("PERKZ")))\
        .withColumn("IN_LCL_PRCSG", trim(col("XDEZV")))\
        .withColumn("IN_PRD_ACCT_STMT", trim(col("XAUSZ")))\
        .withColumn("BILL_EXCH_LMT", col("WEBTR").cast(DecimalType(18, 4)))\
        .withColumn("NEXT_PAYEE", trim(col("REMIT")))\
        .withColumn(
          "LAST_INT_CALC_RUN_DTTM",
          when(
              (
                ((col("DATLZ") == lit("00000000")) | (length(regexp_replace(col("DATLZ"), "(\\d+)", "")) > lit(0)))
                | (length(col("DATLZ")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(col("DATLZ"), "yyyyMMdd"))
        )\
        .withColumn("IN_REC_PMT_HIST", trim(col("XZVER")))\
        .withColumn("TLRNC_GRP_BUSN_PTNR_ACCT", trim(col("TOGRU")))\
        .withColumn("PRBL_TIME_UNTL_CHK_PD", trim(col("KULTG")))\
        .withColumn("SHRT_KEY_HS_BANK", trim(col("HBKID")))\
        .withColumn("IN_PAY_ALL_ITM_SEP", trim(col("XPORE")))\
        .withColumn("OBSOL_SUB_IN_DTRMN_RDCTN_RT", trim(col("BLNKZ")))\
        .withColumn("PREV_MSTR_REC_NUM", trim(col("ALTKN")))\
        .withColumn("KEY_PMT_GRP", trim(col("ZGRUP")))\
        .withColumn("SHRT_KEY_KNOWN_NEGT_LV", trim(col("URLID")))\
        .withColumn("KEY_DUN_NOTC_GRP", trim(col("MGRUP")))\
        .withColumn("KEY_LKBX_CUST_PAY", trim(col("LOCKB")))\
        .withColumn("PMT_METH_SPLMN", trim(col("UZAWE")))\
        .withColumn("SLCT_RULE_PMT_ADVC", trim(col("SREGL")))\
        .withColumn("IN_SEND_PMT_ADVC_EDI", trim(col("XEDIP")))\
        .withColumn("RLSE_APPR_GRP", trim(col("FRGRP")))\
        .withColumn("RSN_CD_CONV_VERS", trim(col("VRSDG")))\
        .withColumn("ACTG_CLERK_FAX_NUM_CUST_VEND", trim(col("TLFXS")))\
        .withColumn("INET_ADDR_PTNR_CO_CLERK", trim(col("INTAD")))\
        .withColumn("IN_ALT_PYR_USE_ACCT_NUM", trim(col("XKNZB")))\
        .withColumn("PMT_TERM_KEY_FOR_CR_MEMO", trim(col("GUZTE")))\
        .withColumn("ACTV_CD_GRS_INCM_TAX", trim(col("GRICD")))\
        .withColumn("DSTN_TYPE_EMP_TAX", trim(col("GRIDT")))\
        .withColumn("VAL_ADJ_KEY", trim(col("WBRSL")))\
        .withColumn("STS_CHG_AUTH", trim(col("CONFS")))\
        .withColumn(
          "CHG_CNFRM_DTTM",
          when(
              (
                ((col("UPDAT") == lit("00000000")) | (length(regexp_replace(col("UPDAT"), "(\\d+)", "")) > lit(0)))
                | (length(col("UPDAT")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("UPDAT"), col("UPTIM")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "LAST_CHG_CNFRM_DTTM",
          when(
              (
                ((col("UPDAT") == lit("00000000")) | (length(regexp_replace(col("UPDAT"), "(\\d+)", "")) > lit(0)))
                | (length(col("UPDAT")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("UPDAT"), col("UPTIM")), "yyyyMMddHHmmss"))
        )\
        .withColumn("DEL_BOCK_MSTR_REC", trim(col("NODEL")))\
        .withColumn("ACTG_CLERK_PHN_NUM_BUSN_PTNR", trim(col("TLFNS")))\
        .withColumn("ACCT_RCVBL_PLDG_IN", trim(col("CESSION_KZ")))\
        .withColumn("CUST_EXEC", trim(col("GMVKZD")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_pk_", to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CO_CD', CO_CD, 'CUST_NUM', CUST_NUM)")))\
        .withColumn("_pk_md5_", md5(to_json(expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'CO_CD', CO_CD, 'CUST_NUM', CUST_NUM)"))))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))
