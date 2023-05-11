from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_itm.config.ConfigStore import *
from sap_md_mfg_order_itm.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MFG_ORDR_TYP_CD", col("AUART"))\
        .withColumn("MFG_ORDR_NUM", col("AUFNR"))\
        .withColumn("LN_ITM_NBR", col("POSNR"))\
        .withColumn("PRDTN_UOM_CD", trim(col("AMEIN")))\
        .withColumn("BTCH_NUM", trim(col("CHARG")))\
        .withColumn("DLV_CMPLT_IND", trim(col("ELIKZ")))\
        .withColumn("MATL_NUM", trim(col("MATNR")))\
        .withColumn("BASE_UOM_CD", trim(col("MEINS")))\
        .withColumn("MFG_PLNND_ORDR_NUM", trim(col("PLNUM")))\
        .withColumn("SCRP_QTY", col("PSAMG").cast(DecimalType(18, 4)))\
        .withColumn("ITM_QTY", col("PSMNG").cast(DecimalType(18, 4)))\
        .withColumn("PLNT_CD", trim(col("PWERK")))\
        .withColumn("FCTR_DNMNTR_MEAS", col("UMREN").cast(DecimalType(18, 4)))\
        .withColumn("FCTR_NMRTR_MEAS", col("UMREZ").cast(DecimalType(18, 4)))\
        .withColumn("PRDNT_VRSN_NUM", trim(col("VERID")))\
        .withColumn("GOOD_RCPT_LD_DAYS_QTY", col("WEBAZ").cast(DecimalType(18, 4)))\
        .withColumn("RCVD_QTY", col("WEMNG").cast(DecimalType(18, 4)))\
        .withColumn("DEL_IND", trim(col("XLOEK")))\
        .withColumn(
          "L1_LAST_PRODUCTION_DTTM",
          when((col("LTRMI") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("LTRMI"), "yyyyMMdd"))
        )\
        .withColumn("STRG_LOC", trim(col("LGORT")))\
        .withColumn("SPL_PRCMT_TYPE", trim(col("PSOBS")))\
        .withColumn("NUM_OF_QTA_ARNG", trim(col("QUNUM")))\
        .withColumn("QTA_ARNG_ITM", trim(col("QUPOS")))\
        .withColumn("WRK_BRKDWN_STRC_ELMNT", trim(col("PROJN")))\
        .withColumn(
          "PLAN_ORDR_PLAN_STRT_DTTM",
          when((col("STRMP") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("STRMP"), "yyyyMMdd"))
        )\
        .withColumn(
          "PLAN_ORDR_OPN_DTTM",
          when((col("ETRMP") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ETRMP"), "yyyyMMdd"))
        )\
        .withColumn("SLS_ORDR", trim(col("KDAUF")))\
        .withColumn("SLS_ORDR_ITM", trim(col("KDPOS")))\
        .withColumn("DELV_SCHED_FOR_SLS_ORDR", trim(col("KDEIN")))\
        .withColumn("PRCMT_TYPE", trim(col("BESKZ")))\
        .withColumn("EXPTD_SURPLUS", col("IAMNG").cast(DecimalType(18, 4)))\
        .withColumn("PLAN_SCRAP_QTY", col("PAMNG").cast(DecimalType(18, 4)))\
        .withColumn("PLAN_TOT_ORDR_QTY", col("PGMNG").cast(DecimalType(18, 4)))\
        .withColumn("ACCT_ASGNMT_CAT", trim(col("KNTTP")))\
        .withColumn("PRTL_CONV_IN", trim(col("TPAUF")))\
        .withColumn(
          "PLAN_ORDR_DELV_DTTM",
          when((col("LTRMP") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("LTRMP"), "yyyyMMdd"))
        )\
        .withColumn("COST_EST_NUM", trim(col("KALNR")))\
        .withColumn("OVERDELV_TLRNC", col("UEBTO").cast(DecimalType(18, 4)))\
        .withColumn("UNLMTED_OVERDELV_ALLW", trim(col("UEBTK")))\
        .withColumn("UNDRDELV_TLRNC", col("UNTTO").cast(DecimalType(18, 4)))\
        .withColumn("STK_TYPE", trim(col("INSMK")))\
        .withColumn("GOODS_RCPT_IN", trim(col("WEPOS")))\
        .withColumn("VALUT_TYPE", trim(col("BWTAR")))\
        .withColumn("VALUT_CAT", trim(col("BWTTY")))\
        .withColumn("RUN_SCHED_HDR_NUM", trim(col("SAFNR")))\
        .withColumn("BOM_EXPLS_NUM", trim(col("SERNR")))\
        .withColumn("PARM_VARIANT", trim(col("TECHS")))\
        .withColumn("PLNT", trim(col("DWERK")))\
        .withColumn("ORDR_CAT", trim(col("DAUTY")))\
        .withColumn("ORDR_TYPE", trim(col("DAUAT")))\
        .withColumn(
          "BSC_FIN_DTTM",
          when((col("DGLTP") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("DGLTP"), "yyyyMMdd"))
        )\
        .withColumn(
          "SCHD_FIN_DTTM",
          when((col("DGLTS") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("DGLTS"), "yyyyMMdd"))
        )\
        .withColumn("ORDR_RELS_IN", trim(col("DFREI")))\
        .withColumn("ORDR_ITM_NOT_RLVNT_MRP_IN", trim(col("DNREL")))\
        .withColumn("MRP_DSTN_KEY", trim(col("VERTO")))\
        .withColumn("SPL_STK_IN", trim(col("SOBKZ")))\
        .withColumn("CNSMPTN_PSTNG", trim(col("KZVBR")))\
        .withColumn("VAL_GOODS_RECV_IN_LCL_CRNCY", col("WEWRT").cast(DecimalType(18, 4)))\
        .withColumn("GOODS_RCPT_NON_VALUTED", trim(col("WEUNB")))\
        .withColumn("UNLOADNG_PT", trim(col("ABLAD")))\
        .withColumn("GOODS_RCPNT", trim(col("WEMPF")))\
        .withColumn("BUSN_AREA", trim(col("GSBER")))\
        .withColumn("GOODS_RCPT_IN_CN_BE_CHG_IN", trim(col("WEAED")))\
        .withColumn("CNFG", trim(col("CUOBJ")))\
        .withColumn("KANBAN_IN", trim(col("KBNKZ")))\
        .withColumn("SETLM_RESV_NUM", trim(col("ARSNR")))\
        .withColumn("ITM_NUM_OF_THE_SETLM_RESV", trim(col("ARSPS")))\
        .withColumn("NUM_OF_RESV", trim(col("KRSNR")))\
        .withColumn("ITM_NUM_OF_RESV", trim(col("KRSPS")))\
        .withColumn("COST_CLCT_KEY", trim(col("KCKEY")))\
        .withColumn("COST_CLCT_FOR_REPTV_MFG", trim(col("RTP01")))\
        .withColumn("COST_CLCT_FOR_KANBAN", trim(col("RTP02")))\
        .withColumn("COST_CLCT_VAL_SLS_ORDR_STK", trim(col("RTP03")))\
        .withColumn("COST_CLCT_FOR_EXTRNL_PPC", trim(col("RTP04")))\
        .withColumn(
          "COST_CLCT_VLD_FROM_DTTM",
          when((col("KSVON") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("KSVON"), "yyyyMMdd"))
        )\
        .withColumn(
          "COST_CLCT_VLD_TO_DTTM",
          when((col("KSBIS") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("KSBIS"), "yyyyMMdd"))
        )\
        .withColumn("OBJ_NUM", trim(col("OBJNP")))\
        .withColumn("MATL_ORDR_ITM_IS_NOT_RLVNT", trim(col("NDISR")))\
        .withColumn("CMT_QTY_FOR_ORDR_ACC", col("VFMNG").cast(DecimalType(18, 4)))\
        .withColumn(
          "TOT_CMMT_DTTM",
          when((col("GSBTR") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("GSBTR"), "yyyyMMdd"))
        )\
        .withColumn("TYPE_OF_AVLBLTY_CHK_IN", trim(col("KZAVC")))\
        .withColumn("VALUT_OF_SPL_STK", trim(col("KZBWS")))\
        .withColumn("SER_NUM_PRFL", trim(col("SERNP")))\
        .withColumn("NUM_OF_SER_NUMS", trim(col("ANZSN")).cast(IntegerType()))\
        .withColumn("CHG_IN", trim(col("OBJTYPE")))\
        .withColumn("PRCS_LEAD_TO_CHG_OF_AN_OBJ", trim(col("CH_PROC")))\
        .withColumn("FX_PRC_CO_PROD", trim(col("FXPRU")))\
        .withColumn("CNFG_INTRNAL_OBJ_NUM", trim(col("CUOBJ_ROOT")))\
        .withColumn("MRP_AREA", trim(col("BERID")))\
        .withColumn("PARM_VRNT", trim(col("TECHS_COPY")))\
        .withColumn("STK_SGMNT", col("STK_SGMNT"))\
        .withColumn("CUST_NUM1", col("CUST_NUM1"))\
        .withColumn("SEASN_YR", col("SEASN_YR"))\
        .withColumn("SEASN", col("SEASN"))\
        .withColumn("FSHN_CLCT", col("FSHN_CLCT"))\
        .withColumn("FSHN_THEME", col("FSHN_THEME"))\
        .withColumn("ALC_STK_QTY", col("ALC_STK_QTY"))\
        .withColumn("NUM_OF_ORIG_ORDR", col("NUM_OF_ORIG_ORDR"))\
        .withColumn("CNFRM_QTY_FOR_ITM", col("CNFRM_QTY_FOR_ITM"))\
        .withColumn("ITM_SEQ", col("ITM_SEQ"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM, 'LN_ITM_NBR', LN_ITM_NBR)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM, 'LN_ITM_NBR', LN_ITM_NBR)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
