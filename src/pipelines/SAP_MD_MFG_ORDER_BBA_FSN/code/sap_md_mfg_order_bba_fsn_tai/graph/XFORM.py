from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_bba_fsn_tai.config.ConfigStore import *
from sap_md_mfg_order_bba_fsn_tai.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MFG_ORDR_TYP_CD", col("AUART"))\
        .withColumn("MFG_ORDR_NUM", col("AUFNR"))\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn(
          "CRTD_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("DEL_IND", trim(col("LOEKZ")))\
        .withColumn("OBJECT_NUMBER", trim(col("OBJNR")))\
        .withColumn("PLNT_CD", trim(col("WERKS")))\
        .withColumn("ORDR_CAT", trim(col("AUTYP")))\
        .withColumn("REF_ORDR_NUM", trim(col("REFNR")))\
        .withColumn("ENT_BY", trim(col("ERNAM")))\
        .withColumn("LAST_CHG_BY", trim(col("AENAM")))\
        .withColumn("DESC", trim(col("KTEXT")))\
        .withColumn("LONG_TEXT_EXISTS", trim(col("LTEXT")))\
        .withColumn("CO_CD", trim(col("BUKRS")))\
        .withColumn("BUSN_AREA", trim(col("GSBER")))\
        .withColumn("CNTL_AREA", trim(col("KOKRS")))\
        .withColumn("COST_CLCT_KEY", trim(col("CCKEY")))\
        .withColumn("ORDR_CRNCY", trim(col("WAERS")))\
        .withColumn("ORDR_STS", trim(col("ASTNR")))\
        .withColumn(
          "LAST_STS_CHG_DTTM",
          when((col("STDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("STDAT"), "yyyyMMdd"))
        )\
        .withColumn("STS_REACHED_SO_FAR", trim(col("ESTNR")))\
        .withColumn("PH_ORDR_RELS", trim(col("PHAS1")))\
        .withColumn("PH_ORDR_CMPLT", trim(col("PHAS2")))\
        .withColumn(
          "PLAN_CMPLT_DTTM",
          when((col("PDAT2") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("PDAT2"), "yyyyMMdd"))
        )\
        .withColumn(
          "PLAN_CLS_DTTM",
          when((col("PDAT3") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("PDAT3"), "yyyyMMdd"))
        )\
        .withColumn(
          "RLSE_DTTM",
          when((col("IDAT1") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("IDAT1"), "yyyyMMdd"))
        )\
        .withColumn(
          "TECH_CMPLT_DTTM",
          when((col("IDAT2") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("IDAT2"), "yyyyMMdd"))
        )\
        .withColumn(
          "CLSE_DTTM",
          when((col("IDAT3") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("IDAT3"), "yyyyMMdd"))
        )\
        .withColumn("OBJ_ID", trim(col("OBJID")))\
        .withColumn("USG_OF_THE_COND_TBL", trim(col("KVEWE")))\
        .withColumn("APPL", trim(col("KAPPL")))\
        .withColumn("COST_SHT", trim(col("KALSM")))\
        .withColumn("OVHD_KEY", trim(col("ZSCHL")))\
        .withColumn("PRCSG_GRP", trim(col("ABKRS")))\
        .withColumn("SEQ_NUM", trim(col("SEQNR")))\
        .withColumn("APPLT", trim(col("USER0")))\
        .withColumn("EST_TOT_COSTS_OF_ORDR", col("USER4").cast(DecimalType(18, 4)))\
        .withColumn(
          "APPL_DTTM",
          when((col("USER5") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("USER5"), "yyyyMMdd"))
        )\
        .withColumn(
          "WRK_STRT_DTTM",
          when((col("USER7") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("USER7"), "yyyyMMdd"))
        )\
        .withColumn(
          "END_OF_WRK_DTTM",
          when((col("USER8") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("USER8"), "yyyyMMdd"))
        )\
        .withColumn("PRFT_CTR", trim(col("PRCTR")))\
        .withColumn("WRK_BRKDWN_STRC_ELMNT", trim(col("PSPEL")))\
        .withColumn("VAR_KEY", trim(col("AWSLS")))\
        .withColumn("RSLTS_ANAL_KEY", trim(col("ABGSL")))\
        .withColumn("REQ_CO_CD", trim(col("ABUKR")))\
        .withColumn("ITM_NUM_IN_SLS_ORDR", trim(col("KDPOS")))\
        .withColumn("PRDTN_PRCS", trim(col("PROCNR")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_deleted_", col("_deleted_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM)"
              )
            )
          )
        )\
        .withColumn("MFG_ORDR_STTS_CD", trim(col("STAT_LIST")))\
        .withColumn("ORDR_RTNG_NUM", trim(col("AFKO_AUFPL")))\
        .withColumn("MRP_CNTRLLR_CD", trim(col("AFKO_DISPO")))\
        .withColumn("PRD_SPVSR_CD", trim(col("AFKO_FEVOR")))\
        .withColumn(
          "ACT_RLSE_DTTM",
          when((col("AFKO_FTRMI") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_FTRMI"), "yyyyMMdd"))
        )\
        .withColumn(
          "PLAN_RLSE_DTTM",
          when((col("AFKO_FTRMP") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_FTRMP"), "yyyyMMdd"))
        )\
        .withColumn(
          "SCH_REL_DTTM",
          when(
              (
                ((col("AFKO_GETRI") == lit("00000000")) | (length(regexp_replace(col("AFKO_GETRI"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GETRI")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GETRI"), col("AFKO_GEUZI")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "PRDTN_END_DTTM",
          when((col("AFKO_GLTRI") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_GLTRI"), "yyyyMMdd"))
        )\
        .withColumn(
          "END_DTTM",
          when(
              (
                ((col("AFKO_GLTRP") == lit("00000000")) | (length(regexp_replace(col("AFKO_GLTRP"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GLTRP")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GLTRP"), col("AFKO_GLUZP")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "SCHD_END_DTTM",
          when(
              (
                ((col("AFKO_GLTRS") == lit("00000000")) | (length(regexp_replace(col("AFKO_GLTRS"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GLTRS")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GLTRS"), col("AFKO_GLUZS")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "ACT_STRT_DTTM",
          when(
              (
                ((col("AFKO_GSTRI") == lit("00000000")) | (length(regexp_replace(col("AFKO_GSTRI"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GSTRI")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GSTRI"), col("AFKO_GSUZI")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "STRT_DTTM",
          when(
              (
                ((col("AFKO_GSTRP") == lit("00000000")) | (length(regexp_replace(col("AFKO_GSTRP"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GSTRP")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GSTRP"), col("AFKO_GSUZP")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "SCHD_STRT_DTTM",
          when(
              (
                ((col("AFKO_GSTRS") == lit("00000000")) | (length(regexp_replace(col("AFKO_GSTRS"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GSTRS")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GSTRS"), col("AFKO_GSUZS")), "yyyyMMddHHmmss"))
        )\
        .withColumn("CNFRMD_SCRP_QTY", col("AFKO_IASMG").cast(DecimalType(18, 4)))\
        .withColumn("CNFRMD_YLD_QTY", col("AFKO_IGMNG").cast(DecimalType(18, 4)))\
        .withColumn("RTNG_GRP_CNTR_NUM", trim(col("AFKO_PLNAL")))\
        .withColumn("RTNG_GRP_CD", trim(col("AFKO_PLNNR")))\
        .withColumn("RTNG_TYP_CD", trim(col("AFKO_PLNTY")))\
        .withColumn("RSRVTN_NUM", trim(col("AFKO_RSNUM")))\
        .withColumn("BOM_ALT_NUM", trim(col("AFKO_STLAL")))\
        .withColumn("BOM_NUM", trim(col("AFKO_STLNR")))\
        .withColumn("BOM_CAT_CD", trim(col("AFKO_STLTY")))\
        .withColumn("MATL_NUM", trim(col("AFKO_PLNBEZ")))\
        .withColumn("TOT_ORDR_QTY", col("AFKO_GAMNG").cast(DecimalType(18, 4)))\
        .withColumn("TOT_SCRAP_QTY_IN_ORDR", col("AFKO_GASMG").cast(DecimalType(18, 4)))\
        .withColumn("BASE_UOM", trim(col("AFKO_GMEIN")))\
        .withColumn("APPL_OF_THE_TASK_LIST", trim(col("AFKO_PLNAW")))\
        .withColumn("TASK_LIST_USG", trim(col("AFKO_PVERW")))\
        .withColumn("MAX_LOT_SIZE", col("AFKO_PLSVB").cast(DecimalType(18, 4)))\
        .withColumn("TASK_LIST_UOM", trim(col("AFKO_PLNME")))\
        .withColumn("MIN_LOT_SIZE", col("AFKO_PLSVN").cast(DecimalType(18, 4)))\
        .withColumn("CHG_NUM1", trim(col("AFKO_PAENR")))\
        .withColumn("RESP_PLNR_GRP_OR_DEPT", trim(col("AFKO_PLGRP")))\
        .withColumn("LOT_SIZE_DIVSR", col("AFKO_LODIV").cast(DecimalType(18, 4)))\
        .withColumn("MATL_NUM1", trim(col("AFKO_STLBEZ")))\
        .withColumn("BILL_OF_MATL_STS", trim(col("AFKO_STLST")))\
        .withColumn("BASE_QTY", col("AFKO_SBMNG").cast(DecimalType(18, 4)))\
        .withColumn("BASE_UNIT_OF_MEAS", trim(col("AFKO_SBMEH")))\
        .withColumn("CHG_NUM2", trim(col("AFKO_SAENR")))\
        .withColumn("BOM_USG", trim(col("AFKO_STLAN")))\
        .withColumn("FROM_LOT_SIZE", col("AFKO_SLSVN").cast(DecimalType(18, 4)))\
        .withColumn("TO_LOT_SIZE", col("AFKO_SLSBS").cast(DecimalType(18, 4)))\
        .withColumn("SCHDLNG_MRGN_KEY_FOR_FLOATS", trim(col("AFKO_FHORI")))\
        .withColumn("SCHDLNG_TYPE", trim(col("AFKO_TERKZ")))\
        .withColumn("FLOAT_BEF_PRDTN", trim(col("AFKO_VORGZ")))\
        .withColumn("FLOAT_AFTER_PRDTN", trim(col("AFKO_SICHZ")))\
        .withColumn("RLSE_PER", trim(col("AFKO_FREIZ")))\
        .withColumn("CHG_TO_SCHD_DT_IN", trim(col("AFKO_UPTER")))\
        .withColumn("ID_OF_THE_CAPY_RQR_REC", trim(col("AFKO_BEDID")))\
        .withColumn("PROJ_DEF", trim(col("AFKO_PRONR")))\
        .withColumn("INTRNL_CNTR1", trim(col("AFKO_ZAEHL")))\
        .withColumn("INTRNL_CNTR2", trim(col("AFKO_MZAEHL")))\
        .withColumn("CNTR_FOR_ADDL_CRITA", trim(col("AFKO_ZKRIZ")))\
        .withColumn("INSP_LOT_NUM", trim(col("AFKO_PRUEFLOS")))\
        .withColumn("COST_VRNT_FOR_PLAN_COSTS", trim(col("AFKO_KLVARP")))\
        .withColumn("COST_VRNT_FOR_ACTL_COSTS", trim(col("AFKO_KLVARI")))\
        .withColumn("CMPLT_CNFRM_NUM_FOR_THE_OPR", trim(col("AFKO_RUECK")))\
        .withColumn("INTRNL_CNTR3", trim(col("AFKO_RMZHL")))\
        .withColumn("CNFG", trim(col("AFKO_CUOBJ")))\
        .withColumn("OBJ_ID_OF_THE_RSRS1", trim(col("AFKO_RSHID")))\
        .withColumn("OBJ_ID_OF_THE_RSRS2", trim(col("AFKO_RSNID")))\
        .withColumn("LVL", col("AFKO_STUFE").cast(DecimalType(18, 4)))\
        .withColumn("PATH1", col("AFKO_WEGXX").cast(DecimalType(18, 4)))\
        .withColumn("PATH2", col("AFKO_VWEGX").cast(DecimalType(18, 4)))\
        .withColumn("NUM_OF_RESV", trim(col("AFKO_ARSNR")))\
        .withColumn("ORDR_ITM_NUM", trim(col("AFKO_ARSPS")))\
        .withColumn("LEFT_NODE_IN_CLCTV_ORDR", trim(col("AFKO_LKNOT")))\
        .withColumn("RIGHT_NODE_OF_CLCTV_ORDR", trim(col("AFKO_RKNOT")))\
        .withColumn("CNFRM_DEG_OF_PRCSG", trim(col("AFKO_ABARB")))\
        .withColumn("RTG_NUM_OF_OPS_IN_THE_ORDR", trim(col("AFKO_AUFPT")))\
        .withColumn("GENL_CNTR_FOR_ORDR", trim(col("AFKO_APLZT")))\
        .withColumn(
          "RTG_EXPLS_DTTM",
          when((col("AFKO_PLAUF") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_PLAUF"), "yyyyMMdd"))
        )\
        .withColumn(
          "VLD_FROM_DTTM",
          when((col("AFKO_PDATV") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_PDATV"), "yyyyMMdd"))
        )\
        .withColumn(
          "VLD_FROM_DTTM1",
          when((col("AFKO_SDATV") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_SDATV"), "yyyyMMdd"))
        )\
        .withColumn(
          "BOM_EXPLS_TFR_DTTM",
          when((col("AFKO_AUFLD") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AFKO_AUFLD"), "yyyyMMdd"))
        )\
        .withColumn(
          "FIN_DTTM",
          when(
              (
                ((col("AFKO_GLTPP") == lit("00000000")) | (length(regexp_replace(col("AFKO_GLTPP"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GLTPP")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GLTPP"), col("AFKO_GLUPP")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "FCST_STRT_DTTM",
          when(
              (
                ((col("AFKO_GSTPP") == lit("00000000")) | (length(regexp_replace(col("AFKO_GSTPP"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GSTPP")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GSTPP"), col("AFKO_GSUPP")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "SCHD_FCST_FIN_DTTM",
          when(
              (
                ((col("AFKO_GLTPS") == lit("00000000")) | (length(regexp_replace(col("AFKO_GLTPS"), "(\\d+)", "")) > lit(0)))
                | (length(col("AFKO_GLTPS")) < lit(8))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("AFKO_GLTPS"), col("AFKO_GLUPS")), "yyyyMMddHHmmss"))
        )\
        .withColumn("SCHD_FCST_STRT_DTTM", when(
          (
            ((col("AFKO_GSTPS") == lit("00000000")) | (length(regexp_replace(col("AFKO_GSTPS"), "(\\d+)", "")) > lit(0)))
            | (length(col("AFKO_GSTPS")) < lit(8))
          ), 
          lit(None)
        )\
        .otherwise(to_timestamp(concat(col("AFKO_GSTPS"), col("AFKO_GSUPS")), "yyyyMMddHHmmss")))
