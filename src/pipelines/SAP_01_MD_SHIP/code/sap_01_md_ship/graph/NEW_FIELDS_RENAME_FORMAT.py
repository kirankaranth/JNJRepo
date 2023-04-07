from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_ship.config.ConfigStore import *
from sap_01_md_ship.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SHIP_NUM", col("TKNUM"))\
        .withColumn(
          "DELV_TYP_CD",
          lit(
            "#"
          )
        )\
        .withColumn(
          "DOC_REF_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "CO_CD",
          lit(
            "#"
          )
        )\
        .withColumn(
          "DELV_LINE_NBR",
          lit(
            "#"
          )
        )\
        .withColumn(
          "ACTL_SHIP_DTTM",
          when((col("DTABF") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DTABF"), col("UZABF")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "PLAN_SHIP_DTTM",
          when((col("DPABF") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DPABF"), col("UPABF")), "yyyyMMddHHmmss"))
        )\
        .withColumn("SLS_ORDR_CAR_CD", trim(col("VBTYP")))\
        .withColumn("SHIP_CNTNR_NUM", trim(col("SIGNI")))\
        .withColumn("SHIP_STS_CD", trim(col("STTRG")))\
        .withColumn("SHIP_TYPE_CD", trim(col("SHTYP")))\
        .withColumn("TRSPN_PLNG_PT", trim(col("TPLST")))\
        .withColumn("NM_PRSN_RESP_CREAT_OBJ", trim(col("ERNAM")))\
        .withColumn(
          "CRT_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("ERDAT"), col("ERZET")), "yyyyMMddHHmmss"))
        )\
        .withColumn("NM_PRSN_CHG_OBJ", trim(col("AENAM")))\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("AEDAT"), col("AEZET")), "yyyyMMddHHmmss"))
        )\
        .withColumn("LEG_DTRMN", trim(col("STERM")))\
        .withColumn("SHIP_CMPLT_TYPE", trim(col("ABFER")))\
        .withColumn("PRCSG_CNTL", trim(col("ABWST")))\
        .withColumn("SRVC_LVL", trim(col("BFART")))\
        .withColumn("SHIPPING_TYPE", trim(col("VSART")))\
        .withColumn("SHIPPING_TYPE_PRLM_LEG", trim(col("VSAVL")))\
        .withColumn("SHIPPING_TYPE_SUBSQ_LEG", trim(col("VSANL")))\
        .withColumn("LEG_IN", trim(col("LAUFK")))\
        .withColumn("SHIPPING_COND", trim(col("VSBED")))\
        .withColumn("SHIP_RTE", trim(col("ROUTE")))\
        .withColumn("EXTRNL_ID_1", trim(col("EXTI1")))\
        .withColumn("EXTRNL_ID_2", trim(col("EXTI2")))\
        .withColumn("DESC_SHIP", trim(col("TPBEZ")))\
        .withColumn("STS_TRSPN_PLNG", trim(col("STDIS")))\
        .withColumn(
          "END_PLNG_DTTM",
          when((col("DTDIS") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DTDIS"), col("UZDIS")), "yyyyMMddHHmmss"))
        )\
        .withColumn("STS_OF_CHKIN", trim(col("STREG")))\
        .withColumn(
          "PLAN_CHKIN_DTTM",
          when((col("DPREG") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DPREG"), col("UPREG")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "CHKIN_DTTM",
          when((col("DAREG") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DAREG"), col("UAREG")), "yyyyMMddHHmmss"))
        )\
        .withColumn("STS_STRT_LD", trim(col("STLBG")))\
        .withColumn(
          "PLAN_LD_STRT_DTTM",
          when((col("DPLBG") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DPLBG"), col("UPLBG")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "CUR_STRT_LD_DTTM",
          when((col("DALBG") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DALBG"), col("UALBG")), "yyyyMMddHHmmss"))
        )\
        .withColumn("STS_FOR_END_OF_LD", trim(col("STLAD")))\
        .withColumn(
          "PLAN_END_LD_DTTM",
          when((col("DPLEN") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DPLEN"), col("UPLEN")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "ACTL_END_LD_DTTM",
          when((col("DALEN") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DALEN"), col("UALEN")), "yyyyMMddHHmmss"))
        )\
        .withColumn("STS_SHIP_CMPLT", trim(col("STABF")))\
        .withColumn("STS_FOR_STRT_OF_SHIP", trim(col("STTBG")))\
        .withColumn(
          "PLAN_TRNSP_STRT_DTTM",
          when((col("DPTBG") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DPTBG"), col("UPTBG")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "ACTUAL_TRNSP_STRT_DTTM",
          when((col("DATBG") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DATBG"), col("UATBG")), "yyyyMMddHHmmss"))
        )\
        .withColumn("STS_END_SHIP", trim(col("STTEN")))\
        .withColumn(
          "PLAN_TRNSP_END_DTTM",
          when((col("DPTEN") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DPTEN"), col("UPTEN")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "ACTL_SHIP_END_DTTM",
          when((col("DATEN") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("DATEN"), col("UATEN")), "yyyyMMddHHmmss"))
        )\
        .withColumn("NUM_FRWD_AGN", trim(col("TDLNR")))\
        .withColumn("ORDR_NUM", trim(col("TERNR")))\
        .withColumn("SHIP_CONT_HNDG_UNIT", trim(col("PKSTK")))\
        .withColumn("UNIT_WT_TRSPN_PLNG", trim(col("DTMEG")))\
        .withColumn("VOL_UNIT_TRSPN_PLNG", trim(col("DTMEV")))\
        .withColumn("DSTC", trim(col("DISTZ")).cast(DecimalType(18, 4)))\
        .withColumn("DSTC_UNIT_MEAS", trim(col("MEDST")))\
        .withColumn("TRAVL_TIME_ONLY_BTWN_TWO_LOC", trim(col("FAHZT")).cast(DecimalType(18, 4)))\
        .withColumn("TOT_TRAVL_TIME_BTWN_TWO_LOC_INCL_BRK", trim(col("GESZT")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_MEAS_TRAVL_TM", trim(col("MEIZT")))\
        .withColumn("UPDT_GRP_STATS_UPDT", trim(col("STAFO")))\
        .withColumn("STS_SHIP_COST_CALC", trim(col("FBSTA")))\
        .withColumn("OVRL_STS_CALC_SHIP_COST_SHIP", trim(col("FBGST")))\
        .withColumn("STS_SHIP_COST_SETLM", trim(col("ARSTA")))\
        .withColumn("TOT_STS_SHIP_COST_SETLM_SHIP", trim(col("ARGST")))\
        .withColumn("LEG_DTRMN_CMPLT", trim(col("STERM_DONE")))\
        .withColumn("HNDG_UNIT_DATA_REF_SHIP_COST_DOC", trim(col("VSE_FRK")))\
        .withColumn("PRC_PCDR_SHIP_HDR", trim(col("KKALSM")))\
        .withColumn("SPL_PRCSG_IN", trim(col("SDABW")))\
        .withColumn("SHIP_COST_RLVNT", trim(col("FRKRL")))\
        .withColumn("PLAN_TOT_TIME_TRSPN", trim(col("GESZTD")))\
        .withColumn("PLAN_DUR_TRSPN", trim(col("FAHZTD")))\
        .withColumn("ACTL_TOT_TIME_SHIP", trim(col("GESZTDA")))\
        .withColumn("ACTL_TIME_NEED_TRSPN", trim(col("FAHZTDA")))\
        .withColumn("RTE_COPY_FROM_DELV", trim(col("ROCPY_DONE")))\
        .withColumn("GLOBL_UNIQ_ID", trim(col("HANDLE")))\
        .withColumn("TIME_SGMNT_EXISTS", trim(col("TSEGFL")))\
        .withColumn("TIME_SGMNT_EV_GRP_SHIP_HDR", trim(col("TSEGTP")))\
        .withColumn("SUP_1", trim(col("ADD01")))\
        .withColumn("SUP_2", trim(col("ADD02")))\
        .withColumn("SUP_3", trim(col("ADD03")))\
        .withColumn("SUP_4", trim(col("ADD04")))\
        .withColumn("ADDID_TEXT_1", trim(col("TEXT1")))\
        .withColumn("ADDID_TEXT_2", trim(col("TEXT2")))\
        .withColumn("ADDID_TEXT_3", trim(col("TEXT3")))\
        .withColumn("ADDID_TEXT_4", trim(col("TEXT4")))\
        .withColumn("DNGRS_GOODS_MGMT_PRFL_SD_DOC", trim(col("PROLI")))\
        .withColumn("BLK_IN_DNGRS_GOODS", trim(col("DGTLOCK")))\
        .withColumn(
          "SLCT_DNGRS_GOODS_MSTR_DATA_DTTM",
          when((col("DGMDDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("DGMDDAT"), "yyyyMMdd"))
        )\
        .withColumn("IN_DOC_CONT_DNGRS_GOODS", trim(col("CONT_DG")))\
        .withColumn("PLAN_WAIT_TIME_SHIP", trim(col("WARZTD")))\
        .withColumn("CUR_WAIT_TIME_SHIP", trim(col("WARZTDA")))\
        .withColumn("RTE_SCHED", trim(col("AULWE")))\
        .withColumn("TENDER_STS", trim(col("TNDRST")))\
        .withColumn("ACC_COND_REJ_RSN", trim(col("TNDRRC")))\
        .withColumn("TENDER_TEXT", trim(col("TNDR_TEXT")))\
        .withColumn("MAX_PRC_SHIP", trim(col("TNDR_MAXP")).cast(DecimalType(18, 4)))\
        .withColumn("CRNCY_MAX_PRC", trim(col("TNDR_MAXC")).cast(DecimalType(18, 4)))\
        .withColumn("ACTL_SHIP_COST_SHIP", trim(col("TNDR_ACTP")).cast(DecimalType(18, 4)))\
        .withColumn("CRNCY_ACTL_SHIP_COST", trim(col("TNDR_ACTC")))\
        .withColumn("FRWD_AGN_ACPT_SHIP", trim(col("TNDR_CARR")))\
        .withColumn("NM_CARR_ACPT_SHIP", trim(col("TNDR_CRNM")))\
        .withColumn("FRWD_AGN_TCKG_ID", trim(col("TNDR_TRKID")))\
        .withColumn(
          "QTA_EXP_DTTM",
          when((col("TNDR_EXPD") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("TNDR_EXPD"), col("TNDR_EXPT")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "EARLY_PCKUP_DTTM",
          when((col("TNDR_ERPD") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("TNDR_ERPD"), col("TNDR_ERPT")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "LTST_PCKUP_DTTM",
          when((col("TNDR_LTPD") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("TNDR_LTPD"), col("TNDR_LTPT")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "EARLY_DELV_DTTM",
          when((col("TNDR_ERDD") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("TNDR_ERDD"), col("TNDR_ERDT")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "LTST_DELV_DTTM",
          when((col("TNDR_LTDD") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("TNDR_LTDD"), col("TNDR_LTDT")), "yyyyMMddHHmmss"))
        )\
        .withColumn("LGTH_LD_PLTF", trim(col("TNDR_LDLG")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_MEAS_LD_LGTH", trim(col("TNDR_LDLU")))\
        .withColumn("IN_HUS_RLVNT_DELV_ITM_GNR", trim(col("KZHULFR")))\
        .withColumn("ALLW_TOT_WT_SHIP", trim(col("ALLOWED_TWGT")))\
        .withColumn("DSTN_STS", trim(col("VLSTK")))\
        .withColumn("SHIP_DSTN_ORIG_SYS", trim(col("VERURSYS")))\
        .withColumn("ID_NUM_CONT_MOVE", trim(col("CM_IDENT")))\
        .withColumn("SEQ_TRNSP_CNCT_TRFC", trim(col("CM_SEQUENCE")))\
        .withColumn("EXTRNL_FRGHT_ORDR_ID", trim(col("EXT_FREIGHT_ORD")))\
        .withColumn("KEY_NM_BUSN_SYS", trim(col("EXT_TM_SYS")))\
        .withColumn("DRVR_1", trim(col("_BEV1_RPFAR1")))\
        .withColumn("DRVR_2", trim(col("_BEV1_RPFAR2")))\
        .withColumn("VEH", trim(col("_BEV1_RPMOWA")))\
        .withColumn("TRLR", trim(col("_BEV1_RPANHAE")))\
        .withColumn("LD_SEQ_NUM_TOUR", trim(col("_BEV1_RPFLGNR")))\
        .withColumn("STS_VEH_SPACE_OPTZ", trim(col("_VSO_R_STATUS")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SHIP_NUM', SHIP_NUM, 'DELV_TYP_CD', DELV_TYP_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'CO_CD', CO_CD, 'DELV_LINE_NBR', DELV_LINE_NBR)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SHIP_NUM', SHIP_NUM, 'DELV_TYP_CD', DELV_TYP_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'CO_CD', CO_CD, 'DELV_LINE_NBR', DELV_LINE_NBR)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
