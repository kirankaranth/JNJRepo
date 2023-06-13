from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_ldgr_deu_djd_jem_jet_sjd.config.ConfigStore import *
from jde_md_sls_ordr_hist_ldgr_deu_djd_jem_jet_sjd.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("ORDR_TYPE", col("SLDCTO"))\
        .withColumn(
          "UPDT_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLUPMJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("TIME_OF_DAY", col("SLTDAY"))\
        .withColumn("LINE_NUM", col("SLLNID"))\
        .withColumn("ORDR_CO", col("SLKCOO"))\
        .withColumn("DOC_NUM", col("SLDOCO"))\
        .withColumn("ORDR_SFX", trim(col("SLSFXO")))\
        .withColumn("BUSN_UNIT", trim(col("SLMCU")))\
        .withColumn("CO", trim(col("SLCO")))\
        .withColumn("DOC_CO_ORIG_ORDR", trim(col("SLOKCO")))\
        .withColumn("ORIG_ORDR_NUM", trim(col("SLOORN")))\
        .withColumn("ORIG_ORDR_TYPE", trim(col("SLOCTO")))\
        .withColumn("ORIG_LINE_NUM", trim(col("SLOGNO")))\
        .withColumn("COMPANY_KEY", trim(col("SLRKCO")))\
        .withColumn("RLTD_PO_NUM", trim(col("SLRORN")))\
        .withColumn("RLTD_PO_ORDR_TYPE", trim(col("SLRCTO")))\
        .withColumn("RLTD_PO_LINE_NUM", trim(col("SLRLLN")))\
        .withColumn("AGMT_NUM_DSTN", trim(col("SLDMCT")))\
        .withColumn("AGMT_SPLMN_DSTN", trim(col("SLDMCS")))\
        .withColumn("ADDR_NUM", trim(col("SLAN8")))\
        .withColumn("ADDR_NUM_SHIP_TO", trim(col("SLSHAN")))\
        .withColumn("ADDR_NUM_PARNT", trim(col("SLPA8")))\
        .withColumn(
          "RQST_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLDRQJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLDRQJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "ORDR_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLTRDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLTRDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SCHD_PICK_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLPDDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLPDDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "ACTL_SHIP_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLADDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLADDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "INVC_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLIVD") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLIVD"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "CAN_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLCNDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLCNDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "FOR_GL_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLDGL") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLDGL"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "PROM_DELV_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLRSDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLRSDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "PRC_EFF_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLPEFJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLPEFJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "PROM_SHIP_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLPPDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLPPDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("REF", trim(col("SLVR01")))\
        .withColumn("REF_2", trim(col("SLVR02")))\
        .withColumn("ITM_NUM_SHRT", trim(col("SLITM")))\
        .withColumn("ITM_NUM_2", trim(col("SLLITM")))\
        .withColumn("ITM_NUM_3", trim(col("SLAITM")))\
        .withColumn("LOC", trim(col("SLLOCN")))\
        .withColumn("LOT_SER_NUM", trim(col("SLLOTN")))\
        .withColumn("FROM_GRADE", trim(col("SLFRGD")))\
        .withColumn("THRU_GRADE", trim(col("SLTHGD")))\
        .withColumn("FROM_POTENCY", trim(col("SLFRMP")).cast(DecimalType(18, 4)))\
        .withColumn("THRU_POTENCY", trim(col("SLTHRP")).cast(DecimalType(18, 4)))\
        .withColumn("DAYS_BEF_EXPN", trim(col("SLEXDP")).cast(IntegerType()))\
        .withColumn("DESC", trim(col("SLDSC1")))\
        .withColumn("DESC_LINE_2", trim(col("SLDSC2")))\
        .withColumn("LINE_TYPE", trim(col("SLLNTY")))\
        .withColumn("STS_CD_NEXT", trim(col("SLNXTR")))\
        .withColumn("STS_CD_LAST", trim(col("SLLTTR")))\
        .withColumn("BUSN_UNIT_HDR", trim(col("SLEMCU")))\
        .withColumn("ITM_NUM_RLTD", trim(col("SLRLIT")))\
        .withColumn("KIT_MSTR_LINE_NUM", trim(col("SLKTLN")))\
        .withColumn("CMPNT_LINE_NUM", trim(col("SLCPNT")))\
        .withColumn("RLTD_KIT_CMPNT", trim(col("SLRKIT")))\
        .withColumn("NUM_OF_CMPNT_PER_PARNT", trim(col("SLKTP")))\
        .withColumn("SLS_CATLG_SECTN", trim(col("SLSRP1")))\
        .withColumn("SUB_SECTN", trim(col("SLSRP2")))\
        .withColumn("SLS_CAT_CD_3", trim(col("SLSRP3")))\
        .withColumn("SLS_CAT_CD_4", trim(col("SLSRP4")))\
        .withColumn("SLS_CAT_CD_5", trim(col("SLSRP5")))\
        .withColumn("CMMDTY_CLS", trim(col("SLPRP1")))\
        .withColumn("CMMDTY_SUB_CLS", trim(col("SLPRP2")))\
        .withColumn("SUP_REBT_CD", trim(col("SLPRP3")))\
        .withColumn("MSTR_PLNG_FMLY", trim(col("SLPRP4")))\
        .withColumn("PRCHSNG_CAT_CD_5", trim(col("SLPRP5")))\
        .withColumn("UNIT_OF_MEAS_AS_INP", trim(col("SLUOM")))\
        .withColumn("UNIT_ORDR_QTY", trim(col("SLUORG")).cast(DecimalType(18, 4)))\
        .withColumn("QTY_SHIP", trim(col("SLSOQS")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_QTY_BKORD_HELD", trim(col("SLSOBK")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_QTY_CAN_SCRAP", trim(col("SLSOCN")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_FUT_QTY_CMT", trim(col("SLSONE")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_OPEN", trim(col("SLUOPN")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_SHIP_TO_DT", trim(col("SLQTYT")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_RELIEVED", trim(col("SLQRLV")).cast(DecimalType(18, 4)))\
        .withColumn("CMT", trim(col("SLCOMM")))\
        .withColumn("OTH_QTY", trim(col("SLOTQY")))\
        .withColumn("AMT_PRC_PER_UNIT", trim(col("SLUPRC")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_EXTD_PRC", trim(col("SLAEXP")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_OPEN", trim(col("SLAOPN")).cast(DecimalType(18, 4)))\
        .withColumn("PRC_OVRD_CD", trim(col("SLPROV")))\
        .withColumn("TEMP_PRC", trim(col("SLTPC")))\
        .withColumn("UNIT_OF_MEAS_ENT_FOR_UNIT_PRC", trim(col("SLAPUM")))\
        .withColumn("AMT_LIST_PRC", trim(col("SLLPRC")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_UNIT_COST", trim(col("SLUNCS")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_EXTD_COST", trim(col("SLECST")).cast(DecimalType(18, 4)))\
        .withColumn("COST_OVRD_CD", trim(col("SLCSTO")))\
        .withColumn("EXTD_COST_TFR", trim(col("SLTCST")).cast(DecimalType(18, 4)))\
        .withColumn("PRT_MSG", trim(col("SLINMG")))\
        .withColumn("PMT_TERM_CD", trim(col("SLPTC")))\
        .withColumn("PMT_INSTM", trim(col("SLRYIN")))\
        .withColumn("BAS_ON_DT", trim(col("SLDTBS")))\
        .withColumn("DISC_TRD", trim(col("SLTRDC")).cast(DecimalType(18, 4)))\
        .withColumn("TRD_DISC", trim(col("SLFUN2")).cast(DecimalType(18, 4)))\
        .withColumn("PRC_AND_ADJ_SCHED", trim(col("SLASN")))\
        .withColumn("ITM_PRC_GRP", trim(col("SLPRGR")))\
        .withColumn("PRC_CAT_LVL", trim(col("SLCLVL")))\
        .withColumn("DISC_CASH", trim(col("SLCADC")).cast(DecimalType(18, 4)))\
        .withColumn("DOC_CO", trim(col("SLKCO")))\
        .withColumn("DOC", trim(col("SLDOC")))\
        .withColumn("DOC_TYPE", trim(col("SLDCT")))\
        .withColumn("DOC_ORIG", trim(col("SLODOC")))\
        .withColumn("DOC_TYPE_ORIG", trim(col("SLODCT")))\
        .withColumn("DOC_CO_ORIG", trim(col("SLOKC")))\
        .withColumn("PICK_SLIP_NUM", trim(col("SLPSN")))\
        .withColumn("DELV_NUM", trim(col("SLDELN")))\
        .withColumn("SLS_TAX", trim(col("SLTAX1")))\
        .withColumn("TAX_RATE", trim(col("SLTXA1")))\
        .withColumn("TAX_EXPL_CD_1", trim(col("SLEXR1")))\
        .withColumn("ASSCTD_TEXT", trim(col("SLATXT")))\
        .withColumn("PRIR_PRCSG", trim(col("SLPRIO")))\
        .withColumn("PRTD_CD", trim(col("SLRESL")))\
        .withColumn("BKORD_ALLW", trim(col("SLBACK")))\
        .withColumn("SUBST_ALLW", trim(col("SLSBAL")))\
        .withColumn("PRTL_LINE_SHIP_ALLW", trim(col("SLAPTS")))\
        .withColumn("LINE_OF_BUSN", trim(col("SLLOB")))\
        .withColumn("END_USE", trim(col("SLEUSE")))\
        .withColumn("DUTY_STS", trim(col("SLDTYS")))\
        .withColumn("NATR_OF_TRX", trim(col("SLNTR")))\
        .withColumn("PRMRY_LAST_SUP_NUM", trim(col("SLVEND")))\
        .withColumn("CARR_NUM", trim(col("SLCARS")))\
        .withColumn("MODE_OF_TRNSP", trim(col("SLMOT")))\
        .withColumn("RTE_CD", trim(col("SLROUT")))\
        .withColumn("STOP_CD", trim(col("SLSTOP")))\
        .withColumn("ZN_NUM", trim(col("SLZON")))\
        .withColumn("CNTNR_ID", trim(col("SLCNID")))\
        .withColumn("FRGHT_HNDG_CD", trim(col("SLFRTH")))\
        .withColumn("SHIPPING_CMMDTY_CLS", trim(col("SLSHCM")))\
        .withColumn("SHIPPING_COND_CD", trim(col("SLSHCN")))\
        .withColumn("SER_NUM_LOT", trim(col("SLSERN")))\
        .withColumn("UNIT_OF_MEAS_PRMRY", trim(col("SLUOM1")))\
        .withColumn("UNIT_PRMRY_QTY_ORD", trim(col("SLPQOR")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_OF_MEAS_SEC", trim(col("SLUOM2")))\
        .withColumn("UNIT_SEC_QTY_ORD", trim(col("SLSQOR")).cast(DecimalType(18, 4)))\
        .withColumn("UNIT_OF_MEAS_PRC", trim(col("SLUOM4")))\
        .withColumn("UNIT_WT", trim(col("SLITWT")).cast(DecimalType(18, 4)))\
        .withColumn("WT_UNIT_OF_MEAS", trim(col("SLWTUM")))\
        .withColumn("UNIT_VOL", trim(col("SLITVL")).cast(DecimalType(18, 4)))\
        .withColumn("VOL_UNIT_OF_MEAS", trim(col("SLVLUM")))\
        .withColumn("REPRC_BSK_PRC_CAT", trim(col("SLRPRC")))\
        .withColumn("ORDR_REPRC_CAT", trim(col("SLORPR")))\
        .withColumn("ORDR_REPRC_IN", trim(col("SLORP")))\
        .withColumn("COST_METH_INV", trim(col("SLCMGP")))\
        .withColumn("GL_OFFST", trim(col("SLGLC")))\
        .withColumn("CEN", trim(col("SLCTRY")))\
        .withColumn("FISC_YR", trim(col("SLFY")))\
        .withColumn("INTER_BRNCH_SLS", trim(col("SLSO01")))\
        .withColumn("ON_HAND_UPDT", trim(col("SLSO02")))\
        .withColumn("CNFG_PRT_FL", trim(col("SLSO03")))\
        .withColumn("SLS_ORDR_STS_04", trim(col("SLSO04")))\
        .withColumn("SUBST_ITM_IN", trim(col("SLSO05")))\
        .withColumn("PREF_CMMT_IN", trim(col("SLSO06")))\
        .withColumn("SHIP_DT_OVRD", trim(col("SLSO07")))\
        .withColumn("PRC_ADJ_LINE_IN", trim(col("SLSO08")))\
        .withColumn("PRC_ADJ_HIST_IN", trim(col("SLSO09")))\
        .withColumn("PREF_PRDTN_ALLC", trim(col("SLSO10")))\
        .withColumn("TFR_DIR_SHIP_INTCO_FL", trim(col("SLSO11")))\
        .withColumn("DFR_ENTR_FL", trim(col("SLSO12")))\
        .withColumn("EURO_CONV_STS_FL", trim(col("SLSO13")))\
        .withColumn("SLS_ORDR_STS_14", trim(col("SLSO14")))\
        .withColumn("SLS_ORDR_STS_15", trim(col("SLSO15")))\
        .withColumn("APPL_COMMSN", trim(col("SLACOM")))\
        .withColumn("COMMSN_CAT", trim(col("SLCMCG")))\
        .withColumn("RSN_CD", trim(col("SLRCD")))\
        .withColumn("GRS_WT", trim(col("SLGRWT")).cast(DecimalType(18, 4)))\
        .withColumn("GRS_WT_UNIT_OF_MEAS", trim(col("SLGWUM")))\
        .withColumn("SUBLDGR_GL", trim(col("SLSBL")))\
        .withColumn("SUBLDGR_TYPE", trim(col("SLSBLT")))\
        .withColumn("CD_LOC_TAX_STS", trim(col("SLLCOD")))\
        .withColumn("PRC_CD_1", trim(col("SLUPC1")))\
        .withColumn("PRC_CD_2", trim(col("SLUPC2")))\
        .withColumn("PRC_CD_3", trim(col("SLUPC3")))\
        .withColumn("STS_IN_WHSE", trim(col("SLSWMS")))\
        .withColumn("WRK_ORDR_FRZ_CD", trim(col("SLUNCD")))\
        .withColumn("SEND_METH", trim(col("SLCRMD")))\
        .withColumn("CRNCY_CD_FROM", trim(col("SLCRCD")))\
        .withColumn("CRNCY_CONV_RT_SPOT_RT", trim(col("SLCRR")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_LIST_PRC_PER_UNIT", trim(col("SLFPRC")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_FRGN_PRC_PER_UNIT", trim(col("SLFUP")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_FRGN_EXTD_PRC", trim(col("SLFEA")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_FRGN_UNIT_COST", trim(col("SLFUC")).cast(DecimalType(18, 4)))\
        .withColumn("AMT_FRGN_EXTD_COST", trim(col("SLFEC")).cast(DecimalType(18, 4)))\
        .withColumn("USER_RSRV_CD", trim(col("SLURCD")))\
        .withColumn(
          "USER_RSRV_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLURDT") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLURDT"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("USER_RSRV_AMT", trim(col("SLURAT")).cast(DecimalType(18, 4)))\
        .withColumn("USER_RSRV_NUM", trim(col("SLURAB")).cast(DecimalType(18, 4)))\
        .withColumn("USER_RSRV_REF", trim(col("SLURRF")))\
        .withColumn("TRX_ORIGNTR", trim(col("SLTORG")))\
        .withColumn("USER_ID", trim(col("SLUSER")))\
        .withColumn("PRG_ID", trim(col("SLPID")))\
        .withColumn("WRK_STN_ID", trim(col("SLJOBN")))\
        .withColumn(
          "ORIG_PROM_DELV_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLOPDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLOPDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("BUY_NUM", trim(col("SLANBY")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", col("_deleted_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'ORDR_TYPE', ORDR_TYPE, 'UPDT_DTTM', UPDT_DTTM, 'TIME_OF_DAY', TIME_OF_DAY, 'LINE_NUM', LINE_NUM, 'ORDR_CO', ORDR_CO, 'DOC_NUM', DOC_NUM)"
            )
          )
        )\
        .withColumn("_pk_md5_", md5(
        to_json(
          expr(
            "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'ORDR_TYPE', ORDR_TYPE, 'UPDT_DTTM', UPDT_DTTM, 'TIME_OF_DAY', TIME_OF_DAY, 'LINE_NUM', LINE_NUM, 'ORDR_CO', ORDR_CO, 'DOC_NUM', DOC_NUM)"
          )
        )
    ))
