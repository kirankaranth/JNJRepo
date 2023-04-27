from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_svs.config.ConfigStore import *
from sap_01_md_sls_ordr_line_svs.udfs.UDFs import *

def Join_1(spark: SparkSession, VBAP: DataFrame, VBAK: DataFrame, VBKD: DataFrame, TVST: DataFrame) -> DataFrame:
    return VBAP\
        .alias("VBAP")\
        .join(VBAK.alias("VBAK"), (col("VBAP.VBELN") == col("VBAK.VBELN")), "left_outer")\
        .join(
          VBKD.alias("VBKD"),
          ((col("VBAP.VBELN") == col("VBKD.VBELN")) & (col("VBAP.POSNR") == col("VBKD.POSNR"))),
          "left_outer"
        )\
        .join(TVST.alias("TVST"), (col("VBAP.VSTEL") == col("TVST.VSTEL")), "left_outer")\
        .select(
        lit(Config.sourceSystem).alias("SRC_SYS_CD"), 
        trim(col("VBAK.BUKRS_VF")).alias("COMPANY_CD"), 
        trim(col("VBAP.VBELN")).alias("SLS_ORDR_DOC_ID"), 
        trim(col("VBAP.POSNR")).alias("SLS_ORDR_LINE_NBR"), 
        trim(col("VBAK.AUART")).alias("SLS_ORDR_TYPE_CD"), 
        trim(col("VBAP.ABGRU")).alias("REJ_RSN_CD"), 
        trim(col("VBAP.FAKSP")).alias("BILL_BLK_LINE_CD"), 
        trim(col("VBAP.FKREL")).alias("BILL_RLVNT_ID"), 
        col("VBAP.KBMENG").cast(DecimalType(18, 4)).alias("CNFRM_QTY"), 
        col("KWMENG").cast(DecimalType(18, 4)).alias("SLS_UNIT_ORDR_QTY"), 
        trim(col("VBAP.LFREL")).alias("DELV_RLVNT_IND"), 
        trim(col("VBAP.LGORT")).alias("SLOC_CD"), 
        trim(col("VBAP.MATNR")).alias("MATL_NUM"), 
        col("VBAP.NETWR").cast(DecimalType(18, 4)).alias("NET_VAL_AMT"), 
        trim(col("VBAP.PSTYV")).alias("LINE_ITEM_CAT_CD"), 
        trim(col("VBAP.ROUTE")).alias("RTE_ID"), 
        trim(col("VBAP.VRKME")).alias("SLS_UOM_CD"), 
        trim(col("VBAP.VSTEL")).alias("SHIPPING_PT_ID"), 
        trim(col("VBAP.WAERK")).alias("SLS_ORDR_CRNCY_CD"), 
        trim(col("VBAP.SPART")).alias("DIVISION_CD"), 
        trim(col("VBAP.WERKS")).alias("PLNT_CD"), 
        trim(col("VBKD.INCO1")).alias("INTNL_COM_CD"), 
        trim(col("VBKD.INCO2")).alias("DEL_DPRT_PT_CD"), 
        when((col("VBAK.FMBDAT") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("VBAK.FMBDAT"), "yyyyMMdd"))\
          .alias("ORG_MTL_AVAL_DTTM"), 
        when((col("VBAP.erdat") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(concat(col("VBAP.erdat"), col("VBAP.erzet")), "yyyyMMddHHmmss"))\
          .alias("CR_DTTM"), 
        lookup("LU_SAP_TVAGT", col("ABGRU")).getField("BEZEI").alias("REJ_RSN_DESC"), 
        lookup("LU_SAP_TVAPT", col("PSTYV")).getField("VTEXT").alias("LINE_ITEM_CAT_DESC"), 
        col("NETPR").cast(DecimalType(18, 4)).alias("NET_PRC_AMT"), 
        trim(col("VBAP.MATWA")).alias("ENT_MATL_NUM"), 
        trim(col("VBAP.MEINS")).alias("BASE_UOM_CD"), 
        trim(col("VBKD.BSTKD")).alias("CUST_PO_NUM"), 
        col("VBAP.UMVKZ").cast(DecimalType(18, 4)).alias("FCTR_NMRTR_MEAS"), 
        col("VBAP.UMVKN").cast(DecimalType(18, 4)).alias("FCTR_DNMNTR_MEAS"), 
        col("VBAP.LSMENG").cast(DecimalType(18, 4)).alias("SLS_VOL_QTY"), 
        col("VBAP.BRGEW").cast(DecimalType(18, 4)).alias("GRS_WT_MEAS"), 
        trim(col("VBAP.GEWEI")).alias("WT_UOM_CD"), 
        col("VBAP.NTGEW").cast(DecimalType(18, 4)).alias("NET_WT_MEAS"), 
        trim(col("VBAP.KDMAT")).alias("MATL_NUM_USED_BY_CUST"), 
        col("VBAP.KLMENG").cast(DecimalType(18, 4)).alias("CUM_CNFRM_QTY_BASE_UNIT"), 
        trim(col("VBAP.KZTLF")).alias("PRTL_DELV_ITM_LVL"), 
        trim(col("VBAP.PRODH")).alias("PROD_HIER_CD"), 
        trim(col("VBAP.STKEY")).alias("BOM_ORIG"), 
        trim(col("VBAP.UEPOS")).alias("PARNT_BOM_CNTR_NBR"), 
        trim(col("VBAP.VOLEH")).alias("VOL_UOM_CD"), 
        col("VBAP.VOLUM").cast(DecimalType(18, 4)).alias("VOL_MEAS"), 
        col("VBAP.WAVWR").cast(DecimalType(18, 4)).alias("COST_IN_DOC_CRNCY"), 
        col("VBAP.UMZIZ").cast(DecimalType(18, 4)).alias("FACT_FOR_CNV_SLS_UNIT_TO_BASE_UNIT"), 
        trim(col("VBAP.ARKTX")).alias("MATL_SHRT_DESC"), 
        trim(col("VBAP.CHARG")).alias("BTCH_NUM"), 
        col("VBAP.LFMNG").cast(DecimalType(18, 4)).alias("MIN_DELV_QTY"), 
        trim(col("VBAP.LPRIO")).alias("DELV_PRIR"), 
        trim(col("VBAP.MATKL")).alias("MATL_GRP_CD"), 
        trim(col("VBAP.ZIEME")).alias("TRGT_QTY_UOM"), 
        trim(col("VBAP.GRKOR")).alias("DELV_GRP"), 
        when((col("VBAP.aedat") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("VBAP.aedat"), "yyyyMMdd"))\
          .alias("CHG_DTTM"), 
        col("VBAP.KZWI2").cast(DecimalType(18, 4)).alias("SBTOT_2_PRC_PCDR"), 
        col("VBAP.KZWI3").cast(DecimalType(18, 4)).alias("SBTOT_3_PRC_PCDR"), 
        col("VBAP.KZWI4").cast(DecimalType(18, 4)).alias("SBTOT_4_PRC_PCDR"), 
        col("VBAP.KZWI5").cast(DecimalType(18, 4)).alias("SBTOT_5_PRC_PCDR"), 
        col("VBAP.KZWI6").cast(DecimalType(18, 4)).alias("SBTOT_6_PRC_PCDR"), 
        col("VBAP.AWAHR").cast(IntegerType()).alias("ORDR_PRBLTY_ITM"), 
        trim(col("VBAP.SHKZG")).alias("RTRN_ITM"), 
        trim(col("VBAP.SKTOF")).alias("DR_CR_IN"), 
        col("VBAP.ZMENG").cast(DecimalType(18, 4)).alias("TRGT_QTY_SLS_UNIT"), 
        trim(col("VBAP.ERNAM")).alias("CRT_BY_NM"), 
        trim(col("VBAP.MVGR1")).alias("MATL_GRP_1"), 
        lookup("LU_SAP_TVM1T", col("MVGR1")).getField("BEZEI").alias("MATL_GRP_1_DESC"), 
        trim(col("VBAP.MVGR2")).alias("MATL_GRP_2_MVGR2"), 
        lookup("LU_SAP_TVM2T", col("MVGR2")).getField("BEZEI").alias("MATL_GRP_2_DESC"), 
        trim(col("VBAP.MVGR3")).alias("MATL_GRP_3"), 
        lookup("LU_SAP_TVM3T", col("MVGR3")).getField("BEZEI").alias("MATL_GRP_3_DESC"), 
        trim(col("VBAP.MVGR4")).alias("MATL_GRP_4"), 
        trim(col("VBAP.MVGR5")).alias("MATL_GRP_5"), 
        lookup("LU_SAP_TVM5T", col("MVGR5")).getField("BEZEI").alias("MATL_GRP_5_DESC"), 
        trim(col("VBAP.AUFNR")).alias("ORDR_NUM"), 
        col("VBAP.KPEIN").cast(DecimalType(18, 4)).alias("COND_PRC_UNIT"), 
        lookup("LU_SAP_TVSTT", col("VBAP.VSTEL")).getField("VTEXT").alias("SHIPPING_PT_DESC"), 
        trim(col("TVST.LOADTN")).alias("LD_TIME_WRK_HRS"), 
        trim(col("TVST.PIPATN")).alias("PICK_PACK_TIME"), 
        trim(col("TVST.TSTRID")).alias("WRK_TIMES"), 
        trim(col("TVST.FABKL")).alias("SHIPPING_PT_FCTRY_CAL"), 
        col("TVST.LOADTG").cast(DecimalType(18, 4)).alias("LD_TIME_WRK_DAYS"), 
        col("TVST.PIPATG").cast(DecimalType(18, 4)).alias("PICK_PACK_TIME_WRK_DAYS"), 
        trim(col("TVST.ALAND")).alias("SHIPPING_PT_CTRY"), 
        lookup("LU_SAP_TVRO", col("ROUTE")).getField("SPFBK").alias("RTE_FCTRY_CAL"), 
        lookup("LU_SAP_TVRO", col("ROUTE")).getField("TDVZTD").alias("TRSPN_LEAD_TIME_IN_CAL_DAYS"), 
        lookup("LU_SAP_TVRO", col("ROUTE")).getField("TRAZTD").alias("TRST_DUR_IN_CAL_DAYS"), 
        lookup("LU_SAP_TVROT", col("ROUTE")).getField("BEZEI").alias("RTE_DESC"), 
        when((trim(col("VBKD.bstdk")) == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(trim(col("VBKD.bstdk")), "yyyyMMdd"))\
          .alias("CUST_PO_DTTM"), 
        col("VBAP.UMZIN").cast(DecimalType(18, 4)).alias("CONV_FACT"), 
        trim(col("VBAP.GSBER")).alias("BUSN_AREA"), 
        trim(col("VBAP.TAXM1")).alias("TAX_CLSN_FOR_MATL_1"), 
        trim(col("VBAP.TAXM2")).alias("TAX_CLSN_FOR_MATL_2"), 
        trim(col("VBAP.TAXM3")).alias("TAX_CLSN_FOR_MATL_3"), 
        trim(col("VBAP.KMEIN")).alias("COND_UOM"), 
        trim(col("VBAP.MTVFP")).alias("AVLBLTY_CHK_GRP"), 
        trim(col("VBAP.SUMBD")).alias("RQR_SUM_UP"), 
        trim(col("VBAP.KTGRM")).alias("ACCT_ASGNMT_GRP"), 
        trim(col("VBAP.PRSOK")).alias("PRC_IS_OK"), 
        trim(col("VBAP.XCHPF")).alias("BTCH_MGMT_REQ_IN"), 
        trim(col("VBAP.XCHAR")).alias("BTCH_MGMT_IN"), 
        trim(col("VBAP.STAFO")).alias("STATS_UPDT_GRP"), 
        col("VBAP.KZWI1").cast(DecimalType(18, 4)).alias("SBTOT_1_PRC_PCDR"), 
        col("VBAP.STCUR").cast(DecimalType(18, 4)).alias("STATS_EXCH_RT"), 
        trim(col("VBAP.EAN11")).alias("EAN_CD"), 
        trim(col("VBAP.PRCTR")).alias("PRFT_CTR"), 
        trim(col("VBAP.PAOBJNR")).alias("PRFT_SGMNT_NUM"), 
        trim(col("VBAP.BEDAE")).alias("RQR_TYPE"), 
        col("VBAP.CMPRE").cast(DecimalType(18, 4)).alias("ITM_CR_PRC_CMPRE"), 
        trim(col("VBAP.BERID")).alias("MRP_AREA"), 
        trim(col("VBAP.J_1BTAXLW3")).alias("ISS_TAX_LAW"), 
        trim(col("VBAP.J_1BTAXLW4")).alias("COFINS_TAX_LAW"), 
        trim(col("VBAP.J_1BTAXLW5")).alias("PIS_TAX_LAW"), 
        trim(col("VBAP.VBELV")).alias("ORIG_DOC"), 
        trim(col("VBAP.POSNV")).alias("ORIG_ITM"), 
        trim(col("VBAP.VGBEL")).alias("REF_DOC_NUM"), 
        trim(col("VBAP.VGPOS")).alias("REF_ITM_NUM"), 
        trim(col("TVST.RIZBS")).alias("DTRMN_PICK_PACK_TIME"), 
        trim(col("TVST.VSTEL")).alias("SHIPPING_PT"), 
        trim(col("TVST.ALAND")).alias("CTRY_CD"), 
        lookup("LU_SAP_TVAPT", col("PSTYV")).getField("PSTYV").alias("ITEM_CAT_CD"), 
        lit(
            "#"
          )\
          .alias("SRC_TBL_NM"), 
        trim(col("VBAP.UPMAT")).alias("MATL_PRC_REF"), 
        trim(col("VBAP.PMATN")).alias("PRC_REF_MATL"), 
        trim(col("VBAP.POSAR")).alias("ITM_TYPE"), 
        trim(col("VBAP.GRPOS")).alias("ITM_ITM_ALT"), 
        col("VBAP.ZWERT").cast(DecimalType(18, 4)).alias("TRGT_VAL_OUTLN_AGMT_DOC_CRNCY"), 
        col("VBAP.SMENG").cast(DecimalType(18, 4)).alias("SCALE_QTY_BASE_UNIT_MEAS"), 
        col("VBAP.ABLFZ").cast(DecimalType(18, 4)).alias("RD_QTY_FOR_DELV"), 
        when((col("VBAP.abdat") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("VBAP.abdat"), "yyyyMMdd"))\
          .alias("RCNL_AGR_CUM_QTY_DTTM"), 
        col("VBAP.ABSFZ").cast(DecimalType(18, 4)).alias("ALLW_DEVT_QTY_ABS"), 
        trim(col("VBAP.POSEX")).alias("ITM_NUM_UNDRLY_PRCH_ORDR"), 
        trim(col("VBAP.KBVER")).alias("ALLW_DEVT_QTY_PCT"), 
        trim(col("VBAP.KEVER")).alias("DAYS_QTY_CN_BE_SHFT"), 
        trim(col("VBAP.VKGRU")).alias("REPAR_PRCSG_CLSN_ITM"), 
        trim(col("VBAP.VKAUS")).alias("USG_IN"), 
        trim(col("VBAP.FMENG")).alias("QTY_IS_FX"), 
        trim(col("VBAP.UEBTK")).alias("UNLTD_OVR_DELV_ALLW"), 
        col("VBAP.UEBTO").cast(DecimalType(18, 4)).alias("OVR_DELV_TLRNC"), 
        col("VBAP.UNTTO").cast(DecimalType(18, 4)).alias("UND_DELV_TLRNC"), 
        trim(col("VBAP.ATPKZ")).alias("REPL_PART"), 
        trim(col("VBAP.RKFKF")).alias("METH_BILL_CO_PPC_ORD"), 
        trim(col("VBAP.ANTLF")).alias("MAX_NUM_PRTL_DELV_ALLW_PER_ITM"), 
        trim(col("VBAP.CHSPL")).alias("BTCH_SPLT_ALLW"), 
        trim(col("VBAP.VOREF")).alias("CMPLT_REF_IN"), 
        trim(col("VBAP.UPFLU")).alias("UPDT_IN_DOC_FLOW_SLS_DOC"), 
        trim(col("VBAP.ERLRE")).alias("CMPLT_RULE_QUOT_CNTRC"), 
        when((col("VBAP.stdat") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("VBAP.stdat"), "yyyyMMdd"))\
          .alias("KEY_BILL_MATL_DTTM"), 
        trim(col("VBAP.STLNR")).alias("BILL_OF_MATL"), 
        trim(col("VBAP.STPOS")).alias("BILL_OF_MATL_ITM_NUM_VBAP_NOT_USED"), 
        trim(col("VBAP.TAXM4")).alias("TAX_CLSN_FOR_MATL_4"), 
        trim(col("VBAP.TAXM5")).alias("TAX_CLSN_FOR_MATL_5"), 
        trim(col("VBAP.TAXM6")).alias("TAX_CLSN_FOR_MATL_6"), 
        trim(col("VBAP.TAXM7")).alias("TAX_CLSN_FOR_MATL_7"), 
        trim(col("VBAP.TAXM8")).alias("TAX_CLSN_FOR_MATL_8"), 
        trim(col("VBAP.TAXM9")).alias("TAX_CLSN_FOR_MATL_9"), 
        col("VBAP.VBEAF").cast(DecimalType(18, 4)).alias("FX_SHIPPING_PRCSG_TIME_IN_DAYS"), 
        col("VBAP.VBEAV").cast(DecimalType(18, 4)).alias("VAR_SHIPPING_PRCSG_TIME_IN_DAYS"), 
        trim(col("VBAP.VGREF")).alias("PRCDNG_DOC_RSULT_REF"), 
        trim(col("VBAP.KONDM")).alias("MATL_PRC_GRP"), 
        trim(col("VBAP.BONUS")).alias("VOL_REBT_GRP"), 
        trim(col("VBAP.PROVG")).alias("COMMSN_GRP"), 
        trim(col("VBAP.EANNR")).alias("EUR_ARTCL_NUM_OBSOL"), 
        trim(col("VBAP.BWTAR")).alias("VALUT_TYPE"), 
        trim(col("VBAP.BWTEX")).alias("IN_SEP_VALUT"), 
        trim(col("VBAP.FIXMG")).alias("DELV_DT_QTY_FX"), 
        col("VBAP.KMPMG").cast(DecimalType(18, 4)).alias("CMPNT_QTY"), 
        trim(col("VBAP.SUGRD")).alias("RSN_MATL_SUBST"), 
        trim(col("VBAP.SOBKZ")).alias("SPL_STK_IN"), 
        trim(col("VBAP.VPZUO")).alias("ALLC_IN"), 
        trim(col("VBAP.PS_PSP_PNR")).alias("WRK_BRKDWN_STRC_ELMNT"), 
        trim(col("VBAP.VPMAT")).alias("PLNG_MATL"), 
        trim(col("VBAP.VPWRK")).alias("PLNG_PLNT"), 
        trim(col("VBAP.PRBME")).alias("BASE_UNIT_MEAS_PROD_GRP"), 
        col("VBAP.UMREF").cast(DecimalType(18, 4)).alias("CONV_FACT_QTY"), 
        trim(col("VBAP.KNTTP")).alias("ACCT_ASGNMT_CAT"), 
        trim(col("VBAP.KZVBR")).alias("CNSMPTN_PSTNG"), 
        trim(col("VBAP.SERNR")).alias("BOM_EXPLS_NUM"), 
        trim(col("VBAP.OBJNR")).alias("OBJ_NUM_ITM_LVL"), 
        trim(col("VBAP.ABGRS")).alias("RSLTS_ANAL_KEY"), 
        trim(col("VBAP.CMTFG")).alias("ID_PRTL_RLSE_ORDR_ITM_CR_BLK"), 
        trim(col("VBAP.CMPNT")).alias("ID_ITM_ACT_CR_FUNC_RLVNT_CR"), 
        col("VBAP.CMKUA").cast(DecimalType(18, 4)).alias("CR_DATA_EXCH_RT_FOR_RQST_DELV_DT"), 
        trim(col("VBAP.CUOBJ")).alias("CNFG"), 
        trim(col("VBAP.CUOBJ_CH")).alias("INTRNL_OBJ_NUM_BTCH_CLSN"), 
        trim(col("VBAP.CEPOK")).alias("STS_EXPTD_PRC"), 
        trim(col("VBAP.KOUPD")).alias("COND_UPDT"), 
        trim(col("VBAP.SERAIL")).alias("SER_NUM_PRFL"), 
        trim(col("VBAP.ANZSN")).alias("NUM_SER_NUM"), 
        trim(col("VBAP.NACHL")).alias("CUST_HAS_NOT_PSTD_GOODS_RCPT"), 
        trim(col("VBAP.MAGRV")).alias("MATL_GRP_PKGNG_MATL"), 
        trim(col("VBAP.MPROK")).alias("STS_MAN_PRC_CHG"), 
        trim(col("VBAP.VGTYP")).alias("DOC_CAT_PRCDNG_SD_DOC"), 
        trim(col("VBAP.PROSA")).alias("ID_MATL_DTRMN"), 
        trim(col("VBAP.UEPVW")).alias("ID_HI_LVL_ITM_USG"), 
        trim(col("VBAP.KALNR")).alias("COST_EST_NUM_COST_EST_QTY_STRC"), 
        trim(col("VBAP.KLVAR")).alias("COST_VRNT_KLVAR"), 
        trim(col("VBAP.SPOSN")).alias("BOM_ITM_NUM"), 
        trim(col("VBAP.KOWRR")).alias("STAT_VAL"), 
        when((col("VBAP.stadat") == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(col("VBAP.stadat"), "yyyyMMdd"))\
          .alias("STATS_DTTM"), 
        trim(col("VBAP.EXART")).alias("BUSN_TRX_TYPE"), 
        trim(col("VBAP.PREFE")).alias("CSTMS_PREF"), 
        trim(col("VBAP.KNUMH")).alias("NUM_COND_REC_FROM_BTCH_DTRMN"), 
        trim(col("VBAP.CLINT")).alias("INTRNL_CLS_NUM"), 
        trim(col("VBAP.CHMVS")).alias("BTCH_EXIT_TO_QTY_PROPS"), 
        trim(col("VBAP.STLTY")).alias("BOM_CAT"), 
        trim(col("VBAP.STLKN")).alias("BOM_ITM_NODE_NUM"), 
        trim(col("VBAP.STPOZ")).alias("INTRNL_CNTR_STPOZ"), 
        trim(col("VBAP.STMAN")).alias("INCONS_CNFG"), 
        trim(col("VBAP.ZSCHL_K")).alias("OVHD_KEY"), 
        trim(col("VBAP.KALSM_K")).alias("COST_SHT"), 
        trim(col("VBAP.KALVAR")).alias("COST_VRNT_KALVAR"), 
        trim(col("VBAP.KOSCH")).alias("PROD_ALLC_DTRMN_PCDR"), 
        trim(col("VBAP.UKONM")).alias("MATL_PRC_GRP_MN_ITM"), 
        trim(col("VBAP.MFRGR")).alias("MATL_FRGHT_GRP"), 
        trim(col("VBAP.PLAVO")).alias("INSTR_PLNG_DELV_SCHED"), 
        trim(col("VBAP.KANNR")).alias("KANBAN_SEQ_NUM"), 
        col("VBAP.CMPRE_FLT").cast(DecimalType(18, 4)).alias("ITM_CR_PRC_CMPRE_FLT"), 
        trim(col("VBAP.ABFOR")).alias("FORM_PMT_GUAR"), 
        col("VBAP.ABGES").cast(DecimalType(18, 4)).alias("GUARNTD"), 
        trim(col("VBAP.J_1BCFOP")).alias("CFOP_CD_EXTN"), 
        trim(col("VBAP.J_1BTAXLW1")).alias("TAX_LAW_ICMS"), 
        trim(col("VBAP.J_1BTAXLW2")).alias("TAX_LAW_IPI"), 
        trim(col("VBAP.J_1BTXSDC")).alias("SD_TAX_CD"), 
        trim(col("VBAP.WKTNR")).alias("VAL_CNTRC_NO"), 
        trim(col("VBAP.WKTPS")).alias("VAL_CNTRC_ITM"), 
        trim(col("VBAP.SKOPF")).alias("ASRTMNT_MDLE"), 
        trim(col("VBAP.KZBWS")).alias("VALUT_SPL_STK"), 
        trim(col("VBAP.WGRU1")).alias("MATL_GRP_HIER_1"), 
        trim(col("VBAP.WGRU2")).alias("MATL_GRP_HIER_2"), 
        trim(col("VBAP.KNUMA_PI")).alias("SLS_PROM"), 
        trim(col("VBAP.KNUMA_AG")).alias("SLS_DEAL"), 
        trim(col("VBAP.KZFME")).alias("ID_LEAD_UNIT_MEAS_CMPLT_TRX"), 
        trim(col("VBAP.LSTANR")).alias("FREE_GOODS_DELV_CNTL"), 
        trim(col("VBAP.TECHS")).alias("PARM_VRNT_STD_VRNT"), 
        col("VBAP.MWSBP").cast(DecimalType(18, 4)).alias("TAX_AMT_DOC_CRNCY"), 
        trim(col("VBAP.PCTRF")).alias("PRFT_CTR_BILL"), 
        trim(col("VBAP.LOGSYS_EXT")).alias("LOGL_SYS"), 
        trim(col("VBAP.STOCKLOC")).alias("FST_INV_MNG_LOC"), 
        trim(col("VBAP.SLOCTYPE")).alias("TYPE_FST_INV_MNG_LOC"), 
        trim(col("VBAP.MSR_RET_REASON")).alias("RTN_RSN"), 
        trim(col("VBAP.MSR_REFUND_CODE")).alias("RTRN_RFND_CD"), 
        trim(col("VBAP.MSR_APPROV_BLOCK")).alias("APPR_BLK"), 
        trim(col("VBAP.NRAB_KNUMH")).alias("NUM_COND_REC"), 
        trim(col("VBAP.TC_AUT_DET")).alias("TAX_CD_AUTMT_DTRMN"), 
        trim(col("VBAP.MANUAL_TC_REASON")).alias("MAN_TAX_CD_RSN"), 
        trim(col("VBAP.FISCAL_INCENTIVE")).alias("TAX_INCT_TYPE"), 
        trim(col("VBAP.TAX_SUBJECT_ST")).alias("TAX_SUBJ_SUBST_TRIB"), 
        trim(col("VBAP.AUFPL_OLC")).alias("RTG_NUM_OPS_ORDR_AUFPL_OLC"), 
        trim(col("VBAP.APLZL_OLC")).alias("INTRNL_CNTR_APLZL_OLC"), 
        trim(col("VBAP.FERC_IND")).alias("RGLT_IN"), 
        trim(col("VBAP.KOSTL")).alias("COST_CTR_KOSTL"), 
        trim(col("VBAP.FONDS")).alias("FUND"), 
        trim(col("VBAP.FISTL")).alias("FUND_CTR"), 
        trim(col("VBAP.FKBER")).alias("FUNC_AREA"), 
        trim(col("VBAP.GRANT_NBR")).alias("GRANT"), 
        trim(col("VBAP.IUID_RELEVANT")).alias("IUID_RLVNT_CUST"), 
        trim(col("VBAP.PRS_OBJNR")).alias("ENGAG_MGMT_OBJ_NUM"), 
        trim(col("VBAP.PRS_SD_SPSNR")).alias("STD_WBS_ELMNT_PROJ_INCPT_VIA_SD"), 
        trim(col("VBAP.PRS_WORK_PERIOD")).alias("WRK_PER"), 
        trim(col("VBAP.PARGB")).alias("TRAD_PTNR_BUSN_AREA"), 
        trim(col("VBAP.AUFPL_OAA")).alias("RTG_NUM_OPS_ORDR_AUFPL_OAA"), 
        trim(col("VBAP.APLZL_OAA")).alias("INTRNL_CNTR"), 
        when((trim(col("VBKD.prsdt")) == lit("00000000")), lit(None).cast(TimestampType()))\
          .otherwise(to_timestamp(trim(col("VBKD.prsdt")), "yyyyMMdd"))\
          .alias("PRC_AND_EXCH_RT_DTTM"), 
        lookup("LU_SAP_TVM4T", col("MVGR4")).getField("BEZEI").alias("MATL_GRP_4_DESC"), 
        col("VBAP._upt_").alias("_l0_upt_")
    )
