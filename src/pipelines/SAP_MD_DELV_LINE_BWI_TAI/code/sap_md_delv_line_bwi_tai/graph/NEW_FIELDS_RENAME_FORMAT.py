from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bwi_tai.config.ConfigStore import *
from sap_md_delv_line_bwi_tai.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("DELV_NUM", col("VBELN"))\
        .withColumn("DELV_LINE_NBR", col("POSNR"))\
        .withColumn("DELV_TYP_CD", col("LFART"))\
        .withColumn("CO_CD", col("BUKRS_VF"))\
        .withColumn("MATL_SHRT_DESC", trim(col("ARKTX")))\
        .withColumn("MATL_MVMT_TYPE_CD", trim(col("BWART")))\
        .withColumn("DOC_REF_NUM", col("VGBEL"))\
        .withColumn("PICK_CNTL_STS_CD", trim(col("KOMKZ")))\
        .withColumn("LINE_ITEM_CAT_CD", trim(col("PSTYV")))\
        .withColumn("SPL_STK_TYPE_CD", trim(col("SOBKZ")))\
        .withColumn("PARNT_BTCH_SPLT_CNTR_NBR", trim(col("UECHA")).cast(IntegerType()))\
        .withColumn("PARNT_BOM_CNTR_NBR", trim(col("UEPOS")).cast(IntegerType()))\
        .withColumn("FCTR_NMRTR_MEAS", trim(col("UMVKZ")).cast(DecimalType(18, 4)))\
        .withColumn("FCTR_DNMNTR_MEAS", trim(col("UMVKN")).cast(DecimalType(18, 4)))\
        .withColumn(
          "ACTL_GI_DTTM",
          when((col("WADAT_IST") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("WADAT_IST"), "yyyyMMdd"))
        )\
        .withColumn("ACTL_SLS_UNIT_DELV_QTY", trim(col("LFIMG")).cast(DecimalType(18, 4)))\
        .withColumn("ACTL_SKU_DELV_QTY", trim(col("LGMNG")).cast(DecimalType(18, 4)))\
        .withColumn("BASE_UOM_CD", trim(col("MEINS")))\
        .withColumn("BTCH_NUM", trim(col("CHARG")))\
        .withColumn("BILL_ICMPT_STS_CD", trim(col("UVFAK")))\
        .withColumn("CNFRM_STS_CD", trim(col("BESTA")))\
        .withColumn(
          "MFG_DTTM",
          when((col("HSDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("HSDAT"), "yyyyMMdd"))
        )\
        .withColumn("DELV_BILL_STS_CD", trim(col("FKSTA")))\
        .withColumn("DELV_CMPLT_IND", trim(col("SPE_GEN_ELIKZ")))\
        .withColumn("DELV_ICMPT_STS_CD", trim(col("UVVLK")))\
        .withColumn("DELV_STS_CD", trim(col("LFSTA")))\
        .withColumn("DELV_TOT_STS_CD", trim(col("LFGSA")))\
        .withColumn(
          "EXP_DTTM",
          when((col("VFDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("VFDAT"), "yyyyMMdd"))
        )\
        .withColumn("GM_ICMPT_STS_CD", trim(col("UVWAK")))\
        .withColumn("GM_STS_CD", trim(col("WBSTA")))\
        .withColumn("INTCO_BILL_STS_CD", trim(col("FKIVP")))\
        .withColumn("MATL_NUM", trim(col("MATNR")))\
        .withColumn("ICMPT_STS_CD", trim(col("UVALL")))\
        .withColumn("NUM_OF_CNTNRS_CNT", trim(col("ZZCONTAINER")))\
        .withColumn("ORDR_BILL_STS_CD", trim(col("FKSAA")))\
        .withColumn("PACK_ICMPT_STS_CD", trim(col("UVPAK")))\
        .withColumn("PACK_STS_CD", trim(col("PKSTA")))\
        .withColumn("PICK_CNFRM_STS_CD", trim(col("KOQUA")))\
        .withColumn("PICK_ICMPT_STS_CD", trim(col("UVPIK")))\
        .withColumn("PICK_STS_CD", trim(col("KOSTA")))\
        .withColumn("PRC_ICMPT_STS_CD", trim(col("UVPRS")))\
        .withColumn("PRCSG_TOT_STS_CD", trim(col("GBSTA")))\
        .withColumn("RECV_PLNT_CD", trim(col("UMWRK")))\
        .withColumn("REF_STS_CD", trim(col("RFSTA")))\
        .withColumn("REF_TOT_STS_CD", trim(col("RFGSA")))\
        .withColumn("REJ_STS_CD", trim(col("ABSTA")))\
        .withColumn("SLS_ORDR_LINE_NBR_REF", trim(col("VGPOS")))\
        .withColumn("SLS_UOM_CD", trim(col("VRKME")))\
        .withColumn("SHIPPING_PLNT_CD", trim(col("WERKS")))\
        .withColumn("SLOC_CD", trim(col("LGORT")))\
        .withColumn("VEND_BTCH_NUM", trim(col("LICHN")))\
        .withColumn("WM_STS_CD", trim(col("LVSTA")))\
        .withColumn(
          "CRT_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("ERDAT"), col("ERZET")), "yyyyMMddHHmmss"))
        )\
        .withColumn("ITM_TYPE", trim(col("POSAR")))\
        .withColumn("ITM_OF_REF_DOC", trim(col("LFPOS")))\
        .withColumn("GRS_WT_MEAS", trim(col("BRGEW")).cast(DecimalType(18, 4)))\
        .withColumn("NET_WT_MEAS", trim(col("NTGEW")).cast(DecimalType(18, 4)))\
        .withColumn("WT_UOM_CD", trim(col("GEWEI")))\
        .withColumn("VOL_MEAS", trim(col("VOLUM")).cast(DecimalType(18, 4)))\
        .withColumn("VOL_UOM_CD", trim(col("VOLEH")))\
        .withColumn("NET_VAL_AMT", trim(col("NETWR")).cast(DecimalType(18, 4)))\
        .withColumn("PROD_HIER_CD", trim(col("PRODH")))\
        .withColumn("MATL_GRP_4", trim(col("MVGR4")))\
        .withColumn("DSTR_CHNL_CD", trim(col("VTWEG")))\
        .withColumn("ITM_BILL_BLK_STS_CD", trim(col("FSSTA")))\
        .withColumn("ITM_OVRL_DELV_BLK_STS_CD", trim(col("LSSTA")))\
        .withColumn("ENT_MATL_NUM", trim(col("MATWA")))\
        .withColumn("DIVISION_CD", trim(col("SPART")))\
        .withColumn("DOC_CAT_SD", trim(col("VGTYP")))\
        .withColumn("MATL_GRP_CD", trim(col("MATKL")))\
        .withColumn("MATL_TYPE_CD", trim(col("MTART")))\
        .withColumn("CUM_BTCH_QTY", trim(col("KCMENG")).cast(DecimalType(18, 4)))\
        .withColumn(
          "ORDR_SFX",
          lit(
            "#"
          )
        )\
        .withColumn(
          "MATCH_TYPE",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SRC_TBL_NM",
          lit(
            "#"
          )
        )\
        .withColumn("ORIG_QTY_DELV_ITM", trim(col("ORMNG")).cast(DecimalType(18, 4)))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'DELV_NUM', DELV_NUM, 'DELV_LINE_NBR', DELV_LINE_NBR, 'DELV_TYP_CD', DELV_TYP_CD, 'CO_CD', CO_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'ORDR_SFX', ORDR_SFX, 'MATCH_TYPE', MATCH_TYPE, 'SRC_TBL_NM', SRC_TBL_NM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'DELV_NUM', DELV_NUM, 'DELV_LINE_NBR', DELV_LINE_NBR, 'DELV_TYP_CD', DELV_TYP_CD, 'CO_CD', CO_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'ORDR_SFX', ORDR_SFX, 'MATCH_TYPE', MATCH_TYPE, 'SRC_TBL_NM', SRC_TBL_NM)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
