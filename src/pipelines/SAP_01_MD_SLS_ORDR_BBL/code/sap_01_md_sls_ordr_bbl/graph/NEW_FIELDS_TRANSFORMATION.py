from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bbl.config.ConfigStore import *
from sap_01_md_sls_ordr_bbl.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("COMPANY_CD", trim(col("BUKRS_VF")))\
        .withColumn("SLS_ORDR_DOC_ID", trim(col("VBELN")))\
        .withColumn("SLS_ORDR_TYPE_CD", trim(col("AUART")))\
        .withColumn(
          "CRT_DTTM",
          when(((col("ERDAT") == lit("00000000")) | (col("ERZET") == lit("00000000"))), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(concat(col("ERDAT"), col("ERZET")), "yyyyMMddHHmmss"))
        )\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'COMPANY_CD', COMPANY_CD, 'SLS_ORDR_DOC_ID', SLS_ORDR_DOC_ID, 'SLS_ORDR_TYPE_CD', SLS_ORDR_TYPE_CD)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("SLS_ORDR_NUM_REF", trim(col("VGBEL")))\
        .withColumn("CRT_BY", trim(col("ERNAM")))\
        .withColumn("BILL_BLK_CD", trim(col("FAKSK")))\
        .withColumn("DELV_BLK_CD", trim(col("LIFSK")))\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn("CUST_PO_TYPE_CD", trim(col("BSARK")))\
        .withColumn(
          "VALID_FROM_DTTM",
          when((col("GUEBG") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("GUEBG"), "yyyyMMdd"))
        )\
        .withColumn(
          "VALID_TO_DTTM",
          when((col("GUEEN") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("GUEEN"), "yyyyMMdd"))
        )\
        .withColumn(
          "RQST_DELV_DTTM",
          when((col("VDATU") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("VDATU"), "yyyyMMdd"))
        )\
        .withColumn("PRC_PCDR_CD", trim(col("KALSM")))\
        .withColumn("SLS_ORDR_CRNCY_CD", trim(col("WAERK")))\
        .withColumn("NET_VAL_AMT", trim(col("NETWR")).cast(DecimalType(18, 4)))\
        .withColumn("SOLD_TO_CUST_NUM", trim(col("KUNNR")))\
        .withColumn("SALES_ORGANIZATION_CD", trim(col("VKORG")))\
        .withColumn("DSTR_CHNL_CD", trim(col("VTWEG")))\
        .withColumn("DIVISION_CD", trim(col("SPART")))\
        .withColumn("SHIPPING_COND_CD", trim(col("VSBED")))\
        .withColumn("CUST_PO_NUM", trim(col("BSTNK")))\
        .withColumn("PTNR_REF_CD", trim(col("IHREZ")))\
        .withColumn("SLS_ORDR_CAT_CD", trim(col("VBTYP")))\
        .withColumn("SALES_ORDER_REASON_CD", trim(col("AUGRU")))\
        .withColumn("SLS_TERR_ID", trim(col("VKBUR")))\
        .withColumn("SLS_GRP_CD", trim(col("VKGRP")))\
        .withColumn(
          "ORIG_MATL_AVLBLTY_DTTM",
          when((col("FMBDAT") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("FMBDAT"), "yyyyMMdd"))
        )\
        .withColumn("CMPLT_DELV_CD", trim(col("AUTLF")))\
        .withColumn(
          "RLSE_DTTM",
          when((col("CMFRE") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("CMFRE"), "yyyyMMdd"))
        )\
        .withColumn("SRCH_ITM_PROD_PROPS", trim(col("KTEXT")))\
        .withColumn(
          "DOC_DTTM",
          when((col("AUDAT") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("AUDAT"), "yyyyMMdd"))
        )\
        .withColumn("BUSN_AREA", trim(col("GSBER")))\
        .withColumn("BUSN_AREA_CC", trim(col("GSKST")))\
        .withColumn("SD_DOC_IN", trim(col("VBKLT")))\
        .withColumn("ORDR_RLTD_BILL_TYPE", trim(col("FKARA")))\
        .withColumn("CC_CD", trim(col("KOSTL")))\
        .withColumn("UPDT_GRP", trim(col("STAFO")))\
        .withColumn("CUST_GRP_1", trim(col("KVGR1")))\
        .withColumn("CUST_GRP_2", trim(col("KVGR2")))\
        .withColumn("CUST_GRP_3", trim(col("KVGR3")))\
        .withColumn("CUST_GRP_4", trim(col("KVGR4")))\
        .withColumn("CUST_GRP_5", trim(col("KVGR5")))\
        .withColumn("CNTL_AREA", trim(col("KOKRS")))\
        .withColumn("EXCH_RT_TYPE", trim(col("KURST")))\
        .withColumn("CR_CNTL_AREA", trim(col("KKBER")))\
        .withColumn("CR_MGMT_RISK_CAT", trim(col("CTLPC")))\
        .withColumn("ALT_TAX_CLSN", trim(col("TAXK1")))\
        .withColumn("TAX_CLSN_2", trim(col("TAXK2")))\
        .withColumn("TAX_CLSN_3", trim(col("TAXK3")))\
        .withColumn("TAX_CLSN_4", trim(col("TAXK4")))\
        .withColumn("TAX_CLSN_5", trim(col("TAXK5")))\
        .withColumn("TAX_CLSN_6", trim(col("TAXK6")))\
        .withColumn("TAX_CLSN_7", trim(col("TAXK7")))\
        .withColumn("TAX_CLSN_8", trim(col("TAXK8")))\
        .withColumn("TAX_CLSN_9", trim(col("TAXK9")))\
        .withColumn("REF_DOC_NUM", trim(col("XBLNR")))\
        .withColumn("ORDR_NUM", trim(col("AUFNR")))\
        .withColumn("TAX_DEST_CTRY", trim(col("STCEG_L")))\
        .withColumn("TAX_DPRT_CTRY", trim(col("LANDTX")))\
        .withColumn("IN_TRNGLR_DEAL_EU", trim(col("XEGDR")))\
        .withColumn("CLS_OF_TRD", expr(Config.CLS_OF_TRD))\
        .withColumn("FORBID_SLS_IN", expr(Config.FORBID_SLS_IN))\
        .withColumn("SLS_ORDR_TYPE_DESC", trim(lookup("LU_SAP_TVAKT", col("AUART")).getField("BEZEI")))\
        .withColumn("BILL_BLK_DESC", trim(lookup("LU_SAP_TVFST", col("FAKSK")).getField("VTEXT")))\
        .withColumn("DELV_BLK_DESC", trim(lookup("LU_SAP_TVLST", col("LIFSK")).getField("VTEXT")))\
        .withColumn("SLORG_NM", trim(lookup("LU_SAP_TVKOT", col("VKORG")).getField("VTEXT")))\
        .withColumn("DSTR_CHNL_NM", trim(lookup("LU_SAP_TVTWT", col("VTWEG")).getField("VTEXT")))\
        .withColumn("DIV_NM", trim(lookup("LU_SAP_TSPAT", col("SPART")).getField("VTEXT")))\
        .withColumn("SLS_ORDR_RSN_DESC", trim(lookup("LU_SAP_TVAUT", col("AUGRU")).getField("BEZEI")))\
        .withColumn("PO_TYPE_DESC", trim(lookup("LU_SAP_T176T", col("BSARK")).getField("VTEXT")))\
        .withColumn("RETRO_BILL", trim(lookup("LU_SAP_TVAU", col("AUGRU")).getField("VAUNA")))\
        .withColumn("CUST_GRP_1_DESC", lit(None).cast(StringType()))\
        .withColumn("CUST_GRP_2_DESC", lit(None).cast(StringType()))\
        .withColumn("CUST_GRP_3_DESC", lit(None).cast(StringType()))\
        .withColumn("CUST_GRP_4_DESC", trim(lookup("LU_SAP_TVV4T", col("KVGR4")).getField("BEZEI")))\
        .withColumn("CUST_GRP_5_DESC", lit(None).cast(StringType()))\
        .withColumn("CO_CD_DESC", trim(lookup("LU_SAP_T001", col("BUKRS_VF")).getField("BUTXT")))\
        .withColumn("SHIPPING_COND_DESC", trim(lookup("LU_SAP_TVSBT", col("VSBED")).getField("VTEXT")))
