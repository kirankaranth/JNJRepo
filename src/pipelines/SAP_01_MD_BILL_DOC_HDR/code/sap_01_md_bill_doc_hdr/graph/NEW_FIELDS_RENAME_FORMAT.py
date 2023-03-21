from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("BILL_DOC", col("VBELN"))\
        .withColumn("SLORG_CD", trim(col("VKORG")))\
        .withColumn("SLORG_DESC", trim(col("TVKOT_VTEXT")))\
        .withColumn("DSTR_CHNL_CD", trim(col("VTWEG")))\
        .withColumn("DSTR_CHNL_DESC", trim(col("TVTWT_VTEXT")))\
        .withColumn("SLS_DIV_CD", trim(col("SPART")))\
        .withColumn("SLS_DIV_DESC", trim(col("TSPAT_VTEXT")))\
        .withColumn("BILL_TYPE_CD", trim(col("FKART")))\
        .withColumn("BILL_TYPE_DESC", trim(col("TVFKT_VTEXT")))\
        .withColumn("BILL_CAT", trim(col("FKTYP")))\
        .withColumn("DOC_CAT", trim(col("VBTYP")))\
        .withColumn("PYR", trim(col("KUNRG")))\
        .withColumn("SOLD_TO", trim(col("KUNAG")))\
        .withColumn("SHIP_TO", trim(col("KUNWE")))\
        .withColumn(
          "CRT_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None))\
            .otherwise(to_timestamp(concat(col("ERDAT"), col("ERZET")), "yyyyMMddHHmmss"))
        )\
        .withColumn(
          "BILL_DTTM",
          when((col("FKDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("FKDAT"), "yyyyMMdd"))
        )\
        .withColumn(
          "BILL_INVC_DTTM",
          when((col("FKDAT_RL") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("FKDAT_RL"), "yyyyMMdd"))
        )\
        .withColumn("CRNCY_CD", trim(col("WAERK")))\
        .withColumn("CRT_BY", trim(col("ERNAM")))\
        .withColumn("PRC_PCDR_CD", trim(col("KALSM")))\
        .withColumn("DOC_COND_OWN_COND", trim(col("KNUMV")))\
        .withColumn("SHIPPING_COND_CD", trim(col("VSBED")))\
        .withColumn("FISC_YR", trim(col("GJAHR")))\
        .withColumn("PSTNG_PER", trim(col("POPER")))\
        .withColumn("CUST_GRP_CD", trim(col("KDGRP")))\
        .withColumn("INTNL_COM_CD", trim(col("INCO1")))\
        .withColumn("DEL_DPRT_PT_CD", trim(col("INCO2")))\
        .withColumn("PSTNG_STS", trim(col("RFBSK")))\
        .withColumn("EXCH_RT_FIN_PSTNG", trim(col("KURRF")))\
        .withColumn("ADDL_VAL_DAYS", trim(col("VALTG")))\
        .withColumn(
          "FX_VAL_DTTM",
          when((col("VALDT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("VALDT"), "yyyyMMdd"))
        )\
        .withColumn("PMT_TERM_CD", trim(col("ZTERM")))\
        .withColumn("ACCT_ASGNMT_GRP", trim(col("KTGRD")))\
        .withColumn("CTRY_CD", trim(col("LAND1")))\
        .withColumn("REGION_CD", trim(col("REGIO")))\
        .withColumn("CO_CD", trim(col("BUKRS")))\
        .withColumn("TAX_CLSN_1", trim(col("TAXK1")))\
        .withColumn("NET_VAL_AMT", trim(col("NETWR")))\
        .withColumn("COMB_CRITA", trim(col("ZUKRI")))\
        .withColumn("STATS_CRNCY", trim(col("STWAE")))\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn("INVC_LIST_TYPE", trim(col("FKART_RL")))\
        .withColumn("CNTL_AREA_CD", trim(col("KKBER")))\
        .withColumn("CR_ACCT", trim(col("KNKLI")))\
        .withColumn("CRNCY_CR_CNTL_AREA", trim(col("CMWAE")))\
        .withColumn("CR_DX_RT", trim(col("CMKUF")))\
        .withColumn("HIER_TYPE_PRC", trim(col("HITYP_PR")))\
        .withColumn("CUST_PO_NUM", trim(col("BSTNK_VF")))\
        .withColumn("TRAD_PTNR_CO_CD", trim(col("VBUND")))\
        .withColumn("TAX_DPRT_CTRY", trim(col("LANDTX")))\
        .withColumn("ORIG_VAT_NUM", trim(col("STCEG_H")))\
        .withColumn("CTRY_VAT_NUM", trim(col("STCEG_L")))\
        .withColumn("REF_DOC_NUM", trim(col("XBLNR")))\
        .withColumn("ASGNMT_NUM", trim(col("ZUONR")))\
        .withColumn("TAX_AMT", trim(col("MWSBK")))\
        .withColumn("LOGL_SYS", trim(col("LOGSYS")))\
        .withColumn(
          "TRNL_DTTM",
          when((col("KURRF_DAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("KURRF_DAT"), "yyyyMMdd"))
        )\
        .withColumn("PMT_REF", trim(col("KIDNO")))\
        .withColumn("NUM_OF_PG", trim(col("NUMPG")))\
        .withColumn("PSTNG_BILL_STS_CD", trim(col("BUCHK")))\
        .withColumn("INVC_LIST_STS_CD", trim(col("RELIK")))\
        .withColumn("CUST_PRC_GRP", trim(col("KONDA")))\
        .withColumn("SLS_DSTRC", trim(col("BZIRK")))\
        .withColumn("PRC_LIST_TYPE", trim(col("PLTYP")))\
        .withColumn("TAX_CLSN_2", trim(col("TAXK2")))\
        .withColumn("TAX_CLSN_3", trim(col("TAXK3")))\
        .withColumn("TAX_CLSN_4", trim(col("TAXK4")))\
        .withColumn("TAX_CLSN_5", trim(col("TAXK5")))\
        .withColumn("TAX_CLSN_6", trim(col("TAXK6")))\
        .withColumn("TAX_CLSN_7", trim(col("TAXK7")))\
        .withColumn("TAX_CLSN_8", trim(col("TAXK8")))\
        .withColumn("TAX_CLSN_9", trim(col("TAXK9")))\
        .withColumn("INDSTR_CD_1", lit(None).cast(StringType()))\
        .withColumn("INDSTR_CD_2", lit(None).cast(StringType()))\
        .withColumn("PRCH_ORDR_TYPE", lit(None).cast(StringType()))\
        .withColumn("BILL_DOC_IS_CAN", trim(col("FKSTO")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", to_timestamp(current_timestamp(), "yyyy-MM-dd"))\
        .withColumn("DAI_UPDT_DTTM", to_timestamp(current_timestamp(), "yyyy-MM-dd"))
