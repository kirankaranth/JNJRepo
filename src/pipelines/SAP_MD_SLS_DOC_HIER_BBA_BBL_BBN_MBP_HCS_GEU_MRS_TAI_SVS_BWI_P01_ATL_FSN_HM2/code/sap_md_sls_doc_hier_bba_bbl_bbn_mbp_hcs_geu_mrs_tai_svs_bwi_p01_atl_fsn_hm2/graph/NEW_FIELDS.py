from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("CO_CD", col("BUKRS_VF"))\
        .withColumn("PREV_DOC_NUM", col("VBELV"))\
        .withColumn("PREV_DOC_LINE_NBR", col("POSNV"))\
        .withColumn("SUBSQ_DOC_NUM", col("VBELN"))\
        .withColumn("SUBSQ_DOC_LINE_NBR", col("POSNN"))\
        .withColumn("SUBSQ_DOC_CAT_CD", col("VBTYP_N"))\
        .withColumn("PREV_DOC_TYPE_CD", col("AUART"))\
        .withColumn("PREV_DOC_CAT_CD", col("VBTYP_V"))\
        .withColumn("REF_QTY", col("RFMNG").cast(DecimalType(18, 4)))\
        .withColumn("BASE_UOM_CD", trim(col("MEINS")))\
        .withColumn("REF_AMT", trim(col("RFWRT")))\
        .withColumn("CRNCY_CD", trim(col("WAERS")))\
        .withColumn(
          "CRT_DTTM",
          when(
              (
                ((col("ERDAT") == lit("00000000")) | (col("ERDAT") < lit("19000101")))
                | ((length(regexp_replace(col("ERDAT"), "(\\d+)", "")) > lit(0)) | (length(col("ERDAT")) < lit(8)))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(concat(col("ERDAT"), col("ERZET")), "yyyyMMddHHmmss"))
        )\
        .withColumn("MATL_NUM", trim(col("MATNR")))\
        .withColumn(
          "CHG_DTTM",
          when(
              (
                ((col("AEDAT") == lit("00000000")) | (col("AEDAT") < lit("19000101")))
                | ((length(regexp_replace(col("AEDAT"), "(\\d+)", "")) > lit(0)) | (length(col("AEDAT")) < lit(8)))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn("REQ_TYPE_CD", trim(col("BDART")))\
        .withColumn("PLNG_TYPE_CD", trim(col("PLART")))\
        .withColumn("LVL_CD", trim(col("STUFE")))\
        .withColumn("WHSE_CD", trim(col("LGNUM")))\
        .withColumn("BILL_CAT_CD", trim(col("FKTYP")))\
        .withColumn("GRS_WT_MEAS", col("BRGEW").cast(DecimalType(18, 4)))\
        .withColumn("WT_UOM_CD", trim(col("GEWEI")))\
        .withColumn("VOL_MEAS", col("VOLUM").cast(DecimalType(18, 4)))\
        .withColumn("VOL_UOM_CD", trim(col("VOLEH")))\
        .withColumn("SLS_UOM_CD", trim(col("VRKME")))\
        .withColumn("SPL_STK_TYPE_CD", trim(col("SOBKZ")))\
        .withColumn("SPL_STK_NUM", trim(col("SONUM")))\
        .withColumn("NET_WT_MEAS", col("NTGEW").cast(DecimalType(18, 4)))\
        .withColumn("GM_STS_CD", trim(col("WBSTA")))
