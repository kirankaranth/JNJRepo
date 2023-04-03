from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_doc_ptnr_func.config.ConfigStore import *
from sap_01_md_sls_doc_ptnr_func.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SLS_DOC_ITEM_NBR", col("POSNR"))\
        .withColumn("SLS_DOC_ID", col("VBELN"))\
        .withColumn("PTNR_FUNC_CD", col("PARVW"))\
        .withColumn("COMPANY_CD", col("BUKRS_VF"))\
        .withColumn("SLS_ORDR_TYPE_CD", col("AUART"))\
        .withColumn("FURTHER_PTNR_IND", trim(col("PARVW_FF")))\
        .withColumn("SUP_NUM", trim(col("LIFNR")))\
        .withColumn("ADDR_USG_CD", trim(col("ADRNR")))\
        .withColumn("CUST_NUM", trim(col("KUNNR")))\
        .withColumn("CTRY_CD", trim(col("LAND1")))\
        .withColumn("PERS_NUM", trim(col("PERNR")))\
        .withColumn("NUM_CNTCT_PRSN", trim(col("PARNR")))\
        .withColumn("UNLD_PT", trim(col("ABLAD")))\
        .withColumn("ADDR_IN", trim(col("ADRDA")))\
        .withColumn("IN_ACCT_ONE_TM_ACCT", trim(col("XCPDK")))\
        .withColumn("CUST_HIER_TYPE", trim(col("HITYP")))\
        .withColumn("RLVNT_PRC_DTRMN_ID", trim(col("PRFRE")))
