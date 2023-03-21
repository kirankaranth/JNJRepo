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
        .withColumn("SOLD_TO", trim(col("KUNAG")))
