from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn("BILL_DOC", col("VBELN")).withColumn("SLORG_CD", col("VKORG"))
