from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr.config.ConfigStore import *
from sap_01_md_bill_doc_hdr.udfs.UDFs import *

def LU_SAP_TVFKT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''FKART''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVFKT", in0, spark, keyColumns, valueColumns)
