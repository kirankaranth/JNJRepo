from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *

def LU_SAP_TVFKT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''FKART''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVFKT", in0, spark, keyColumns, valueColumns)
