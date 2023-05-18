from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_hmd_hm2.config.ConfigStore import *
from sap_01_md_cust_hmd_hm2.udfs.UDFs import *

def LU_SAP_T077X(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''KTOKD''']
    valueColumns = ['''TXT30''']
    createLookup("LU_SAP_T077X", in0, spark, keyColumns, valueColumns)
