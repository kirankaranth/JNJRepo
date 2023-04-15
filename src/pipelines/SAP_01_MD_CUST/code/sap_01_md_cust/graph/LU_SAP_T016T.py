from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust.config.ConfigStore import *
from sap_01_md_cust.udfs.UDFs import *

def LU_SAP_T016T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''brsch''']
    valueColumns = ['''brtxt''']
    createLookup("LU_SAP_T016T", in0, spark, keyColumns, valueColumns)
