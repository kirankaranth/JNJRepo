from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_03_md_cust_hmd_hm2.config.ConfigStore import *
from sap_03_md_cust_hmd_hm2.udfs.UDFs import *

def LU_SAP_T016T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''BRSCH''']
    valueColumns = ['''BRTXT''']
    createLookup("LU_SAP_T016T", in0, spark, keyColumns, valueColumns)
