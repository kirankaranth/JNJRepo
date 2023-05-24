from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_hmd_hm2.config.ConfigStore import *
from sap_01_md_cust_hmd_hm2.udfs.UDFs import *

def LU_SAP_TBRCT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''BRACO''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TBRCT", in0, spark, keyColumns, valueColumns)
