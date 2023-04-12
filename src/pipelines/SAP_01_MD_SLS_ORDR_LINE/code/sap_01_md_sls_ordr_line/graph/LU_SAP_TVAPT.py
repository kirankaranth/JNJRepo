from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line.config.ConfigStore import *
from sap_01_md_sls_ordr_line.udfs.UDFs import *

def LU_SAP_TVAPT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''PSTYV''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVAPT", in0, spark, keyColumns, valueColumns)
