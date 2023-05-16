from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_bwi.config.ConfigStore import *
from sap_01_md_sls_ordr_line_bwi.udfs.UDFs import *

def LU_SAP_TVAPT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''PSTYV''']
    valueColumns = ['''VTEXT''', '''PSTYV''']
    createLookup("LU_SAP_TVAPT", in0, spark, keyColumns, valueColumns)
