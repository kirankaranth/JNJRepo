from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bwi.config.ConfigStore import *
from sap_01_md_sls_ordr_bwi.udfs.UDFs import *

def LU_SAP_TVFST(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''FAKSP''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVFST", in0, spark, keyColumns, valueColumns)
