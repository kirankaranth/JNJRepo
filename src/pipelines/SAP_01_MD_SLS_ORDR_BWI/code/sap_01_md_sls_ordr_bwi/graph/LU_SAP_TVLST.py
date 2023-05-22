from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bwi.config.ConfigStore import *
from sap_01_md_sls_ordr_bwi.udfs.UDFs import *

def LU_SAP_TVLST(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''LIFSP''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVLST", in0, spark, keyColumns, valueColumns)
