from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bbl.config.ConfigStore import *
from sap_01_md_sls_ordr_bbl.udfs.UDFs import *

def LU_SAP_TVAUT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''AUGRU''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVAUT", in0, spark, keyColumns, valueColumns)
