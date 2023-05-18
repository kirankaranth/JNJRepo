from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bbl.config.ConfigStore import *
from sap_01_md_sls_ordr_bbl.udfs.UDFs import *

def LU_SAP_TSPAT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''SPART''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TSPAT", in0, spark, keyColumns, valueColumns)
