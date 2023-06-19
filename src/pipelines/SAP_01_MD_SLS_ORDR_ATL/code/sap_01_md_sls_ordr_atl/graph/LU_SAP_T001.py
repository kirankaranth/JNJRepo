from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_atl.config.ConfigStore import *
from sap_01_md_sls_ordr_atl.udfs.UDFs import *

def LU_SAP_T001(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''BUKRS''']
    valueColumns = ['''BUTXT''']
    createLookup("LU_SAP_T001", in0, spark, keyColumns, valueColumns)
