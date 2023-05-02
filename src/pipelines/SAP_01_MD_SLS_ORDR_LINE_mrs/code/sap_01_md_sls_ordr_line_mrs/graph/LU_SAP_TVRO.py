from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_mrs.config.ConfigStore import *
from sap_01_md_sls_ordr_line_mrs.udfs.UDFs import *

def LU_SAP_TVRO(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''ROUTE''']
    valueColumns = ['''SPFBK''', '''TDVZTD''', '''TRAZTD''']
    createLookup("LU_SAP_TVRO", in0, spark, keyColumns, valueColumns)
