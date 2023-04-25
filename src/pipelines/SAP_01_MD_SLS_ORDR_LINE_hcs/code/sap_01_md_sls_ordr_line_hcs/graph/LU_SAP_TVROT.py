from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_hcs.config.ConfigStore import *
from sap_01_md_sls_ordr_line_hcs.udfs.UDFs import *

def LU_SAP_TVROT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''ROUTE''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVROT", in0, spark, keyColumns, valueColumns)
