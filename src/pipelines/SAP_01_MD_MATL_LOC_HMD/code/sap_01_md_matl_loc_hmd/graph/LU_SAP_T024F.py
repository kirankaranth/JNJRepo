from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_01_md_matl_loc_hmd.config.ConfigStore import *
from sap_01_md_matl_loc_hmd.udfs.UDFs import *

def LU_SAP_T024F(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''WERKS''', '''FEVOR''']
    valueColumns = ['''TXT''']
    createLookup("LU_SAP_T024F", in0, spark, keyColumns, valueColumns)
