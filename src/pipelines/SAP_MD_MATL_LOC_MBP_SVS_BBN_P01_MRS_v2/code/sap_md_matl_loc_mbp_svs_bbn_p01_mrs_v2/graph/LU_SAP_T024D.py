from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs_v2.udfs.UDFs import *

def LU_SAP_T024D(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''WERKS''', '''DISPO''']
    valueColumns = ['''DSNAM''']
    createLookup("LU_SAP_T024D", in0, spark, keyColumns, valueColumns)
