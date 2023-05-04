from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def Lookup_01_T006A(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MSEHI''']
    valueColumns = ['''MSEH3''']
    createLookup("LU_T006A_MSEH3", in0, spark, keyColumns, valueColumns)
