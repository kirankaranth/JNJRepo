from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc_hmd.config.ConfigStore import *
from sap_01_md_matl_loc_hmd.udfs.UDFs import *

def LU_SAP_T024(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''EKGRP''']
    valueColumns = ['''EKNAM''']
    createLookup("LU_SAP_T024", in0, spark, keyColumns, valueColumns)
