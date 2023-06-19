from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_bwi.config.ConfigStore import *
from sap_md_matl_loc_bwi.udfs.UDFs import *

def LU_SAP_T024(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''EKGRP''']
    valueColumns = ['''EKNAM''']
    createLookup("LU_SAP_T024", in0, spark, keyColumns, valueColumns)
