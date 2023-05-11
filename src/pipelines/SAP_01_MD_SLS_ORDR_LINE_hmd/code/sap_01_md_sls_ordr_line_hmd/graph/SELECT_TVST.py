from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_hmd.config.ConfigStore import *
from sap_01_md_sls_ordr_line_hmd.udfs.UDFs import *

def SELECT_TVST(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("VSTEL"), 
        col("LOADTN"), 
        col("PIPATN"), 
        col("TSTRID"), 
        col("FABKL"), 
        col("LOADTG"), 
        col("PIPATG"), 
        col("ALAND"), 
        col("RIZBS")
    )
