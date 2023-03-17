from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def SAP_MARC(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bbl.marc WHERE _deleted_ = 'F'")
