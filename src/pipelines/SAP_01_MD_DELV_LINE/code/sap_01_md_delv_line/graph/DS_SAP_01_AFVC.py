from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_delv_line.config.ConfigStore import *
from sap_01_md_delv_line.udfs.UDFs import *

def DS_SAP_01_AFVC(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bbl.afvc WHERE _deleted_ = 'F'")
