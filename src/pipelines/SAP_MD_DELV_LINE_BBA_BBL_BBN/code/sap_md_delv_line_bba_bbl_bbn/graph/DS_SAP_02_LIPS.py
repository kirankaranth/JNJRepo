from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bba_bbl_bbn.config.ConfigStore import *
from sap_md_delv_line_bba_bbl_bbn.udfs.UDFs import *

def DS_SAP_02_LIPS(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bba.lips WHERE _deleted_ = 'F'")
