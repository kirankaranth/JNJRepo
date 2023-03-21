from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line.config.ConfigStore import *
from sap_01_md_sls_ordr_line.udfs.UDFs import *

def SAP_TVAGT(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bbl.TVAGT WHERE _deleted_ = 'F'")
