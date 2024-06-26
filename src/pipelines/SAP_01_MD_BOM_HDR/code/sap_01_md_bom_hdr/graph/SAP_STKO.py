from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bom_hdr.config.ConfigStore import *
from sap_01_md_bom_hdr.udfs.UDFs import *

def SAP_STKO(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.DBNAME}.stko WHERE _deleted_ = 'F'")
