from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_bba_bbl_bbn.config.ConfigStore import *
from sap_md_matl_loc_bba_bbl_bbn.udfs.UDFs import *

def SAP_MARC(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.{Config.sourceTable} WHERE _deleted_ = 'F'")
