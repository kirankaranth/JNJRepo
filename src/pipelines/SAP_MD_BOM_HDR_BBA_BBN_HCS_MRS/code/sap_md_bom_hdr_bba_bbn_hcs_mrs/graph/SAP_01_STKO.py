from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_bba_bbn_hcs_mrs.config.ConfigStore import *
from sap_md_bom_hdr_bba_bbn_hcs_mrs.udfs.UDFs import *

def SAP_01_STKO(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.stko WHERE _deleted_ = 'F'")
