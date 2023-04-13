from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_bba_bbn_hcs_mrs.config.ConfigStore import *
from sap_md_bom_hdr_bba_bbn_hcs_mrs.udfs.UDFs import *

def Filter_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
