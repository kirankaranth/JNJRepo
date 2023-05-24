from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_own_expln_for_term_of_pmt.config.ConfigStore import *
from sap_md_own_expln_for_term_of_pmt.udfs.UDFs import *

def T052U(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"hm2.T052U")
