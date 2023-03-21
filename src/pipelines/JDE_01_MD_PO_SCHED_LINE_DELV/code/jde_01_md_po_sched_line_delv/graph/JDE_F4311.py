from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_po_sched_line_delv.config.ConfigStore import *
from jde_01_md_po_sched_line_delv.udfs.UDFs import *

def JDE_F4311(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM jes.f4311 WHERE _deleted_ = 'F' ")
