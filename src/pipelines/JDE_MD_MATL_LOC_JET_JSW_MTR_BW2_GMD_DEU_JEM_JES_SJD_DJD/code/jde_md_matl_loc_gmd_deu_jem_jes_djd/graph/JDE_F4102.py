from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.config.ConfigStore import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.udfs.UDFs import *

def JDE_F4102(spark: SparkSession) -> DataFrame:
    return spark.sql(f'SELECT * FROM {f"{Config.sourceSystem}.{Config.sourceTable}"} WHERE _deleted_='F'')
