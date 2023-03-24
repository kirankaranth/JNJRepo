from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_loc.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4102 = JDE_F4102(spark)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_JDE_F4102)
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_LOC_SJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL_LOC_SJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
