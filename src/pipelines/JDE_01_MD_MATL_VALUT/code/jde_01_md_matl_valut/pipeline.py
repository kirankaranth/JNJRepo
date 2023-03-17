from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_matl_valut.config.ConfigStore import *
from jde_01_md_matl_valut.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_matl_valut.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_JDE_01_F4105 = DS_JDE_01_F4105(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_MATL_VALUT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_MATL_VALUT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
