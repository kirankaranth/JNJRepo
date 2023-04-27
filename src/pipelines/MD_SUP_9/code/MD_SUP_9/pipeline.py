from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_SUP_9.config.ConfigStore import *
from MD_SUP_9.udfs.UDFs import *
from prophecy.utils import *
from MD_SUP_9.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP = sql_MD_SUP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP)
    MD_SUP(spark, df_addL1fields)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_9")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_9")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
