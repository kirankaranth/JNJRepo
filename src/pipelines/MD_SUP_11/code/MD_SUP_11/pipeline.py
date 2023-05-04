from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_SUP_11.config.ConfigStore import *
from MD_SUP_11.udfs.UDFs import *
from prophecy.utils import *
from MD_SUP_11.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP = sql_MD_SUP(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "1f5e4598-0593-41fc-b2b9-35c91e822dc8", 
        "11a3f9e5-8968-4207-b8b4-ed6b2199df43"
    )
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
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_11")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_11")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
