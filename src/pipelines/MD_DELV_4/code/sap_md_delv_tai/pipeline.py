from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_tai.config.ConfigStore import *
from sap_md_delv_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_sql_MD_DELV = collectMetrics(
        spark, 
        df_sql_MD_DELV, 
        "graph", 
        "c80f4f7a-9b15-4c72-9f4a-7973f655f664", 
        "cb19620f-d106-41d0-9c34-954be54e10cd"
    )
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "bced0899-ff3a-48c9-bd5d-bce07e46a9ff", 
        "fc5ff9ab-7822-46ed-8275-146c6de9dab2"
    )
    MD_DELV(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_4")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_4")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
