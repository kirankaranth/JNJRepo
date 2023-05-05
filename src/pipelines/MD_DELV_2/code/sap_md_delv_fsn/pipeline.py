from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_fsn.config.ConfigStore import *
from sap_md_delv_fsn.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_fsn.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_sql_MD_DELV = collectMetrics(
        spark, 
        df_sql_MD_DELV, 
        "graph", 
        "3d0f9068-2429-41e6-a5a9-316d5c2e1d51", 
        "c20fa1e5-3e35-4f75-ac04-3e7bda11c4aa"
    )
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "8d6c57fe-17ed-44c6-8744-bce74b4a2daf", 
        "c8df667b-c32a-43df-bc87-6912900543ef"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
