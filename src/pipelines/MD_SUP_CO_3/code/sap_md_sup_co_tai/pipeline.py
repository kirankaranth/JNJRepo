from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sup_co_tai.config.ConfigStore import *
from sap_md_sup_co_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_md_sup_co_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP_CO = sql_MD_SUP_CO(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP_CO)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "418f53fa-15a1-4a35-8638-08037f9ec7ee", 
        "cc09e1d4-d024-42e1-81f5-23d3f69f9921"
    )
    MD_SUP_CO(spark, df_addL1fields)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_CO_3")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_CO_3")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
