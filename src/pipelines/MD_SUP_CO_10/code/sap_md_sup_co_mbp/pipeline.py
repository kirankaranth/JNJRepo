from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sup_co_mbp.config.ConfigStore import *
from sap_md_sup_co_mbp.udfs.UDFs import *
from prophecy.utils import *
from sap_md_sup_co_mbp.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP_CO = sql_MD_SUP_CO(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP_CO)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "a0e43630-cdbe-4c56-ad65-c4eb6e79695d", 
        "209180c6-a1d3-4c5b-8d77-650e48afb3c7"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_CO_10")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_CO_10")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
