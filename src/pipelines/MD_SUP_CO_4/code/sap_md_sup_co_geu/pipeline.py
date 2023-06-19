from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sup_co_geu.config.ConfigStore import *
from sap_md_sup_co_geu.udfs.UDFs import *
from prophecy.utils import *
from sap_md_sup_co_geu.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_SUP_CO = sql_MD_SUP_CO(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_SUP_CO)
    df_addL1fields = collectMetrics(
        spark, 
        df_addL1fields, 
        "graph", 
        "9580eb9e-8bfa-499e-b431-1b0275781bd2", 
        "1d727579-db20-4e0e-b443-42e7893b454d"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_SUP_CO_4")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_SUP_CO_4")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
