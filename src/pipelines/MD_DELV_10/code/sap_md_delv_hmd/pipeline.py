from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_hmd.config.ConfigStore import *
from sap_md_delv_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DELV = sql_MD_DELV(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_DELV)
    df_Deduplicate = Deduplicate(spark, df_addL1fields)
    df_Deduplicate = collectMetrics(
        spark, 
        df_Deduplicate, 
        "graph", 
        "HH5ZE1oxpSxeQM_00SZGs$$Mh3xOQC9Ljg_h4ts3E-Rz", 
        "_QveIovRVkd3jP6vMQTCi$$-BLMba_UtR37EO7A7T12E"
    )
    MD_DELV(spark, df_Deduplicate)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DELV_10")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DELV_10")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
