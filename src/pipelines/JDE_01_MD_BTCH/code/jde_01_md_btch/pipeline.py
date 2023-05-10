from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_01_md_btch.config.ConfigStore import *
from jde_01_md_btch.udfs.UDFs import *
from prophecy.utils import *
from jde_01_md_btch.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4108 = JDE_F4108(spark)
    df_JDE_F4108 = collectMetrics(
        spark, 
        df_JDE_F4108, 
        "graph", 
        "8fMrcCUOLSI_2oqqv4c51$$WrIKhr57mhf0ta4mVv4Wn", 
        "P-RtSVV4grUBi1snAleZi$$pIT6KHsudM7q1u2gBoVgU"
    )
    df_F4108_FILTER = F4108_FILTER(spark, df_JDE_F4108)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_F4108_FILTER)
    df_NEW_FIELDS = collectMetrics(
        spark, 
        df_NEW_FIELDS, 
        "graph", 
        "JIZXIO5ks9ITA52MdWcuD$$NrGIHPmjKr276iq77ZAsc", 
        "7whobdQR7Iz85Xz3OD6sa$$sVn3e-ESYLFXePsJa4zF0"
    )
    df_NEW_FIELDS.cache().count()
    df_NEW_FIELDS.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_01_MD_BTCH")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_01_MD_BTCH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
