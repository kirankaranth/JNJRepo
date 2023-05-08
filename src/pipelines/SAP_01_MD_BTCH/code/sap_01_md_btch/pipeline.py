from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_btch.config.ConfigStore import *
from sap_01_md_btch.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_btch.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_MCH1 = SAP_MCH1(spark)
    df_SAP_MCH1 = collectMetrics(
        spark, 
        df_SAP_MCH1, 
        "graph", 
        "quEGu54DAY_SVy_Whzirn$$ZxUqOZJzlRqN_-vExlPAY", 
        "EJUkXbKyNibpO5mLhL7bQ$$lLxo-IjawFsLNBYDUZ2iY"
    )
    df_SAP_MCH1_FILTER = SAP_MCH1_FILTER(spark, df_SAP_MCH1)
    df_SAP_MCH1_FILTER = collectMetrics(
        spark, 
        df_SAP_MCH1_FILTER, 
        "graph", 
        "ozYMMcm2ZakfkMSmP_aMP$$MRutNd_vJDVFWju_ahnY2", 
        "e6X4P6dLRCIXEhnZAuozS$$NDp48YB75EU6o0Q3jWupM"
    )
    df_SAP_MCH1_FILTER.cache().count()
    df_SAP_MCH1_FILTER.unpersist()
    df_SAP_MCHA = SAP_MCHA(spark)
    df_SAP_MCHA = collectMetrics(
        spark, 
        df_SAP_MCHA, 
        "graph", 
        "1W2sgecpbEOJwBqAU_pjT$$uPyH1HhmGlLMikmpL9-RS", 
        "dWVBL007rGO1Y3AmetkDL$$jWDAYLwquMLXziN0IcA7s"
    )
    df_SAP_MCHA_FILTER = SAP_MCHA_FILTER(spark, df_SAP_MCHA)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_SAP_MCHA_FILTER)
    df_SchemaTransform_1 = collectMetrics(
        spark, 
        df_SchemaTransform_1, 
        "graph", 
        "s5xK0Q3ueppqhnbIu7eeV$$GzpEJ3zHadu_FSnxxgmo4", 
        "uOXJJeGi5UWLNjvFtqa4L$$vKF-jCi23r65FPR1byTPU"
    )
    df_SchemaTransform_1.cache().count()
    df_SchemaTransform_1.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_BTCH")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_BTCH")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
