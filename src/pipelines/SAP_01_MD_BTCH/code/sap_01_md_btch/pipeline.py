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
    df_SchemaTransform_MCH1 = SchemaTransform_MCH1(spark, df_SAP_MCH1_FILTER)
    df_Reformat_MCH1 = Reformat_MCH1(spark, df_SchemaTransform_MCH1)
    df_SAP_MCHA = SAP_MCHA(spark)
    df_SAP_MCHA = collectMetrics(
        spark, 
        df_SAP_MCHA, 
        "graph", 
        "1W2sgecpbEOJwBqAU_pjT$$uPyH1HhmGlLMikmpL9-RS", 
        "dWVBL007rGO1Y3AmetkDL$$jWDAYLwquMLXziN0IcA7s"
    )
    df_SAP_MCHA_FILTER = SAP_MCHA_FILTER(spark, df_SAP_MCHA)
    df_SchemaTransform_MCHA = SchemaTransform_MCHA(spark, df_SAP_MCHA_FILTER)
    df_Reformat_MCHA = Reformat_MCHA(spark, df_SchemaTransform_MCHA)
    df_UNION = UNION(spark, df_Reformat_MCHA, df_Reformat_MCH1)
    df_UNION = collectMetrics(
        spark, 
        df_UNION, 
        "graph", 
        "lJMnrv5eL7R2Q6XTMZ77V$$ruguIIbHjyLv4ih-xxNo0", 
        "6ySEpQqbJmmzfphIHlgTZ$$WdVUHW8tEBBfu65iCb-1H"
    )
    MD_BTCH(spark, df_UNION)

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
