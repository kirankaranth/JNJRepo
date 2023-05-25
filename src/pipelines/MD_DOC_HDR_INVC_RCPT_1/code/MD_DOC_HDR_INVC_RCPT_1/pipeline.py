from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from MD_DOC_HDR_INVC_RCPT_1.config.ConfigStore import *
from MD_DOC_HDR_INVC_RCPT_1.udfs.UDFs import *
from prophecy.utils import *
from MD_DOC_HDR_INVC_RCPT_1.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sql_MD_DOC_HDR_INVC_RCPT = sql_MD_DOC_HDR_INVC_RCPT(spark)
    df_addL1fields = addL1fields(spark, df_sql_MD_DOC_HDR_INVC_RCPT)
    MD_DOC_HDR_INVC_RCPT(spark, df_addL1fields)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/MD_DOC_HDR_INVC_RCPT_1")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/MD_DOC_HDR_INVC_RCPT_1")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
