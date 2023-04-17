from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sk_sap_delete_me.config.ConfigStore import *
from sk_sap_delete_me.udfs.UDFs import *
from prophecy.utils import *
from sk_sap_delete_me.graph import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SK_SAP_DELETE_ME")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SK_SAP_DELETE_ME")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
