from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from generic_delete.config.ConfigStore import *
from generic_delete.udfs.UDFs import *
from prophecy.utils import *
from generic_delete.graph import *

def pipeline(spark: SparkSession) -> None:
    Generic_Delete(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/GENERIC_DELETE")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/GENERIC_DELETE")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
