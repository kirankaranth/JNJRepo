from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from generic_swap.config.ConfigStore import *
from generic_swap.udfs.UDFs import *
from prophecy.utils import *
from generic_swap.graph import *

def pipeline(spark: SparkSession) -> None:
    GENERIC_SWAP(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/GENERIC_SWAP")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/GENERIC_SWAP")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
